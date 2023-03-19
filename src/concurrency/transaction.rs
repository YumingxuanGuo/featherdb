use std::iter::Peekable;
use std::ops::{RangeBounds, Bound};
use std::{sync::Arc, borrow::Cow};
use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::storage::kv::{KvStore, Range, KvScan};

/// An MVCC transaction.
pub struct Transaction {
    /// The underlying store for the transaction. Shared between transactions using a mutex.
    store: Arc<Box<dyn KvStore>>,
    /// The unique transaction ID.
    id: u64,
    /// The transaction mode.
    mode: Mode,
    /// The snapshot that the transaction is running in.
    snapshot: Snapshot,
}

impl Transaction {
    /// Begins a new transaction in the given mode.
    pub(super) fn begin(store: Arc<Box<dyn KvStore>>, mode: Mode) -> Result<Self> {
        let id = match store.get(&MvccKey::TxnNext.encode())? {
            Some(ref v) => deserialize(v)?,
            None => 1,
        };
        store.set(&MvccKey::TxnNext.encode(), serialize(&(id + 1))?)?;
        store.set(&MvccKey::TxnActive(id).encode(), serialize(&mode)?)?;

        // We always take a new snapshot, even for snapshot transactions, because all transactions
        // increment the transaction ID and we need to properly record currently active transactions
        // for any future snapshot transactions looking at this one.
        let mut snapshot = Snapshot::take(store.clone(), id)?;
        if let Mode::Snapshot { version } = &mode {
            snapshot = Snapshot::restore(store.clone(), *version)?
        }

        Ok(Self { store, id, mode, snapshot })
    }

    /// Resumes an active transaction with the given ID. Errors if the transaction is not active.
    pub(super) fn resume(store: Arc<Box<dyn KvStore>>, id: u64) -> Result<Self> {
        let mode = match store.get(&MvccKey::TxnActive(id).encode())? {
            Some(v) => deserialize(&v)?,
            None => return Err(Error::Value(format!("No active transaction {}", id))),
        };

        // If the txn's mode is `Snapshot`, then restore that particular one.
        // Otherwise restore the one with the txn id.
        let snapshot = match &mode {
            Mode::Snapshot { version } => Snapshot::restore(store.clone(), *version)?,
            _ => Snapshot::restore(store.clone(), id)?,
        };
        Ok(Self { store, id, mode, snapshot })
    }

    /// Returns the transaction ID.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Returns the transaction mode.
    pub fn mode(&self) -> Mode {
        self.mode
    }

    /// Commits the transaction, by removing the txn from the active set.
    pub fn commit(self) -> Result<()> {
        self.store.delete(&MvccKey::TxnActive(self.id).encode())?;
        self.store.flush()
    }

    /// Rolls back the transaction, by removing all updated entries.
    pub fn rollback(self) -> Result<()> {
        if self.mode.allows_write() {
            let mut keys_to_rollback = vec![];
            let mut scan = self.store.scan(Range::from(
                MvccKey::TxnUpdate(self.id, vec![].into()).encode()
                    ..MvccKey::TxnUpdate(self.id + 1, vec![].into()).encode()
            ))?;

            // Delete all `TxnUpdate`s and all `Record`s.
            while let Some((key, _)) = scan.next().transpose()? {
                match MvccKey::decode(&key)? {
                    MvccKey::TxnUpdate(_, updated_key) => keys_to_rollback.push(updated_key.into_owned()),
                    k => return Err(Error::Internal(format!("Expected TxnUpdate, got {:?}", k))),
                }
                keys_to_rollback.push(key);
            }
            std::mem::drop(scan);
            for key in keys_to_rollback.into_iter() {
                self.store.delete(&key)?;
            }
        }
        self.store.delete(&MvccKey::TxnActive(self.id).encode())
    }

    /// Writes a value for a key. None is used for deletion.
    fn write(&self, key: &[u8], value: Option<Vec<u8>>) -> Result<()> {
        if !self.mode.allows_write() {
            return Err(Error::ReadOnly);
        }

        // Checks if the key has any uncommitted changes by scanning the invisible versions.
        // If there are, returns `Error::Serialization` and has the client retry the request.
        let min = self.snapshot.invisible.iter().min().cloned().unwrap_or(self.id + 1);
        let mut scan = self.store.scan(Range::from(
            MvccKey::Record(key.into(), min).encode()
                ..MvccKey::Record(key.into(), std::u64::MAX).encode()
        ))?.rev();
        while let Some((k, _)) = scan.next().transpose()? {
            match MvccKey::decode(&k) ?{
                MvccKey::Record(_, version) => {
                    if !self.snapshot.can_access(version) {
                        return Err(Error::Serialization);
                    }
                }
                k => return Err(Error::Internal(format!("Expected Txn::Record, got {:?}", k))),
            };
        }

        // Writes the key and the update record.
        let key = MvccKey::Record(key.into(), self.id).encode();
        let update = MvccKey::TxnUpdate(self.id, (&key).into()).encode();
        self.store.set(&update, vec![0x00])?;   // A non-empty placeholder value.
        self.store.set(&key, serialize(&value)?)
    }

    /// Sets a key.
    pub fn set(&self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.write(key, Some(value))
    }

    /// Deletes a key.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.write(key, None)
    }

    /// Fetches a key.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // Fetches the most recent version of the key.
        let mut scan = self.store.scan(Range::from(
            MvccKey::Record(key.into(), 0).encode()..=MvccKey::Record(key.into(), self.id).encode(),
        ))?.rev();
        while let Some((k, v)) = scan.next().transpose()? {
            match MvccKey::decode(&k)? {
                MvccKey::Record(_, version) => {
                    if self.snapshot.can_access(version) {
                        return deserialize(&v);
                    }
                }
                k => return Err(Error::Internal(format!("Expected Txn::Record, got {:?}", k))),
            }
        }
        Ok(None)
    }

    /// Scans a key range.
    pub fn scan(&self, range: impl RangeBounds<Vec<u8>>) -> Result<KvScan> {
        let start = match range.start_bound() {
            Bound::Excluded(k) => Bound::Excluded(MvccKey::Record(k.into(), std::u64::MAX).encode()),
            Bound::Included(k) => Bound::Included(MvccKey::Record(k.into(), 0).encode()),
            Bound::Unbounded => Bound::Included(MvccKey::Record(vec![].into(), 0).encode()),
        };
        let end = match range.end_bound() {
            Bound::Excluded(k) => Bound::Excluded(MvccKey::Record(k.into(), 0).encode()),
            Bound::Included(k) => Bound::Included(MvccKey::Record(k.into(), std::u64::MAX).encode()),
            Bound::Unbounded => Bound::Unbounded,
        };
        let scan = self.store.scan(Range::from((start,end)))?;
        Ok(Box::new(MvccScan::new(scan, self.snapshot.clone())))
    }

    /// Scans keys with a given prefix.
    pub fn scan_prefix(&self, prefix: &[u8]) -> Result<KvScan> {
        if prefix.is_empty() {
            return Err(Error::Internal("Scan prefix cannot be empty".into()));
        }
        let start = prefix.to_vec();
        let mut end = start.clone();
        for i in (0..end.len()).rev() {
            match end[i] {
                // If all 0xff we could in principle use Range::Unbounded, but it won't happen
                0xff if i == 0 => return Err(Error::Internal("Invalid prefix scan range".into())),
                0xff => {
                    end[i] = 0x00;
                    continue;
                }
                v => {
                    end[i] = v + 1;
                    break;
                }
            }
        }
        self.scan(start..end)
    }
}

/// An MVCC transaction mode.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum Mode {
    /// A read-write transaction.
    ReadWrite,
    /// A read-only transaction.
    ReadOnly,
    /// A read-only transaction running in a snapshot of a given version.
    ///
    /// The version must refer to a committed transaction ID. Any changes visible to the original
    /// transaction will be visible in the snapshot (i.e. transactions that had not committed before
    /// the snapshot transaction started will not be visible, even though they have a lower version).
    Snapshot { version: u64 },
}

impl Mode {
    /// Checks whether the transaction mode can mutate data.
    pub fn allows_write(&self) -> bool {
        match self {
            Self::ReadWrite => true,
            _ => false,
        }
    }
}

/// A versioned snapshot, containing visibility information about concurrent transactions.
#[derive(Clone)]
struct Snapshot {
    /// The version (i.e. transaction ID) that the snapshot belongs to.
    version: u64,
    /// The set of transaction IDs that were active at the start of the transactions,
    /// and thus should be invisible to the snapshot.
    invisible: HashSet<u64>,
}

impl Snapshot {
    /// Takes a new snapshot, persisting it as `Key::TxnSnapshot(version)`.
    fn take(store: Arc<Box<dyn KvStore>>, version: u64) -> Result<Self> {
        let mut invisible = HashSet::new();
        let mut scan = store.scan(Range::from(
            MvccKey::TxnActive(0).encode()..MvccKey::TxnActive(version).encode()
        ))?;
        while let Some((key, _)) = scan.next().transpose()? {
            match MvccKey::decode(&key)? {
                MvccKey::TxnActive(id) => invisible.insert(id),
                k => return Err(Error::Internal(format!("Expected TxnActive, got {:?}", k))),
            };
        }
        std::mem::drop(scan);
        store.set(&MvccKey::TxnSnapshot(version).encode(), serialize(&invisible)?)?;
        Ok(Self { version, invisible })
    }

    /// Restores an existing snapshot from `Key::TxnSnapshot(version)`, or errors if not found.
    fn restore(store: Arc<Box<dyn KvStore>>, version: u64) -> Result<Self> {
        match store.get(&MvccKey::TxnSnapshot(version).encode())? {
            Some(ref v) => Ok(Self { version, invisible: deserialize(v)? }),
            None => Err(Error::Value(format!("Snapshot not found for version {}", version))),
        }
    }

    /// Checks whether the given version is visible in this snapshot.
    fn can_access(&self, version: u64) -> bool {
        version <= self.version && self.invisible.get(&version).is_none()
    }
}

/// MVCC keys. The encoding preserves the grouping and ordering of keys. 
/// Uses a Cow since we want to take borrows when encoding and return owned when decoding.
#[derive(Debug)]
pub(super) enum MvccKey<'a> {
    /// The next available txn ID. Used when starting new txns.
    TxnNext,
    /// Active txn markers, containing the mode. Used to detect concurrent txns, and to resume.
    TxnActive(u64),
    /// Txn snapshot, containing concurrent active txns at start of txn.
    TxnSnapshot(u64),
    /// Update marker for a txn ID and key, used for rollback.
    TxnUpdate(u64, Cow<'a, [u8]>),
    /// A record for a key/version pair.
    Record(Cow<'a, [u8]>, u64),
    /// Arbitrary unversioned metadata.
    Metadata(Cow<'a, [u8]>),
}

impl<'a> MvccKey<'a> {
    /// Encodes a key into a byte vector.
    pub(super) fn encode(self) -> Vec<u8> {
        use crate::encoding::*;
        match self {
            Self::TxnNext => vec![0x01],
            Self::TxnActive(id) => [&[0x02][..], &encode_u64(id)].concat(),
            Self::TxnSnapshot(version) => [&[0x03][..], &encode_u64(version)].concat(),
            Self::TxnUpdate(id, key) => {
                [&[0x04][..], &encode_u64(id), &encode_bytes(&key)].concat()
            }
            Self::Metadata(key) => [&[0x05][..], &encode_bytes(&key)].concat(),
            Self::Record(key, version) => {
                [&[0xff][..], &encode_bytes(&key), &encode_u64(version)].concat()
            }
        }
    }

    /// Decodes a key from a byte representation.
    fn decode(mut bytes: &[u8]) -> Result<Self> {
        use crate::encoding::*;
        let bytes = &mut bytes;
        let key = match take_byte(bytes)? {
            0x01 => Self::TxnNext,
            0x02 => Self::TxnActive(take_u64(bytes)?),
            0x03 => Self::TxnSnapshot(take_u64(bytes)?),
            0x04 => Self::TxnUpdate(take_u64(bytes)?, take_bytes(bytes)?.into()),
            0x05 => Self::Metadata(take_bytes(bytes)?.into()),
            0xff => Self::Record(take_bytes(bytes)?.into(), take_u64(bytes)?),
            b => return Err(Error::Internal(format!("Unknown MVCC key prefix {:x?}", b))),
        };
        if !bytes.is_empty() {
            return Err(Error::Internal("Unexpected data remaining at end of key".into()));
        }
        Ok(key)
    }
}

/// Serializes MVCC metadata.
fn serialize<V: Serialize>(value: &V) -> Result<Vec<u8>> {
    Ok(bincode::serialize(value)?)
}

/// Deserializes MVCC metadata.
fn deserialize<'a, V: Deserialize<'a>>(bytes: &'a [u8]) -> Result<V> {
    Ok(bincode::deserialize(bytes)?)
}

/// A key range scan.
pub struct MvccScan {
    /// The augmented KV store iterator, with key (decoded) and value. Note that we don't retain
    /// the decoded version, so there will be multiple keys (for each version). We want the last.
    scan: Peekable<KvScan>,
    /// Keeps track of next_back() seen key, whose previous versions should be ignored.
    next_back_seen: Option<Vec<u8>>,
}

impl MvccScan {
    fn new(mut scan: KvScan, snapshot: Snapshot) -> Self {
        // Augment the underlying scan to decode the key and filter invisible versions. We don't
        // return the version, since we don't need it, but beware that all versions of the key
        // will still be returned - we usually only need the last, which is what the next() and
        // next_back() methods need to handle. We also don't decode the value, since we only need
        // to decode the last version.
        scan = Box::new(scan.filter_map(move |r| {
            r.and_then(|(k, v)| match MvccKey::decode(&k)? {
                MvccKey::Record(_, version) if !snapshot.can_access(version) => Ok(None),
                MvccKey::Record(key, _) => Ok(Some((key.into_owned(), v))),
                k => Err(Error::Internal(format!("Expected Record, got {:?}", k))),
            }).transpose()
        }));
        Self { scan: scan.peekable(), next_back_seen: None }
    }

    // next() with error handling.
    fn try_next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        while let Some((key, value)) = self.scan.next().transpose()? {
            // Only return the item if it is the last version of the key.
            if match self.scan.peek() {
                Some(Ok((peek_key, _))) if *peek_key != key => true,
                Some(Ok(_)) => false,
                Some(Err(e)) => return Err(e.clone()),
                None => true,
            } {
                // Only return non-deleted items.
                if let Some(value) = deserialize(&value)? {
                    return Ok(Some((key, value)));
                }
            }
        }
        Ok(None)
    }

    /// next_back() with error handling.
    fn try_next_back(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        while let Some((key, value)) = self.scan.next_back().transpose()? {
            // Only return the last version of the key (so skip if seen).
            if match &self.next_back_seen {
                Some(seen_key) if *seen_key != key => true,
                Some(_) => false,
                None => true,
            } {
                self.next_back_seen = Some(key.clone());
                // Only return non-deleted items.
                if let Some(value) = deserialize(&value)? {
                    return Ok(Some((key, value)));
                }
            }
        }
        Ok(None)
    }
}

impl Iterator for MvccScan {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
    }
}

impl DoubleEndedIterator for MvccScan {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.try_next_back().transpose()
    }
}