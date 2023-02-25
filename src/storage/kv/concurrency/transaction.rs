use std::{sync::{Arc, RwLock}, ops::{RangeBounds, Bound}};

use crate::{storage::{Store, Range, StorageScan}, error::{Result, Error}, common::ValueType};

use super::{Mode, snapshot::{Snapshot}, txnkey::TxnKey, mvcc::{serialize, deserialize}, scan::Scan};

/// An MVCC transaction.
pub struct Transaction {
    /// The underlying store for the transaction. Shared between transactions using a mutex.
    store: Arc<RwLock<Box<dyn Store>>>,
    /// The unique transaction ID.
    id: u64,
    /// The transaction mode.
    mode: Mode,
    /// The snapshot that the transaction is running in.
    snapshot: Snapshot,
}

impl Transaction {
    /// Begins a new transaction in the given mode.
    pub(super) fn begin(store: Arc<RwLock<Box<dyn Store>>>, mode: Mode) -> Result<Self> {
        let mut session = store.write()?;

        // Gets the next available txn ID.
        let id = match session.get(&TxnKey::TxnNext.encode())? {
            Some(ref v) => deserialize(v)?,
            None => 1,
        };
        // Increments TxnNext and store it back.
        session.set_or_insert(&TxnKey::TxnNext.encode(), serialize(&(id + 1))?)?;
        // Stores the info of the current active txn.
        session.set_or_insert(&TxnKey::TxnActive(id).encode(), serialize(&mode)?)?;
        
        // Takes a new snapshot.
        let mut snapshot = Snapshot::take(&mut session, id)?;
        // If txn mode is set to operating on previous version, fetch the corresponding snapshot.
        std::mem::drop(session);
        if let Mode::Snapshot { version } = &mode {
            snapshot = Snapshot::restore(&store.read()?, *version)?
        }

        Ok(Self { store, id, mode, snapshot })
    }

    /// Resumes an active transaction with the given ID. Errors if the transaction is not active.
    pub fn resume(store: Arc<RwLock<Box<dyn Store>>>, id: u64) -> Result<Self> {
        let session = store.read()?;
        let mode = match session.get(&TxnKey::TxnActive(id).encode())? {
            Some(v) => deserialize(&v)?,
            None => return Err(Error::Value(format!("No active transaction {}", id))),
        };
        let snapshot = match &mode {
            Mode::Snapshot { version } => Snapshot::restore(&session, *version)?,
            _otherwise => Snapshot::restore(&session, id)?,
        };
        std::mem::drop(session);
        Ok(Self { store, id, mode, snapshot })
    }

    /// Commits the transaction, by removing the txn from the active set.
    pub fn commit(self) -> Result<()> {
        let mut session = self.store.write()?;
        session.delete(&TxnKey::TxnActive(self.id).encode())?;
        session.flush()
    }

    /// Rolls back the transaction, by removing all updated entries.
    pub fn rollback(self) -> Result<()> {
        let mut session = self.store.write()?;

        // Remove the updated entries if this txn modifies data.
        if self.mode.allow_writing() {
            let mut to_rollback = Vec::new();
            let mut scan = session.scan(Range::from(
                TxnKey::TxnUpdate(self.id, vec![].into()).encode()
                ..TxnKey::TxnUpdate(self.id + 1, vec![].into()).encode(),
            ));
            while let Some((key, _)) = scan.next().transpose()? {
                match TxnKey::decode(&key)? {
                    TxnKey::TxnUpdate(_, updated_key)
                        => to_rollback.push(updated_key.into_owned()),
                    otherwise
                        => return Err(Error::Internal(format!("Expected TxnUpdate, got {:?}", otherwise))),
                }
                to_rollback.push(key);
            }
            std::mem::drop(scan);
            for key in to_rollback.into_iter() {
                session.delete(&key)?;
            }
        }
        session.delete(&TxnKey::TxnActive(self.id).encode())
    }

    /// Sets or inserts a key.
    pub fn set_or_insert(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.write(key, Some(value))
    }

    /// Deletes a key.
    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.write(key, None)
    }

    /// Writes a value for a key. None is used for deletion.
    fn write(&self, key: &[u8], value: Option<ValueType>) -> Result<()> {
        if !self.mode.allow_writing() {
            return Err(Error::ReadOnly);
        }
        let mut session = self.store.write()?;

        // Check if the key has any uncommitted changes by scanning the invisible versions.
        let min_invisible_version = 
            self.snapshot.invisible.iter().min().cloned().unwrap_or(self.id + 1);
        let mut scan = session.scan(Range::from(
                TxnKey::Record(key.into(), min_invisible_version).encode()
                ..=TxnKey::Record(key.into(), std::u64::MAX).encode()
            )).rev();
        while let Some((k, _)) = scan.next().transpose()? {
            match TxnKey::decode(&k)? {
                // ???
                TxnKey::Record(_, version) => {
                    if !self.snapshot.is_visible(version) {
                        return Err(Error::Serialization);
                    }
                },
                otherwise
                    => return Err(Error::Internal(format!("Expected Txn::Record, got {:?}", otherwise))),
            }
        }
        std::mem::drop(scan);

        // Write the key and its updated record.
        let record = TxnKey::Record(key.into(), self.id).encode();
        let update = TxnKey::TxnUpdate(self.id, (&record).into()).encode();
        session.set_or_insert(&update, vec![])?;
        session.set_or_insert(&record, serialize(&value)?)
    }

    /// Fetches a key.
    pub fn get(&self, key: &[u8]) ->Result<Option<ValueType>> {
        let session = self.store.read()?;
        let mut scan = session.scan(Range::from(
            TxnKey::Record(key.into(), 0).encode()
            ..=TxnKey::Record(key.into(), self.id).encode()
        )).rev();
        while let Some((k, v)) = scan.next().transpose()? {
            match TxnKey::decode(&k)? {
                TxnKey::Record(_, version) => {
                    if self.snapshot.is_visible(version) {
                        return deserialize(&v)?;
                    }
                }, 
                otherwise
                    => return Err(Error::Internal(format!("Expected Txn::Record, got {:?}", otherwise))),
            }
        }
        Ok(None)
    }
    
    /// Scans a key range.
    pub fn scan(&self, range: impl RangeBounds<Vec<u8>>) -> Result<StorageScan> {
        let start = match range.start_bound() {
            Bound::Excluded(k) => Bound::Excluded(TxnKey::Record(k.into(), std::u64::MAX).encode()),
            Bound::Included(k) => Bound::Included(TxnKey::Record(k.into(), 0).encode()),
            Bound::Unbounded => Bound::Included(TxnKey::Record(vec![].into(), 0).encode()),
        };
        let end = match range.end_bound() {
            Bound::Excluded(k) => Bound::Excluded(TxnKey::Record(k.into(), 0).encode()),
            Bound::Included(k) => Bound::Included(TxnKey::Record(k.into(), std::u64::MAX).encode()),
            Bound::Unbounded => Bound::Unbounded,
        };
        let scan = self.store.read()?.scan(Range::from((start, end)));
        Ok(Box::new(Scan::new(scan, self.snapshot.clone())))
    }

    /// Scans keys under a given prefix.
    pub fn scan_prefix(&self, prefix: &[u8]) -> Result<StorageScan> {
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

    /// Returns the transaction ID.
    pub fn get_id(&self) -> u64 {
        self.id
    }

    /// Returns the transaction mode.
    pub fn get_mode(&self) -> Mode {
        self.mode
    }
}
