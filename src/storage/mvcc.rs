use std::{sync::{Arc, RwLock, RwLockWriteGuard, RwLockReadGuard}, collections::HashSet};

use serde::{Serialize, Deserialize};
use serde_derive::{Serialize, Deserialize};

use crate::{error::{Result, Error}, common::KeyType};

use super::{Store, Range};


/// An MVCC-based transactional key-value store.
pub struct MVCC {
    /// The underlying KV store. It is protected by a mutex so it can be shared between txns.
    store: Arc<RwLock<Box<dyn Store>>>,
}

impl Clone for MVCC {
    fn clone(&self) -> Self {
        MVCC { store: self.store.clone() }
    }
}

impl MVCC {
    /// Creates a new MVCC key-value store with the given key-value store for storage.
    pub fn new(store: Box<dyn Store>) -> Self {
        Self { store: Arc::new(RwLock::new(store)) }
    }
    
    /// Begins a new transaction in default read-write mode.
    #[allow(dead_code)]
    pub fn begin(&self) -> Result<Transaction> {
        self.begin_with_mode(Mode::ReadWrite)
    }

    /// Begins a new transaction in the given mode.
    pub fn begin_with_mode(&self, mode: Mode) -> Result<Transaction> {
        Transaction::begin(Arc::clone(&self.store), mode)
    }
}

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

/// Serializes MVCC metadata.
fn serialize<V: Serialize>(value: &V) -> Result<Vec<u8>> {
    Ok(bincode::serialize(value)?)
}

/// Deserializes MVCC metadata.
fn deserialize<'a, V: Deserialize<'a>>(bytes: &'a [u8]) -> Result<V> {
    Ok(bincode::deserialize(bytes)?)
}

impl Transaction {
    /// Begins a new transaction in the given mode.
    fn begin(store: Arc<RwLock<Box<dyn Store>>>, mode: Mode) -> Result<Self> {
        // Starts a writing session on the underlying txn store.
        let mut session = store.write()?;

        // Gets the next available txn ID.
        let id = match session.get(&Key::TxnNext.encode())? {
            Some(ref v) => 1,
            None => 1,
        };

        // Increments TxnNext and store it back.
        session.set_or_insert(&Key::TxnNext.encode(), serialize(&(id + 1))?)?;

        // Stores the info of the current active txn.
        session.set_or_insert(&Key::TxnActive(id).encode(), serialize(&mode)?)?;
        
        // Takes a new snapshot.
        let mut snapshot = Snapshot::take(&mut session, id)?;

        // If txn mode is set to operate on previous version, fetch the corresponding snapshot.
        std::mem::drop(session);
        if let Mode::Snapshot { version } = &mode {
            snapshot = Snapshot::restore(&store.read()?, *version)?
        }

        Ok(Self { store, id, mode, snapshot })
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
    fn take(session: &mut RwLockWriteGuard<Box<dyn Store>>, version: u64) -> Result<Self> {
        let mut snapshot = Self { version, invisible: HashSet::new() };

        // A scan of all the TxnActive keys.
        let mut scan = 
            session.scan(std::ops::Range::from(Key::TxnActive(0).encode()..Key::TxnActive(version).encode()));

        // Records the currently active txn IDs as invisible.
        while let Some((key, _)) = scan.next().transpose()? {
            // Ensures the scan contains only TxnActive keys (based on the encoding property).
            match Key::decode(&key)? {
                Key::TxnActive(id) => snapshot.invisible.insert(id),
                otherwise => return Err(Error::Internal(format!("Expected TxnActive, got {:?}", otherwise))),
            };
        }

        // Store info of this new snapshot.
        std::mem::drop(scan);
        session.set_or_insert(&Key::TxnSnapshot(version).encode(), serialize(&snapshot.invisible)?)?;

        Ok(snapshot)
    }

    /// Restores an existing snapshot from `Key::TxnSnapshot(version)`, or errors if not found.
    fn restore(session: &RwLockReadGuard<Box<dyn Store>>, version: u64) -> Result<Self> {
        match session.get(&Key::TxnSnapshot(version).encode())? {
            Some(ref invisible) => Ok(Self { version, invisible: deserialize(invisible)? }),
            None => Err(Error::Value(format!("Snapshot not found for version {}", version))),
        }
    }
}

/// MVCC keys. The encoding preserves the grouping and ordering of keys. Uses a Cow since we want
/// to take borrows when encoding and return owned when decoding.
#[derive(Debug)]
enum Key {
    /// The next available txn ID. Used when starting new txns.
    TxnNext,
    /// Active txn markers, containing the mode. Used to detect concurrent txns, and to resume.
    TxnActive(u64),
    /// Txn snapshot, containing concurrent active txns at start of txn.
    TxnSnapshot(u64),
}

impl Key {
    /// Encodes a key into a byte vector.
    fn encode(self) -> Vec<u8> {
        // use encoding::*;
        match self {
            Self::TxnNext => vec![0x01],
            Self::TxnActive(id) => vec![0x02],
            Self::TxnSnapshot(version) => vec![0x03],
        }
    }

    /// Decodes a key from a byte representation.
    fn decode(mut bytes: &[u8]) -> Result<Self> {
        // use encoding::*;
        Ok(Key::TxnNext)
    }
}