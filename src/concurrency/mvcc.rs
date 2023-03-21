use std::sync::Arc;

use parking_lot::RwLock;

use crate::{storage::kv::KvStore, error::Result};
use super::{Mode, Transaction};

/// An MVCC-based transactional key-value store.
pub struct MVCC {
    /// The underlying KV store. It is protected by a mutex so it can be shared between txns.
    store: Arc<RwLock<Box<dyn KvStore>>>,
}

impl Clone for MVCC {
    fn clone(&self) -> Self {
        MVCC { store: self.store.clone() }
    }
}

impl MVCC {
    /// Creates a new MVCC key-value store with the given key-value store for storage.
    pub fn new(store: Box<dyn KvStore>) -> Self {
        Self { store: Arc::new(RwLock::new(store)) }
    }

    /// Begins a new transaction in default read-write mode.
    pub fn begin(&self) -> Result<Transaction> {
        self.begin_with_mode(Mode::ReadWrite)
    }

    /// Begins a new transaction in the given mode.
    pub fn begin_with_mode(&self, mode: Mode) -> Result<Transaction> {
        Transaction::begin(self.store.clone(), mode)
    }

    /// Resumes a transaction with the given ID.
    pub fn resume(&self, id: u64) -> Result<Transaction> {
        Transaction::resume(self.store.clone(), id)
    }

    /// Fetches an unversioned metadata value
    pub fn get_metadata(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        use super::transaction::MvccKey;
        let session = self.store.read();
        session.get(&MvccKey::Metadata(key.into()).encode())
    }

    /// Sets an unversioned metadata value
    pub fn set_metadata(&self, key: &[u8], value: Vec<u8>) -> Result<()> {
        use super::transaction::MvccKey;
        let session = self.store.write();
        session.set(&MvccKey::Metadata(key.into()).encode(), value)
    }
}