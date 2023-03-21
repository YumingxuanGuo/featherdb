#![allow(dead_code)]

use std::sync::Arc;

use dashmap::DashMap;
use parking_lot::RwLock;

use crate::{storage::kv::KvStore, error::Result};
use super::{Mode, Transaction};

/// An MVCC-based transactional key-value store.
#[derive(Clone)]
pub struct MVCC {
    /// The underlying KV store. It is protected by a mutex so it can be shared between txns.
    store: Arc<RwLock<Box<dyn KvStore>>>,
    lock_manager: Option<Arc<LockManager>>,
}

impl MVCC {
    /// Creates a new MVCC key-value store with the given key-value store for storage.
    pub fn new(store: Box<dyn KvStore>, serializable: bool) -> Self {
        Self {
            store: Arc::new(RwLock::new(store)),
            lock_manager: match serializable {
                true => Some(Arc::new(LockManager::new())),
                false => None,
            }
        }
    }

    /// Begins a new transaction in default read-write mode.
    pub fn begin(&self) -> Result<Transaction> {
        self.begin_with_mode(Mode::ReadWrite)
    }

    /// Begins a new transaction in the given mode.
    pub fn begin_with_mode(&self, mode: Mode) -> Result<Transaction> {
        Transaction::begin(self.store.clone(), mode, self.lock_manager.clone())
    }

    /// Resumes a transaction with the given ID.
    pub fn resume(&self, id: u64) -> Result<Transaction> {
        Transaction::resume(self.store.clone(), id, self.lock_manager.clone())
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

/// An SSI-lock type.
#[derive(Clone)]
enum LockType {
    SIRead,
    Write,
}

/// Info for a particular SSI-lock.
#[derive(Clone)]
struct Lock {
    /// The type of this SSI-lock.
    lock_type: LockType,
    /// The id of the transaction that takes this SSI-lock.
    owner_id: u64,
}

/// A lock manager for Serializable Snapshot Isolation.
#[derive(Clone)]
pub(super) struct LockManager {
    locks: DashMap<Vec<u8>, Vec<Lock>>,
}

impl LockManager {
    pub(super) fn new() -> Self {
        todo!()
    }
}