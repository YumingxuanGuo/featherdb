use std::sync::{Arc, RwLock};

use crate::error::{Result};
use crate::storage::Store;

use super::{Mode, transaction::Transaction};


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
