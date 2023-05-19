use std::collections::HashMap;
use std::fmt::Display;
use std::ops::Bound;
use std::sync::{Arc, RwLock};

use crate::error::{Error, Result};
use super::{Range, LogStore, LogScan};

/// Log storage backend for testing. Protects an inner Memory backend using a mutex, so it can
/// be cloned and inspected.
#[derive(Clone)]
pub struct LogDemo {
    store: Arc<RwLock<Memory>>,
}

impl LogDemo {
    /// Creates a new Demo key-value storage engine.
    pub fn new() -> Self {
        Self { store: Arc::new(RwLock::new(Memory::new())) }
    }
}

impl Display for LogDemo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "test")
    }
}

impl LogStore for LogDemo {
    fn append(&mut self, entry: Vec<u8>) -> Result<u64> {
        self.store.write()?.append(entry)
    }

    fn commit(&mut self, index: u64) -> Result<()> {
        self.store.write()?.commit(index)
    }

    fn commit_index(&self) -> u64 {
        self.store.read().unwrap().commit_index()
    }

    fn get(&self, index: u64) -> Result<Option<Vec<u8>>> {
        self.store.read()?.get(index)
    }

    fn len(&self) -> u64 {
        self.store.read().unwrap().len()
    }

    fn scan(&self, range: Range) -> LogScan {
        // Since the mutex guard is scoped to this method, we simply buffer the result.
        Box::new(self.store.read().unwrap().scan(range).collect::<Vec<Result<_>>>().into_iter())
    }

    fn size(&self) -> u64 {
        self.store.read().unwrap().size()
    }

    fn truncate(&mut self, index: u64) -> Result<u64> {
        self.store.write()?.truncate(index)
    }

    fn get_metadata(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.store.read()?.get_metadata(key)
    }

    fn set_metadata(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.store.write()?.set_metadata(key, value)
    }
}

#[derive(Clone)]
// An in-memory log store.
pub struct Memory {
    log: Vec<Vec<u8>>,
    commit_index: u64,
    metadata: HashMap<Vec<u8>, Vec<u8>>,
}

impl Memory {
    /// Creates a new in-memory log.
    pub fn new() -> Self {
        Self { log: Vec::new(), commit_index: 0, metadata: HashMap::new() }
    }
}

impl Display for Memory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "memory")
    }
}

impl LogStore for Memory {
    fn append(&mut self, entry: Vec<u8>) -> Result<u64> {
        self.log.push(entry);
        Ok(self.log.len() as u64)
    }

    fn commit(&mut self, index: u64) -> Result<()> {
        if index > self.len() {
            return Err(Error::Internal(format!("Cannot commit non-existant index {}", index)));
        }
        if index < self.commit_index {
            return Err(Error::Internal(format!(
                "Cannot commit below current index {}",
                self.commit_index
            )));
        }
        self.commit_index = index;
        Ok(())
    }

    fn commit_index(&self) -> u64 {
        self.commit_index
    }

    fn get(&self, index: u64) -> Result<Option<Vec<u8>>> {
        match index {
            0 => Ok(None),
            i => Ok(self.log.get(i as usize - 1).cloned()),
        }
    }

    fn len(&self) -> u64 {
        self.log.len() as u64
    }

    fn scan(&self, range: Range) -> LogScan {
        Box::new(
            self.log
                .iter()
                .take(match range.end {
                    Bound::Included(n) => n as usize,
                    Bound::Excluded(0) => 0,
                    Bound::Excluded(n) => n as usize - 1,
                    Bound::Unbounded => std::usize::MAX,
                })
                .skip(match range.start {
                    Bound::Included(0) => 0,
                    Bound::Included(n) => n as usize - 1,
                    Bound::Excluded(n) => n as usize,
                    Bound::Unbounded => 0,
                })
                .cloned()
                .map(Ok),
        )
    }

    fn size(&self) -> u64 {
        self.log.iter().map(|v| v.len() as u64).sum()
    }

    fn truncate(&mut self, index: u64) -> Result<u64> {
        if index < self.commit_index {
            return Err(Error::Internal(format!(
                "Cannot truncate below commit_index index {}",
                self.commit_index
            )));
        }
        self.log.truncate(index as usize);
        Ok(self.log.len() as u64)
    }

    fn get_metadata(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.metadata.get(key).cloned())
    }

    fn set_metadata(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.metadata.insert(key.to_vec(), value);
        Ok(())
    }
}
