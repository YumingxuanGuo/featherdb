use parking_lot::RwLock;

use super::{Range, KvScan, KvStore};
use crate::error::Result;

use std::collections::BTreeMap;
use std::fmt::Display;
use std::sync::Arc;

/// In-memory key-value store using the Rust standard library B-tree implementation.
pub struct StdBPlusTree {
    data: Arc<RwLock<BTreeMap<Vec<u8>, Vec<u8>>>>,
}

impl StdBPlusTree {
    /// Creates a new Memory key-value storage engine.
    pub fn new() -> Self {
        Self { data: Arc::new(RwLock::new(BTreeMap::new())) }
    }
}

impl Display for StdBPlusTree {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "stdmemory")
    }
}

impl KvStore for StdBPlusTree {
    fn set(&self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.data.write().insert(key.to_vec(), value);
        Ok(())
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.data.read().get(key).cloned())
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        self.data.write().remove(key);
        Ok(())
    }

    fn scan(&self, range: Range) -> Result<KvScan> {
        // FIXME Since the range iterator returns borrowed items it would require a read-lock for
        // the duration of the iteration. This is too coarse, so we buffer the entire iteration
        // here. An iterator with an arc-mutex should be used instead, which is able to resume
        // iteration by grabbing the lock again.
        Ok(Box::new(
            self.data
                .read()
                .range(range)
                .map(|(k, v)| Ok((k.clone(), v.clone())))
                .collect::<Vec<_>>()
                .into_iter()
        ))
    }

    fn flush(&self) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
impl super::TestSuite<StdBPlusTree> for StdBPlusTree {
    fn setup() -> Result<Self> {
        Ok(StdBPlusTree::new())
    }
}

#[test]
fn tests() -> Result<()> {
    use super::TestSuite;
    StdBPlusTree::test()
}
