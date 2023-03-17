use std::fmt::Display;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::{RwLock, Mutex};

use crate::error::Result;
use super::super::{KvStore, Range, KvScan};
use super::block::Block;
use super::iterators::{MergeIter, TwoMergeIter};
use super::lsm_iterator::LsmIter;
use super::memtable::MemTable;
use super::sstable::{SsTable, SsTableBuilder, SsTableIter};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

#[derive(Clone)]
pub struct LsmStorageInner {
    /// The current memtable.
    memtable: Arc<MemTable>,
    /// Immutable memTables, from earliest to latest.
    imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SsTables, from earliest to latest.
    l0_sstables: Vec<Arc<SsTable>>,
    /// L1 - L6 SsTables, sorted by key range.
    #[allow(dead_code)]
    levels: Vec<Vec<Arc<SsTable>>>,
    /// The next SSTable ID.
    next_sst_id: usize,
}

impl LsmStorageInner {
    fn create() -> Self {
        Self {
            memtable: Arc::new(MemTable::create()),
            imm_memtables: vec![],
            l0_sstables: vec![],
            levels: vec![],
            next_sst_id: 1,
        }
    }
}

/// The storage interface of the LSM tree.
pub struct LsmStorage {
    inner: Arc<RwLock<Arc<LsmStorageInner>>>,
    flush_lock: Mutex<()>,
    path: PathBuf,
    block_cache: Arc<BlockCache>,
}

impl LsmStorage {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            inner: Arc::new(RwLock::new(Arc::new(LsmStorageInner::create()))),
            flush_lock: Mutex::new(()),
            path: path.as_ref().to_path_buf(),
            block_cache: Arc::new(BlockCache::new(1 << 20)), // 4GB block cache
        })
    }
}

impl KvStore for LsmStorage {
    fn set(&self, key: &[u8], value: Vec<u8>) -> Result<()> {
        assert!(!key.is_empty(), "key cannot be empty");
        assert!(!value.is_empty(), "value cannot be empty");

        let session = self.inner.read();
        session.memtable.set(key, value);

        Ok(())
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let snapshot = {
            let session = self.inner.read();
            Arc::clone(&session)
        };

        // Search in the current memtable.
        if let Some(value) = snapshot.memtable.get(key) {
            match value.is_empty() {
                true => return Ok(None),
                false => return Ok(Some(value)),
            }
        }

        // Search in immutable memtables.
        for memtable in snapshot.imm_memtables.iter().rev() {
            if let Some(value) = memtable.get(key) {
                match value.is_empty() {
                    true => return Ok(None),
                    false => return Ok(Some(value)),
                }
            }
        }

        // Search in SsTables.
        let mut sstable_iters = vec![];
        sstable_iters.reserve(snapshot.l0_sstables.len());
        for sstable in snapshot.l0_sstables.iter().rev() {
            sstable_iters.push(Box::new(
                SsTableIter::create_and_seek_to_key(sstable.clone(), key, true)?
            ));
        }
        let mut merge_iter = MergeIter::create(sstable_iters)?;
        match merge_iter.next() {
            None => Ok(None),
            Some(Err(e)) => Err(e),
            Some(Ok((result_key, value))) => {
                match key == result_key {
                    true => Ok(Some(value)),
                    false => Ok(None),
                }
            },
        }
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        assert!(!key.is_empty(), "key cannot be empty");
        
        let session = self.inner.read();
        session.memtable.set(key, vec![]);

        Ok(())
    }

    fn scan(&self, range: Range) -> Result<KvScan> {
        let snapshot = {
            let session = self.inner.read();
            Arc::clone(&session)
        };

        let mut memtable_iters = vec![];
        memtable_iters.reserve(snapshot.imm_memtables.len() + 1);
        memtable_iters.push(Box::new(snapshot.memtable.scan(range.clone())));
        for memtable in snapshot.imm_memtables.iter().rev() {
            memtable_iters.push(Box::new(memtable.scan(range.clone())));
        }
        let memtable_merge_iter = MergeIter::create(memtable_iters)?;

        let mut sstable_iters = vec![];
        sstable_iters.reserve(snapshot.l0_sstables.len());
        for sstable in snapshot.l0_sstables.iter().rev() {
            sstable_iters.push(Box::new(SsTableIter::create(sstable.clone(), range.clone())?));
        }
        let sstable_merge_iter = MergeIter::create(sstable_iters)?;

        let two_merge_iter = TwoMergeIter::create(
            memtable_merge_iter, sstable_merge_iter
        )?;

        Ok(Box::new(LsmIter::create(two_merge_iter)))
    }

    fn flush(&self) -> Result<()> {
        let _flush_guard = self.flush_lock.lock();

        let memtable_to_flush;
        let sstable_id;

        // Move mutable memtable to immutable memtables.
        {
            let mut session = self.inner.write();

            // Swap the current memtable with a new one.
            let mut snapshot = session.as_ref().clone();
            let memtable = std::mem::replace(
                &mut snapshot.memtable,
                Arc::new(MemTable::create())
            );
            memtable_to_flush = memtable.clone();
            sstable_id = snapshot.next_sst_id;

            // Add the memtable to the immutable memtables.
            snapshot.imm_memtables.push(memtable);

            // Update the snapshot.
            *session = Arc::new(snapshot);
        }

        // At this point, the old memtable should be disabled for write, and all write threads
        // should be operating on the new memtable. We can safely flush the old memtable to
        // disk.

        let mut sstable_builder = SsTableBuilder::new(4096);
        memtable_to_flush.flush(&mut sstable_builder)?;
        let sstable = Arc::new(sstable_builder.build(
            sstable_id, 
            Some(self.block_cache.clone()), 
            self.path.join(format!("{:05}.sst", sstable_id)),
        )?);

        // Add the flushed L0 table to the list.
        {
            let mut session = self.inner.write();
            let mut snapshot = session.as_ref().clone();
            // Remove the memtable from the immutable memtables.
            snapshot.imm_memtables.pop();
            // Add L0 table
            snapshot.l0_sstables.push(sstable);
            // Update SST ID
            snapshot.next_sst_id += 1;
            // Update the snapshot.
            *session = Arc::new(snapshot);
        }

        Ok(())
    }
}

impl Display for LsmStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LsmStorage")
    }
}