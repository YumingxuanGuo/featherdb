#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::{RwLock, Mutex};

use crate::error::Result;
use super::block::Block;
use super::iterators::{MergeIterator, StorageIterator, TwoMergeIterator};
use super::lsm_iterator::{FusedIterator, LsmIterator};
use super::memtable::{MemTable, map_bound};
use super::sstable::{SsTable, SsTableIterator, SsTableBuilder};

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

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
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

        // Search in sorted string tables.
        let mut sstable_iters = Vec::new();
        sstable_iters.reserve(snapshot.l0_sstables.len());
        for sstable in snapshot.l0_sstables.iter().rev() {
            sstable_iters.push(Box::new(SsTableIterator::create_and_seek_to_key(
                sstable.clone(), 
                key,
            )?));
        }
        let merge_iter = MergeIterator::create(sstable_iters);
        match merge_iter.is_valid() {
            true => Ok(Some(Bytes::copy_from_slice(merge_iter.value()))),
            false => Ok(None),
        }
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        assert!(!value.is_empty(), "value cannot be empty");
        assert!(!key.is_empty(), "key cannot be empty");

        let session = self.inner.read();
        session.memtable.put(key, value);

        Ok(())
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        assert!(!key.is_empty(), "key cannot be empty");
        
        let session = self.inner.read();
        session.memtable.put(key, b"");

        Ok(())
    }

    /// Persist data to disk.
    ///
    /// In day 3: flush the current memtable to disk as L0 SST.
    /// In day 6: call `fsync` on WAL.
    pub fn sync(&self) -> Result<()> {
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

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        let snapshot = {
            let session = self.inner.read();
            Arc::clone(&session)
        };

        let mut memtable_iters = Vec::new();
        memtable_iters.reserve(snapshot.imm_memtables.len() + 1);
        memtable_iters.push(Box::new(snapshot.memtable.scan(lower, upper)));
        for memtable in snapshot.imm_memtables.iter().rev() {
            memtable_iters.push(Box::new(memtable.scan(lower, upper)));
        }
        let memtable_merge_iter = MergeIterator::create(memtable_iters);

        let mut sstable_iters = Vec::new();
        sstable_iters.reserve(snapshot.l0_sstables.len());
        for sstable in snapshot.l0_sstables.iter().rev() {
            let iter = match lower {
                Bound::Included(key) => {
                    SsTableIterator::create_and_seek_to_key(sstable.clone(), key)?
                },
                Bound::Excluded(key) => {
                    let mut iter =
                        SsTableIterator::create_and_seek_to_key(sstable.clone(), key)?;
                    if iter.is_valid() && iter.key() == key {
                        iter.next()?;
                    }
                    iter
                },
                Bound::Unbounded => SsTableIterator::create_and_seek_to_first(sstable.clone())?,
            };
            sstable_iters.push(Box::new(iter));
        }
        let sstable_merge_iter = MergeIterator::create(sstable_iters);

        let two_merge_iter =
            TwoMergeIterator::create(memtable_merge_iter, sstable_merge_iter)?;

        Ok(FusedIterator::new(LsmIterator::new(two_merge_iter, map_bound(upper))?))
    }
}
