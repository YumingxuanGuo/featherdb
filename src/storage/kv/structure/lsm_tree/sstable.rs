#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::path::Path;
use std::sync::Arc;

use bytes::{Buf, Bytes, BufMut};

use crate::error::Result;
use super::block::{Block, BlockBuilder, BlockIterator};
use super::iterator::StorageIterator;
use super::lsm_storage::BlockCache;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: Bytes,
}

/// Data alignment: 
/// 
///     |                        meta_entry_1                          |
///     | offset (4B) | first_key_len (2B) | first_key (first_key_len) | ... |
/// 
impl BlockMeta {
    /// Encode block meta to a buffer.
    pub fn encode_block_meta(block_meta: &[BlockMeta], buffer: &mut Vec<u8>) {
        let mut meta_size = 0;
        for meta in block_meta {
            meta_size += std::mem::size_of::<u32>();
            meta_size += std::mem::size_of::<u16>();
            meta_size += meta.first_key.len();
        }
        buffer.reserve(meta_size);
        let original_len = buffer.len();
        for meta in block_meta {
            buffer.put_u32(meta.offset as u32);
            buffer.put_u16(meta.first_key.len() as u16);
            buffer.put_slice(&meta.first_key);
        }
        assert_eq!(meta_size + original_len, buffer.len());
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(mut buffer: impl Buf) -> Vec<BlockMeta> {
        let mut block_meta = Vec::new();
        while buffer.has_remaining() {
            let offset = buffer.get_u32() as usize;
            let first_key_len = buffer.get_u16() as usize;
            let first_key = buffer.copy_to_bytes(first_key_len);
            block_meta.push(BlockMeta { offset, first_key });
        }
        block_meta
    }
}

/// A file object.
pub struct FileObject(Bytes);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        Ok(self.0[offset as usize..(offset + len) as usize].to_vec())
    }

    pub fn size(&self) -> u64 {
        self.0.len() as u64
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        Ok(FileObject(data.into()))
    }

    pub fn open(path: &Path) -> Result<Self> {
        unimplemented!()
    }
}

pub struct SsTable {
    id: usize,
    file: FileObject,
    block_metas: Vec<BlockMeta>,
    block_meta_offset: usize,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    /// 
    /// Data alignment: 
    /// 
    ///     | data block | data block | ... | data block | meta block | meta block offset (u32) |
    /// 
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let file_len = file.size();
        let meta_offset_raw = file.read(file_len - 4, 4)?;
        let block_meta_offset = (&meta_offset_raw[..]).get_u32() as u64;
        let meta_raw = file.read(block_meta_offset, file_len - 4 - block_meta_offset)?;
        let block_metas = BlockMeta::decode_block_meta(&meta_raw[..]);
        Ok(Self {
            id,
            file,
            block_metas,
            block_meta_offset: block_meta_offset as usize,
        })
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        let block_offset = self.block_metas[block_idx].offset;
        let block_end = self
            .block_metas
            .get(block_idx + 1)
            .map_or(self.block_meta_offset, |meta| meta.offset);
        let block_len = block_end - block_offset;
        let block_raw = self.file.read(block_offset as u64, block_len as u64)?;
        Ok(Arc::new(Block::decode(&block_raw)))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        self.read_block(block_idx)
    }

    /// Find the block that may contain `key`.
    pub fn find_block_idx(&self, key: &[u8]) -> usize {
        self.block_metas
            .partition_point(|meta| meta.first_key <= key)
            .saturating_sub(1)
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_metas.len()
    }
}

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    pub(super) meta: Vec<BlockMeta>,
    data: Vec<u8>,
    cur_block_first_key: Vec<u8>,
    block_builder: BlockBuilder,
    block_size: usize,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            meta: Vec::new(),
            data: Vec::new(),
            cur_block_first_key: Vec::new(),
            block_builder: BlockBuilder::new(block_size),
            block_size,
        }
    }

    /// Adds a key-value pair to SSTable
    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        if self.cur_block_first_key.is_empty() {
            self.cur_block_first_key = key.into();
        }
        if !self.block_builder.add(key, value) {
            self.finalize_block();
            assert!(self.block_builder.add(key, value));
            self.cur_block_first_key = key.into();
        }
    }

    fn finalize_block(&mut self) {
        let old_builder = 
            std::mem::replace(&mut self.block_builder, BlockBuilder::new(self.block_size));
        let encoded_block = old_builder.build().encode();
        self.meta.push(BlockMeta {
            offset: self.data.len(),
            first_key: self.cur_block_first_key.clone().into(),
        });
        self.data.extend(encoded_block);
    }

    /// Get the estimated size of the SSTable.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. No need to actually write to disk until
    /// chapter 4 block cache.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        self.finalize_block();
        let mut sst_data = self.data;
        let block_meta_offset = sst_data.len();
        BlockMeta::encode_block_meta(&self.meta, &mut sst_data);
        sst_data.put_u32(block_meta_offset as u32);
        let file = FileObject::create(path.as_ref(), sst_data)?;
        Ok(SsTable {
            id,
            file,
            block_metas: self.meta,
            block_meta_offset,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    block_iter: BlockIterator,
    block_idx: usize,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let block = table.read_block_cached(0)?;
        Ok(Self {
            table,
            block_iter: BlockIterator::create_and_seek_to_first(block),
            block_idx: 0
        })
    }

    /// Seek to the first key-value pair.
    pub fn seek_to_first(&mut self) -> Result<()> {
        let block = self.table.read_block_cached(0)?;
        self.block_iter = BlockIterator::create_and_seek_to_first(block);
        self.block_idx = 0;
        Ok(())
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: &[u8]) -> Result<Self> {
        let mut this = SsTableIterator::create_and_seek_to_first(table)?;
        this.seek_to_key(key)?;
        Ok(this)
    }

    /// Seek to the first key-value pair which >= `key`.
    pub fn seek_to_key(&mut self, key: &[u8]) -> Result<()> {
        let mut block_idx = self.table.find_block_idx(key);
        let mut block_iter = 
            BlockIterator::create_and_seek_to_key(self.table.read_block_cached(block_idx)?, key);
        if !block_iter.is_valid() {
            block_idx += 1;
            if block_idx < self.table.num_of_blocks() {
                block_iter = 
                    BlockIterator::create_and_seek_to_key(self.table.read_block_cached(block_idx)?, key);
            }
        }
        self.block_idx = block_idx;
        self.block_iter = block_iter;
        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    fn value(&self) -> &[u8] {
        self.block_iter.value()
    }

    fn key(&self) -> &[u8] {
        self.block_iter.key()
    }

    fn is_valid(&self) -> bool {
        self.block_iter.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        self.block_iter.next();
        if !self.block_iter.is_valid() {
            self.block_idx += 1;
            if self.block_idx < self.table.num_of_blocks() {
                self.block_iter = BlockIterator::create_and_seek_to_first(
                    self.table.read_block_cached(self.block_idx)?
                );
            }
        }
        Ok(())
    }
}



#[cfg(test)]
use tempfile::{tempdir, TempDir};

#[test]
fn test_sst_build_single_key() {
    let mut builder = SsTableBuilder::new(16);
    builder.add(b"233", b"233333");
    let dir = tempdir().unwrap();
    builder.build_for_test(dir.path().join("1.sst")).unwrap();
}

#[test]
fn test_sst_build_two_blocks() {
    let mut builder = SsTableBuilder::new(16);
    builder.add(b"11", b"11");
    builder.add(b"22", b"22");
    builder.add(b"33", b"11");
    builder.add(b"44", b"22");
    builder.add(b"55", b"11");
    builder.add(b"66", b"22");
    assert!(builder.meta.len() >= 2);
    let dir = tempdir().unwrap();
    builder.build_for_test(dir.path().join("1.sst")).unwrap();
}

#[cfg(test)]
fn key_of(idx: usize) -> Vec<u8> {
    format!("key_{:03}", idx * 5).into_bytes()
}

#[cfg(test)]
fn value_of(idx: usize) -> Vec<u8> {
    format!("value_{:010}", idx).into_bytes()
}

#[cfg(test)]
fn num_of_keys() -> usize {
    100
}

#[cfg(test)]
fn generate_sst() -> (TempDir, SsTable) {
    let mut builder = SsTableBuilder::new(128);
    for idx in 0..num_of_keys() {
        let key = key_of(idx);
        let value = value_of(idx);
        builder.add(&key[..], &value[..]);
    }
    let dir = tempdir().unwrap();
    let path = dir.path().join("1.sst");
    (dir, builder.build_for_test(path).unwrap())
}

#[test]
fn test_sst_build_all() {
    generate_sst();
}

#[test]
fn test_sst_decode() {
    let (_dir, sst) = generate_sst();
    let meta = sst.block_metas.clone();
    let new_sst = SsTable::open_for_test(sst.file).unwrap();
    assert_eq!(new_sst.block_metas, meta);
}

#[cfg(test)]
fn as_bytes(x: &[u8]) -> Bytes {
    Bytes::copy_from_slice(x)
}

#[test]
fn test_sst_iterator() {
    let (_dir, sst) = generate_sst();
    let sst = Arc::new(sst);
    let mut iter = SsTableIterator::create_and_seek_to_first(sst).unwrap();
    for _ in 0..5 {
        for i in 0..num_of_keys() {
            let key = iter.key();
            let value = iter.value();
            assert_eq!(
                key,
                key_of(i),
                "expected key: {:?}, actual key: {:?}",
                as_bytes(&key_of(i)),
                as_bytes(key)
            );
            assert_eq!(
                value,
                value_of(i),
                "expected value: {:?}, actual value: {:?}",
                as_bytes(&value_of(i)),
                as_bytes(value)
            );
            iter.next().unwrap();
        }
        iter.seek_to_first().unwrap();
    }
}

#[test]
fn test_sst_seek_key() {
    let (_dir, sst) = generate_sst();
    let sst = Arc::new(sst);
    let mut iter = SsTableIterator::create_and_seek_to_key(sst, &key_of(0)).unwrap();
    for offset in 1..=5 {
        for i in 0..num_of_keys() {
            let key = iter.key();
            let value = iter.value();
            assert_eq!(
                key,
                key_of(i),
                "expected key: {:?}, actual key: {:?}",
                as_bytes(&key_of(i)),
                as_bytes(key)
            );
            assert_eq!(
                value,
                value_of(i),
                "expected value: {:?}, actual value: {:?}",
                as_bytes(&value_of(i)),
                as_bytes(value)
            );
            iter.seek_to_key(&format!("key_{:03}", i * 5 + offset).into_bytes())
                .unwrap();
        }
        iter.seek_to_key(b"k").unwrap();
    }
}
