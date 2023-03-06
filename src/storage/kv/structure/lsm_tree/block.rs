#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;
use bytes::{Bytes, BufMut};

pub const SIZEOF_U16: usize = std::mem::size_of::<u16>();

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted
/// key-value pairs.
pub struct Block {
    data: Vec<u8>,
    offsets: Vec<u16>,
}

impl Block {
    pub fn encode(&self) -> Bytes {
        unimplemented!()
    }

    pub fn decode(data: &[u8]) -> Self {
        unimplemented!()
    }
}

pub struct BlockBuilder {
    data: Vec<u8>,
    offsets: Vec<u16>,
    block_size: usize,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            data: Vec::new(),
            offsets: Vec::new(),
            block_size,
        }
    }

    fn current_size(&self) -> usize {
        self.data.len() + self.offsets.len() * SIZEOF_U16
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> bool {
        assert!(!key.is_empty(), "key must not be empty");
        if self.current_size() + key.len() + value.len() + SIZEOF_U16 * 3 > self.block_size {
            println!("{}", self.current_size() + key.len() + value.len() + SIZEOF_U16 * 3);
            return false;
        }
        self.offsets.push(self.data.len() as u16);
        self.data.put_u16(key.len() as u16);
        self.data.put(key);
        self.data.put_u16(value.len() as u16);
        self.data.put(value);
        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        assert!(!self.is_empty(), "block should not be empty");
        Block {
            data: self.data,
            offsets: self.offsets
        }
    }
}


/// Iterates on a block.
pub struct BlockIterator {
    block: Arc<Block>,
    key: Vec<u8>,
    value: Vec<u8>,
    idx: usize,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: Vec::new(),
            value: Vec::new(),
            idx: 0,
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        unimplemented!()
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: &[u8]) -> Self {
        unimplemented!()
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> &[u8] {
        unimplemented!()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        unimplemented!()
    }

    /// Returns true if the iterator is valid.
    pub fn is_valid(&self) -> bool {
        unimplemented!()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        unimplemented!()
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        unimplemented!()
    }

    /// Seek to the first key that >= `key`.
    pub fn seek_to_key(&mut self, key: &[u8]) {
        unimplemented!()
    }
}



#[test]
fn test_block_build_single_key() {
    let mut builder = BlockBuilder::new(16);
    assert!(builder.add(b"233", b"233333"));
    builder.build();
}

#[test]
fn test_block_build_full() {
    let mut builder = BlockBuilder::new(16);
    assert!(builder.add(b"11", b"11"));
    assert!(!builder.add(b"22", b"22"));
    builder.build();
}

fn key_of(idx: usize) -> Vec<u8> {
    format!("key_{:03}", idx * 5).into_bytes()
}

fn value_of(idx: usize) -> Vec<u8> {
    format!("value_{:010}", idx).into_bytes()
}

fn num_of_keys() -> usize {
    100
}

fn generate_block() -> Block {
    let mut builder = BlockBuilder::new(10000);
    for idx in 0..num_of_keys() {
        let key = key_of(idx);
        let value = value_of(idx);
        assert!(builder.add(&key[..], &value[..]));
    }
    builder.build()
}

#[test]
fn test_block_build_all() {
    generate_block();
}

#[test]
fn test_block_encode() {
    let block = generate_block();
    block.encode();
}

#[test]
fn test_block_decode() {
    let block = generate_block();
    let encoded = block.encode();
    let decoded_block = Block::decode(&encoded);
    assert_eq!(block.offsets, decoded_block.offsets);
    assert_eq!(block.data, decoded_block.data);
}

fn as_bytes(x: &[u8]) -> Bytes {
    Bytes::copy_from_slice(x)
}

#[test]
fn test_block_iterator() {
    let block = Arc::new(generate_block());
    let mut iter = BlockIterator::create_and_seek_to_first(block);
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
            iter.next();
        }
        iter.seek_to_first();
    }
}

#[test]
fn test_block_seek_key() {
    let block = Arc::new(generate_block());
    let mut iter = BlockIterator::create_and_seek_to_key(block, &key_of(0));
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
            iter.seek_to_key(&format!("key_{:03}", i * 5 + offset).into_bytes());
        }
        iter.seek_to_key(b"k");
    }
}
