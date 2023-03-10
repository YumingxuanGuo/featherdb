use std::{sync::Arc, usize};
use bytes::{Bytes, BufMut, Buf};

pub const SIZEOF_U16: usize = std::mem::size_of::<u16>();

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted
/// key-value pairs.
pub struct Block {
    data: Vec<u8>,
    offsets: Vec<u16>,
}

impl Block {
    pub fn encode(&self) -> Bytes {
        let mut buffer = self.data.clone();
        for offset in &self.offsets {
            buffer.put_u16(*offset);
        }
        buffer.put_u16(self.offsets.len() as u16);
        buffer.into()
    }

    pub fn decode(data: &[u8]) -> Self {
        let offset_tail = data.len() - SIZEOF_U16;
        let num_elements = (&data[offset_tail..]).get_u16() as usize;
        let offset_head = data.len() - SIZEOF_U16 * num_elements - SIZEOF_U16;
        let offsets_raw = &data[offset_head..offset_tail];
        let data_raw = &data[..offset_head];
        Self {
            data: data_raw.into(),
            offsets: offsets_raw
                .chunks(SIZEOF_U16)
                .map(|mut iter| iter.get_u16())
                .collect()
        }
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
        self.data.len() + self.offsets.len() * SIZEOF_U16 + SIZEOF_U16
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> bool {
        assert!(!key.is_empty(), "key must not be empty");
        if self.current_size() + key.len() + value.len() + SIZEOF_U16 * 3 > self.block_size 
            && !self.is_empty()
        {
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
        let mut iter = BlockIterator::new(block);
        iter.seek_to_first();
        iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: &[u8]) -> Self {
        let mut iter = BlockIterator::new(block);
        iter.seek_to_key(key);
        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> &[u8] {
        debug_assert!(!self.key.is_empty(), "invalid iterator");
        &self.key
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        debug_assert!(!self.key.is_empty(), "invalid iterator");
        &self.value
    }

    /// Returns true if the iterator is valid.
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    fn seek_to_idx(&mut self, idx: usize) {
        if idx >= self.block.offsets.len() {
            self.key.clear();
            self.value.clear();
            return;
        }
        let offset = self.block.offsets[idx] as usize;
        let mut entry = &self.block.data[offset..];
        let key_len = entry.get_u16() as usize;
        self.key = entry[..key_len].into();
        entry.advance(key_len);
        let value_len = entry.get_u16() as usize;
        self.value = entry[..value_len].into();
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.idx = 0;
        self.seek_to_idx(self.idx);
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.idx += 1;
        self.seek_to_idx(self.idx);
    }

    /// Seek to the first key that >= `key`.
    pub fn seek_to_key(&mut self, key: &[u8]) {
        let mut low = 0;
        let mut high = self.block.offsets.len();
        while low < high {
            let mid = (low + high) / 2;
            self.seek_to_idx(mid);
            assert!(self.is_valid());
            match self.key().cmp(key) {
                std::cmp::Ordering::Less => low = mid + 1,
                std::cmp::Ordering::Equal => {
                    self.idx = low;
                    return;
                }
                std::cmp::Ordering::Greater => high = mid,
            }
        }
        self.seek_to_idx(low);
        self.idx = low;
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

#[cfg(test)]
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
