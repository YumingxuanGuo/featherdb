use std::ops::Bound;
use std::sync::Arc;

use crate::error::Result;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use crossbeam_skiplist::map::Entry;
use ouroboros::self_referencing;

use super::iterators::{StorageIterator, StorageIter};
use super::sstable::SsTableBuilder;

/// A basic mem-table based on crossbeam-skiplist
pub struct MemTable {
    map: Arc<SkipMap<Vec<u8>, Vec<u8>>>,
}

pub(crate) fn map_bound(bound: Bound<&[u8]>) -> Bound<Vec<u8>> {
    match bound {
        Bound::Included(x) => Bound::Included(x.to_vec()),
        Bound::Excluded(x) => Bound::Excluded(x.to_vec()),
        Bound::Unbounded => Bound::Unbounded,
    }
}

impl MemTable {
    /// Create a new mem-table.
    pub fn create() -> Self {
        Self { map: Arc::new(SkipMap::new()) }
    }

    /// Get a value by key.
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.map.get(key).map(|entry| entry.value().clone())
    }

    /// Put a key-value pair into the mem-table.
    pub fn put(&self, key: &[u8], value: &[u8]) {
        self.map.insert(
            key.to_vec(), 
            value.to_vec(),
        );
    }

    /// Get an iterator over a range of keys.
    pub fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> MemTableIter {
        let bound = (map_bound(lower), map_bound(upper));
        MemTableIter::create(self.map.clone(), bound)
    }

    /// Flush the mem-table to SSTable.
    pub fn flush(&self, builder: &mut SsTableBuilder) -> Result<()> {
        for entry in self.map.iter() {
            builder.add(&entry.key()[..], &entry.value()[..]);
        }
        Ok(())
    }
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Vec<u8>, (Bound<Vec<u8>>, Bound<Vec<u8>>), Vec<u8>, Vec<u8>>;

/// An iterator over a range of `SkipMap`.
#[self_referencing]
pub struct MemTableIter {
    map: Arc<SkipMap<Vec<u8>, Vec<u8>>>,
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    bound: (Bound<Vec<u8>>, Bound<Vec<u8>>),
    front_entry: Option<(Vec<u8>, Vec<u8>)>,
    back_entry: Option<(Vec<u8>, Vec<u8>)>,
    is_valid: bool,
}

impl Clone for MemTableIter {
    fn clone(&self) -> Self {
        Self::create(self.borrow_map().clone(), self.borrow_bound().clone())
    }

    fn clone_from(&mut self, _source: &Self) {
        unimplemented!()
    }
}

impl MemTableIter {
    fn create(map: Arc<SkipMap<Vec<u8>, Vec<u8>>>, bound: (Bound<Vec<u8>>, Bound<Vec<u8>>)) -> Self {
        let mut mem_table_iter = MemTableIterBuilder {
            map: map.clone(),
            iter_builder: |map| map.range(bound.clone()),
            bound: bound.clone(),
            front_entry: None,
            back_entry: None,
            is_valid: false,
        }.build();
        mem_table_iter.with_mut(|this| *this.is_valid = map.range(bound).next().is_some());
        mem_table_iter
    }

    fn entry_to_item(entry: Option<Entry<'_, Vec<u8>, Vec<u8>>>) -> Option<(Vec<u8>, Vec<u8>)> {
        entry
            .map(|e| Some((e.key().clone(), e.value().clone())))
            .unwrap_or_else(|| None)
    }
}

impl StorageIter for MemTableIter {
    fn front_entry(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        self.borrow_front_entry().clone()
    }

    fn back_entry(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        self.borrow_back_entry().clone()
    }

    fn is_valid(&self) -> bool {
        self.borrow_is_valid().clone()
    }

    fn try_next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let entry = self.with_iter_mut(
            |iter| MemTableIter::entry_to_item(iter.next())
        );
        self.with_mut(|this| *this.front_entry = entry.clone());
        if entry.is_none() {
            self.with_mut(|this| *this.is_valid = false);
        }
        Ok(entry)
    }

    fn try_next_back(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let entry = self.with_iter_mut(
            |iter| MemTableIter::entry_to_item(iter.next_back())
        );
        self.with_mut(|this| *this.back_entry = entry.clone());
        if entry.is_none() {
            self.with_mut(|this| *this.is_valid = false);
        }
        Ok(entry)
    }
}

impl Iterator for MemTableIter {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
    }
}

impl DoubleEndedIterator for MemTableIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.try_next_back().transpose()
    }
}



#[cfg(test)]
use tempfile::tempdir;
#[cfg(test)]
use super::sstable::SsTableIterator;

#[test]
fn test_memtable_get() {
    let memtable = MemTable::create();
    memtable.put(b"key1", b"value1");
    memtable.put(b"key2", b"value2");
    memtable.put(b"key3", b"value3");
    assert_eq!(&memtable.get(b"key1").unwrap()[..], b"value1");
    assert_eq!(&memtable.get(b"key2").unwrap()[..], b"value2");
    assert_eq!(&memtable.get(b"key3").unwrap()[..], b"value3");
}

#[test]
fn test_memtable_overwrite() {
    let memtable = MemTable::create();
    memtable.put(b"key1", b"value1");
    memtable.put(b"key2", b"value2");
    memtable.put(b"key3", b"value3");
    memtable.put(b"key1", b"value11");
    memtable.put(b"key2", b"value22");
    memtable.put(b"key3", b"value33");
    assert_eq!(&memtable.get(b"key1").unwrap()[..], b"value11");
    assert_eq!(&memtable.get(b"key2").unwrap()[..], b"value22");
    assert_eq!(&memtable.get(b"key3").unwrap()[..], b"value33");
}

#[test]
fn test_memtable_flush() {
    let memtable = MemTable::create();
    memtable.put(b"key1", b"value1");
    memtable.put(b"key2", b"value2");
    memtable.put(b"key3", b"value3");
    let mut builder = SsTableBuilder::new(128);
    memtable.flush(&mut builder).unwrap();
    let dir = tempdir().unwrap();
    let sst = builder.build_for_test(dir.path().join("1.sst")).unwrap();
    let mut iter = SsTableIterator::create_and_seek_to_first(sst.into()).unwrap();
    assert_eq!(iter.key(), b"key1");
    assert_eq!(iter.value(), b"value1");
    iter.next().unwrap();
    assert_eq!(iter.key(), b"key2");
    assert_eq!(iter.value(), b"value2");
    iter.next().unwrap();
    assert_eq!(iter.key(), b"key3");
    assert_eq!(iter.value(), b"value3");
    iter.next().unwrap();
    assert!(!iter.is_valid());
}

#[test]
fn test_memtable_iter() {
    use std::ops::Bound;
    let memtable = MemTable::create();
    memtable.put(b"key1", b"value1");
    memtable.put(b"key2", b"value2");
    memtable.put(b"key3", b"value3");

    {
        let mut iter = memtable.scan(Bound::Unbounded, Bound::Unbounded);
        iter.next().unwrap().unwrap();
        let (key, value) = iter.front_entry().unwrap();
        assert_eq!(key, b"key1");
        assert_eq!(value, b"value1");
        iter.next_back().unwrap().unwrap();
        let (key, value) = iter.back_entry().unwrap();
        assert_eq!(key, b"key3");
        assert_eq!(value, b"value3");
        iter.next().unwrap().unwrap();
        let (key, value) = iter.front_entry().unwrap();
        assert_eq!(key, b"key2");
        assert_eq!(value, b"value2");
        // assert!(!iter.is_valid());
    }

    {
        let mut iter = memtable.scan(Bound::Included(b"key1"), Bound::Included(b"key2"));
        iter.next().unwrap().unwrap();
        let (key, value) = iter.front_entry().unwrap();
        assert_eq!(key, b"key1");
        assert_eq!(value, b"value1");
        iter.next().unwrap().unwrap();
        let (key, value) = iter.front_entry().unwrap();
        assert_eq!(key, b"key2");
        assert_eq!(value, b"value2");
        // assert!(!iter.is_valid());
    }

    {
        let mut iter = memtable.scan(Bound::Excluded(b"key1"), Bound::Excluded(b"key3"));
        iter.next().unwrap().unwrap();
        let (key, value) = iter.front_entry().unwrap();
        assert_eq!(key, b"key2");
        assert_eq!(value, b"value2");
        // assert!(!iter.is_valid());
    }
}



// ============================================================================================= //



/// An iterator over a range of `SkipMap`.
#[self_referencing]
pub struct MemTableIterator {
    map: Arc<SkipMap<Bytes, Bytes>>,
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    item: (Bytes, Bytes),
}

impl MemTableIterator {
    fn entry_to_item(entry: Option<Entry<'_, Bytes, Bytes>>) -> (Bytes, Bytes) {
        entry
            .map(|e| (e.key().clone(), e.value().clone()))
            .unwrap_or_else(|| (Bytes::from_static(&[]), Bytes::from_static(&[])))
    }
}

impl StorageIterator for MemTableIterator {
    fn value(&self) -> &[u8] {
        &self.borrow_item().1[..]
    }

    fn key(&self) -> &[u8] {
        &self.borrow_item().0[..]
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        // let entry = self.with_iter_mut(
        //     |iter| MemTableIterator::entry_to_item(iter.next())
        // );
        // self.with_mut(|this| *this.item = entry);
        Ok(())
    }
}