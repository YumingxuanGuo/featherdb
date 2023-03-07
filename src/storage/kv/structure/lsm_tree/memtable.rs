use std::ops::Bound;
use std::sync::Arc;

use crate::error::Result;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use crossbeam_skiplist::map::Entry;
use ouroboros::self_referencing;

use super::iterators::StorageIterator;
use super::sstable::SsTableBuilder;

/// A basic mem-table based on crossbeam-skiplist
pub struct MemTable {
    map: Arc<SkipMap<Bytes, Bytes>>,
}

pub(crate) fn map_bound(bound: Bound<&[u8]>) -> Bound<Bytes> {
    match bound {
        Bound::Included(x) => Bound::Included(Bytes::copy_from_slice(x)),
        Bound::Excluded(x) => Bound::Excluded(Bytes::copy_from_slice(x)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

impl MemTable {
    /// Create a new mem-table.
    pub fn create() -> Self {
        Self { map: Arc::new(SkipMap::new()) }
    }

    /// Get a value by key.
    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        self.map.get(key).map(|entry| entry.value().clone())
    }

    /// Put a key-value pair into the mem-table.
    pub fn put(&self, key: &[u8], value: &[u8]) {
        self.map.insert(
            Bytes::copy_from_slice(key), 
            Bytes::copy_from_slice(value)
        );
    }

    /// Get an iterator over a range of keys.
    pub fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> MemTableIterator {
        let (lower, upper) = (map_bound(lower), map_bound(upper));
        let mut mem_table_iter = MemTableIteratorBuilder {
            map: self.map.clone(),
            iter_builder: |map| map.range((lower, upper)),
            item: (Bytes::from_static(&[]), Bytes::from_static(&[])),
        }.build();
        let entry = mem_table_iter.with_iter_mut(|iter| MemTableIterator::entry_to_item(iter.next()));
        mem_table_iter.with_mut(|this| *this.item = entry);
        mem_table_iter
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
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

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
        let entry = self.with_iter_mut(
            |iter| MemTableIterator::entry_to_item(iter.next())
        );
        self.with_mut(|this| *this.item = entry);
        Ok(())
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

    {
        let mut iter = memtable.scan(Bound::Included(b"key1"), Bound::Included(b"key2"));
        assert_eq!(iter.key(), b"key1");
        assert_eq!(iter.value(), b"value1");
        iter.next().unwrap();
        assert_eq!(iter.key(), b"key2");
        assert_eq!(iter.value(), b"value2");
        iter.next().unwrap();
        assert!(!iter.is_valid());
    }

    {
        let mut iter = memtable.scan(Bound::Excluded(b"key1"), Bound::Excluded(b"key3"));
        assert_eq!(iter.key(), b"key2");
        assert_eq!(iter.value(), b"value2");
        iter.next().unwrap();
        assert!(!iter.is_valid());
    }
}