use crate::error::Result;

use super::iterators::{TwoMergeIter, MergeIter, StorageIter};
use super::memtable::MemTableIter;
use super::sstable::SsTableIter;

type LsmIterInner = TwoMergeIter<MergeIter<MemTableIter>, MergeIter<SsTableIter>>;

#[derive(Clone)]
pub struct LsmIter {
    inner_iter: LsmIterInner,
}

impl LsmIter {
    pub fn create(inner_iter: LsmIterInner) -> Self {
        Self { inner_iter }
    }
    
    fn try_next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        while let Some((key, value)) = self.inner_iter.try_next()? {
            if !value.is_empty() {
                return Ok(Some((key, value)));
            }
        }
        Ok(None)
    }

    fn try_next_back(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        while let Some((key, value)) = self.inner_iter.try_next_back()? {
            if !value.is_empty() {
                return Ok(Some((key, value)));
            }
        }
        Ok(None)
    }
}

impl Iterator for LsmIter {
    type Item = Result<(Vec<u8>, Vec<u8>)>;
    
    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
    }
}

impl DoubleEndedIterator for LsmIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.try_next_back().transpose()
    }
}



#[cfg(test)]
use tempfile::tempdir;
#[cfg(test)]
use bytes::Bytes;
#[cfg(test)]
use super::memtable::MemTable;
#[cfg(test)]
use super::sstable::{SsTable, SsTableBuilder};
#[cfg(test)]
use crate::storage::kv::Range;

#[cfg(test)]
fn as_bytes(x: &[u8]) -> Bytes {
    Bytes::copy_from_slice(x)
}

#[cfg(test)]
fn key_of(idx: usize) -> Vec<u8> {
    format!("key_{:05}", idx * 5).into_bytes()
}

#[cfg(test)]
fn value_of(idx: usize) -> Vec<u8> {
    format!("value_{:010}", idx).into_bytes()
}

#[cfg(test)]
fn check_result(iter: LsmIter, mut expected: Vec<(Vec<u8>, Vec<u8>)>) {
    check_result_seq(iter.clone(), expected.clone());
    check_result_rev(iter.clone(), {expected.reverse(); expected.clone()});
    check_result_random(iter, {expected.reverse(); expected.clone()});    
}

#[cfg(test)]
fn check_result_seq(iter: LsmIter, expected: Vec<(Vec<u8>, Vec<u8>)>) {
    let mut iter = iter;
    for (k, v) in expected {
        let (key, value) = iter.next().unwrap().unwrap();
        assert_eq!(
            &k,
            &key,
            "expected key: {:?}, actual key: {:?}",
            as_bytes(&k),
            as_bytes(&key),
        );
        assert_eq!(
            &v,
            &value,
            "expected value: {:?}, actual value: {:?}",
            as_bytes(&v),
            as_bytes(&value),
        );
    }
    assert!(iter.next().is_none());
}

#[cfg(test)]
fn check_result_rev(iter: LsmIter, expected: Vec<(Vec<u8>, Vec<u8>)>) {
    let mut iter = iter;
    for (k, v) in expected {
        let (key, value) = iter.next_back().unwrap().unwrap();
        assert_eq!(
            &k,
            &key,
            "expected key: {:?}, actual key: {:?}",
            as_bytes(&k),
            as_bytes(&key),
        );
        assert_eq!(
            &v,
            &value,
            "expected value: {:?}, actual value: {:?}",
            as_bytes(&v),
            as_bytes(&value),
        );
    }
    assert!(iter.next_back().is_none());
}

#[cfg(test)]
fn check_result_random(iter: LsmIter, expected: Vec<(Vec<u8>, Vec<u8>)>) {
    use rand::Rng;
    let mut iter = iter;
    let mut forward = 0;
    let mut backward = 0;
    for _ in 0..expected.len() {
        match rand::thread_rng().gen_range(0..=1) {
            1 => {
                let (key, value) = iter.next().unwrap().unwrap();
                assert_eq!(
                    &key,
                    &expected[forward].0,
                    "expected key: {:?}, actual key: {:?}",
                    as_bytes(&expected[forward].0),
                    as_bytes(&key)
                );
                assert_eq!(
                    &value,
                    &expected[forward].1,
                    "expected value: {:?}, actual value: {:?}",
                    as_bytes(&expected[forward].1),
                    as_bytes(&value)
                );
                forward += 1;
            },

            0 => {
                let (back_key, back_value) = iter.next_back().unwrap().unwrap();
                assert_eq!(
                    &back_key,
                    &expected[expected.len() - backward - 1].0,
                    "expected key: {:?}, actual key: {:?}",
                    as_bytes(&expected[expected.len() - backward - 1].0),
                    as_bytes(&back_key)
                );
                assert_eq!(
                    &back_value,
                    &expected[expected.len() - backward - 1].1,
                    "expected value: {:?}, actual value: {:?}",
                    as_bytes(&expected[expected.len() - backward - 1].1),
                    as_bytes(&back_value)
                );
                backward += 1;
            },

            _ => { assert!(false) },
        }
    }
    assert!(iter.next().is_none());
    assert!(iter.next_back().is_none());
}

#[test]
fn test_lsm_iterator_empty() {
    let memtable_merge_iter = 
        MergeIter::create(Vec::<Box::<MemTableIter>>::new()).unwrap();
    let sstable_merge_iter = 
        MergeIter::create(Vec::<Box::<SsTableIter>>::new()).unwrap();
    let lsm_iterator = LsmIter::create(TwoMergeIter::create(
        memtable_merge_iter, sstable_merge_iter
    ).unwrap());
    check_result(lsm_iterator, vec![]);
}

#[cfg(test)]
fn generate_memtable_mergeiter(scales: Vec<usize>, expected: Vec<(Vec<u8>, Vec<u8>)>) -> MergeIter<MemTableIter> {
    let mut index = 0;
    let mut memtable_iters = vec![];
    for table_size in scales {
        let memtable = MemTable::create();
        for (key, value) in &expected[index..(index + table_size)] {
            memtable.set(&key, value.to_vec());
        }
        memtable_iters.push(Box::new(memtable.scan(Range::from(..))));
        index += table_size;
    }
    MergeIter::create(memtable_iters).unwrap()
}

#[cfg(test)]
fn test_lsm_iterator_multiple_memtable_of_scale(scales: Vec<usize>) {
    use rand::thread_rng;
    use rand::seq::SliceRandom;
    let mut expected = vec![];
    for i in 0..scales.iter().sum() {
        expected.push(((key_of(i)), value_of(i)));
    }
    expected.shuffle(&mut thread_rng());

    let memtable_merge_iter = generate_memtable_mergeiter(scales, expected.clone());
    let sstable_merge_iter = MergeIter::create(Vec::<Box::<SsTableIter>>::new()).unwrap();
    let lsm_iterator = LsmIter::create(TwoMergeIter::create(
        memtable_merge_iter, sstable_merge_iter
    ).unwrap());

    check_result(lsm_iterator.clone(), {expected.sort(); expected}); 
}

#[test]
fn test_lsm_iterator_memtable_only() {
    test_lsm_iterator_multiple_memtable_of_scale(vec![1]);
    test_lsm_iterator_multiple_memtable_of_scale(vec![100]);
    test_lsm_iterator_multiple_memtable_of_scale(vec![10000]);
    test_lsm_iterator_multiple_memtable_of_scale(vec![1, 1]);
    test_lsm_iterator_multiple_memtable_of_scale(vec![1, 2]);
    test_lsm_iterator_multiple_memtable_of_scale(vec![1, 1, 1, 1, 1]);
    test_lsm_iterator_multiple_memtable_of_scale(vec![3, 5, 1, 2, 4]);
    test_lsm_iterator_multiple_memtable_of_scale(vec![30, 50, 1, 2, 40]);
    test_lsm_iterator_multiple_memtable_of_scale(vec![1, 500, 3, 1, 100]);
}

#[cfg(test)]
fn generate_sst(entries: &[(Vec<u8>, Vec<u8>)]) -> SsTable {
    let mut builder = SsTableBuilder::new(128);
    let mut entries = entries.to_vec();
    entries.sort();
    for (key, value) in entries {
        builder.add(&key, &value);
    }
    let dir = tempdir().unwrap();
    let path = dir.path().join("1.sst");
    builder.build_for_test(path).unwrap()
}

#[cfg(test)]
fn generate_sstable_mergeiter(scales: Vec<usize>, expected: Vec<(Vec<u8>, Vec<u8>)>) -> MergeIter<SsTableIter> {
    use std::sync::Arc;
    let mut index = 0;
    let mut sstable_iters = vec![];
    for table_size in scales {
        let sstable = generate_sst(&expected[index..(index + table_size)]);
        sstable_iters.push(Box::new(SsTableIter::new(Arc::new(sstable)).unwrap()));
        index += table_size;
    }
    MergeIter::create(sstable_iters).unwrap()
}

#[cfg(test)]
fn test_lsm_iterator_multiple_sstable_of_scale(scales: Vec<usize>) {
    use rand::thread_rng;
    use rand::seq::SliceRandom;
    let mut expected = vec![];
    for i in 0..scales.iter().sum() {
        expected.push(((key_of(i)), value_of(i)));
    }
    expected.shuffle(&mut thread_rng());

    let memtable_merge_iter = MergeIter::create(Vec::<Box::<MemTableIter>>::new()).unwrap();
    let sstable_merge_iter = generate_sstable_mergeiter(scales, expected.clone());
    let lsm_iterator = LsmIter::create(TwoMergeIter::create(
        memtable_merge_iter, sstable_merge_iter
    ).unwrap());

    check_result(lsm_iterator, {expected.sort(); expected}); 
}

#[test]
fn test_lsm_iterator_sstable_only() {
    test_lsm_iterator_multiple_sstable_of_scale(vec![1]);
    test_lsm_iterator_multiple_sstable_of_scale(vec![100]);
    test_lsm_iterator_multiple_sstable_of_scale(vec![10000]);
    test_lsm_iterator_multiple_sstable_of_scale(vec![1, 1]);
    test_lsm_iterator_multiple_sstable_of_scale(vec![1, 2]);
    test_lsm_iterator_multiple_sstable_of_scale(vec![1, 1, 1, 1, 1]);
    test_lsm_iterator_multiple_sstable_of_scale(vec![3, 5, 1, 2, 4]);
    test_lsm_iterator_multiple_sstable_of_scale(vec![30, 50, 1, 2, 40]);
    test_lsm_iterator_multiple_sstable_of_scale(vec![1, 500, 3, 1, 100]);
}

// Multi-level LSM tree
#[cfg(test)]
fn test_lsm_iterator_hybrid_tables_no_duplicate(memtable_scales: Vec<usize>, sstable_scales: Vec<usize>) {
    use rand::thread_rng;
    use rand::seq::SliceRandom;
    let mut expected = vec![];
    let memtable_entry_num = memtable_scales.iter().sum::<usize>();
    let sstable_entry_num = sstable_scales.iter().sum::<usize>();
    let total_entry_num = memtable_entry_num + sstable_entry_num;
    for i in 0..total_entry_num {
        expected.push(((key_of(i)), value_of(i)));
    }
    expected.shuffle(&mut thread_rng());

    let memtable_expected = expected[0..memtable_entry_num].to_vec();
    let sstable_expected = expected[memtable_entry_num..].to_vec();

    let memtable_merge_iter = generate_memtable_mergeiter(memtable_scales, memtable_expected);
    let sstable_merge_iter = generate_sstable_mergeiter(sstable_scales, sstable_expected);
    let lsm_iterator = LsmIter::create(TwoMergeIter::create(
        memtable_merge_iter, sstable_merge_iter
    ).unwrap());

    check_result(lsm_iterator, {expected.sort(); expected}); 
}

#[test]
fn test_lsm_iterator_hybrid_tables() {
    test_lsm_iterator_hybrid_tables_no_duplicate(vec![1], vec![1]);
    test_lsm_iterator_hybrid_tables_no_duplicate(vec![100], vec![1]);
    test_lsm_iterator_hybrid_tables_no_duplicate(vec![1], vec![100]);
    test_lsm_iterator_hybrid_tables_no_duplicate(vec![100], vec![100]);
    test_lsm_iterator_hybrid_tables_no_duplicate(vec![1, 1], vec![1, 1, 1, 1, 1]);
    test_lsm_iterator_hybrid_tables_no_duplicate(vec![1, 2], vec![1, 1, 1, 1, 1]);
    test_lsm_iterator_hybrid_tables_no_duplicate(vec![1, 1, 1, 1, 1], vec![1, 1]);
    test_lsm_iterator_hybrid_tables_no_duplicate(vec![3, 5, 1, 2, 4], vec![1, 2]);
    test_lsm_iterator_hybrid_tables_no_duplicate(vec![30, 50, 1, 2, 40], vec![1, 500, 3, 1, 100]);
}

// Duplicate keys
#[cfg(test)]
fn test_lsm_iterator_hybrid_tables_with_duplicate(memtable_scales: Vec<usize>, sstable_scales: Vec<usize>) {
    use std::collections::HashSet;
    use rand::thread_rng;
    use rand::seq::{SliceRandom, IteratorRandom};
    let mut expected = vec![];
    let memtable_entry_num = memtable_scales.iter().sum::<usize>();
    let sstable_entry_num = sstable_scales.iter().sum::<usize>();
    let total_entry_num = memtable_entry_num + sstable_entry_num;
    for i in 0..total_entry_num {
        expected.push(((key_of(i)), value_of(i)));
    }
    expected.shuffle(&mut thread_rng());

    let mut memtable_expected = expected.clone().into_iter().choose_multiple(&mut thread_rng(), memtable_entry_num);
    let mut sstable_expected = expected.clone().into_iter().choose_multiple(&mut thread_rng(), sstable_entry_num);

    let memtable_merge_iter = generate_memtable_mergeiter(memtable_scales, memtable_expected.clone());
    let sstable_merge_iter = generate_sstable_mergeiter(sstable_scales, sstable_expected.clone());
    let lsm_iterator = LsmIter::create(TwoMergeIter::create(
        memtable_merge_iter, sstable_merge_iter
    ).unwrap());

    memtable_expected.append(&mut sstable_expected);
    let mut unique_expected = memtable_expected.into_iter().collect::<HashSet<_>>().into_iter().collect::<Vec<_>>();
    unique_expected.sort();

    check_result(lsm_iterator, unique_expected); 
}

#[test]
fn test_lsm_iterator_with_duplicate_keys() {
    test_lsm_iterator_hybrid_tables_with_duplicate(vec![1], vec![1]);
    test_lsm_iterator_hybrid_tables_with_duplicate(vec![100], vec![1]);
    test_lsm_iterator_hybrid_tables_with_duplicate(vec![1], vec![100]);
    test_lsm_iterator_hybrid_tables_with_duplicate(vec![100], vec![100]);
    test_lsm_iterator_hybrid_tables_with_duplicate(vec![1, 1], vec![1, 1, 1, 1, 1]);
    test_lsm_iterator_hybrid_tables_with_duplicate(vec![1, 2], vec![1, 1, 1, 1, 1]);
    test_lsm_iterator_hybrid_tables_with_duplicate(vec![1, 1, 1, 1, 1], vec![1, 1]);
    test_lsm_iterator_hybrid_tables_with_duplicate(vec![3, 5, 1, 2, 4], vec![1, 2]);
    test_lsm_iterator_hybrid_tables_with_duplicate(vec![300, 500, 1, 2, 40], vec![1, 500, 3, 1, 100]);
}


// Key range iteration
// Iteration order
// Concurrent modifications
// Large dataset
// Error handling
// Iterator resource management