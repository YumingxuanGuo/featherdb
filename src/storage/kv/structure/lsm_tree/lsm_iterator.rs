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
}

impl StorageIter for LsmIter {
    fn front_entry(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        self.inner_iter.front_entry()
    }

    fn back_entry(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        self.inner_iter.back_entry()
    }

    fn is_valid(&self) -> bool {
        self.inner_iter.is_valid()
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