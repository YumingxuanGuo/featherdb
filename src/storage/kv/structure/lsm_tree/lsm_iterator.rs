#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::ops::Bound;

use bytes::Bytes;

use crate::error::Result;

use super::{iterators::{StorageIterator, TwoMergeIterator, MergeIterator}, memtable::MemTableIterator, sstable::SsTableIterator};

type LsmIteratorInner =
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>;

pub struct LsmIterator {
    inner_iter: LsmIteratorInner,
    end_bound: Bound<Bytes>,
    is_valid: bool,
}

impl LsmIterator {
    pub(crate) fn new(inner_iter: LsmIteratorInner, end_bound: Bound<Bytes>) -> Result<Self> {
        let mut this = Self {
            is_valid: inner_iter.is_valid(),
            inner_iter,
            end_bound,
        };
        this.move_to_non_delete()?;
        Ok(this)
    }

    fn next_inner(&mut self) -> Result<()> {
        self.inner_iter.next()?;
        if !self.inner_iter.is_valid() {
            self.is_valid = false;
            return Ok(());
        }
        match self.end_bound.as_ref() {
            Bound::Unbounded => {}
            Bound::Included(key) => self.is_valid = self.inner_iter.key() <= key.as_ref(),
            Bound::Excluded(key) => self.is_valid = self.inner_iter.key() < key.as_ref(),
        }
        Ok(())
    }
    
    fn move_to_non_delete(&mut self) -> Result<()> {
        while self.is_valid() && self.inner_iter.value().is_empty() {
            self.next_inner()?;
        }
        Ok(())
    }
}

impl StorageIterator for LsmIterator {
    fn is_valid(&self) -> bool {
        self.is_valid
    }

    fn key(&self) -> &[u8] {
        self.inner_iter.key()
    }

    fn value(&self) -> &[u8] {
        self.inner_iter.value()
    }

    fn next(&mut self) -> Result<()> {
        self.next_inner()?;
        self.move_to_non_delete()
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self { iter }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    fn is_valid(&self) -> bool {
        self.iter.is_valid()
    }

    fn key(&self) -> &[u8] {
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn next(&mut self) -> Result<()> {
        // only move when the iterator is valid
        if self.iter.is_valid() {
            self.iter.next()?;
        }
        Ok(())
    }
}
