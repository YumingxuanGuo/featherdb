mod disk;
mod index;
mod table;
mod data_structures;

use std::fmt::Display;
use std::ops::{Bound, RangeBounds};

use crate::error::Result;
use crate::common::{KeyType, ValueType};

/// A key/value store.
pub trait Store: Display + Send + Sync {
    /// Sets a value for a key, replacing the existing value if any.
    fn set_or_insert(&mut self, key: &[u8], value: Vec<u8>) -> Result<()>;

    /// Gets a value for a key, if it exists.
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Deletes a key, doing nothing if it does not exist.
    fn delete(&mut self, key: &[u8]) -> Result<()>;

    /// Iterates over an ordered range of key/value pairs.
    fn scan(&self, range: Range) -> Scan;

    /// Flushes any buffered data to the underlying storage medium.
    fn flush(&mut self) -> Result<()>;
}

/// A scan range wrapper.
pub struct Range {
    start: Bound<KeyType>,
    end: Bound<KeyType>,
}

impl Range {
    /// std::ops::Range does not support inclusive range bounds.
    pub fn from<R: RangeBounds<KeyType>>(range: R) -> Self {
        Self {
            start: match range.start_bound() {
                Bound::Included(v) => Bound::Included(v.to_vec()),
                Bound::Excluded(v) => Bound::Excluded(v.to_vec()),
                Bound::Unbounded => Bound::Unbounded,
            },
            end: match range.start_bound() {
                Bound::Included(v) => Bound::Included(v.to_vec()),
                Bound::Excluded(v) => Bound::Excluded(v.to_vec()),
                Bound::Unbounded => Bound::Unbounded,
            },
        }
    }
}

/// Iterator over a key/value range.
pub type Scan = Box<dyn DoubleEndedIterator<Item = Result<(KeyType, ValueType)>> + Send>;