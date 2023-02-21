mod disk;
mod index;
mod table;

pub mod mvcc;

use std::fmt::Display;

use crate::error::Result;
/// A key/value store.
pub trait Store: Display + Send + Sync {
    /// Sets a value for a key, replacing the existing value if any.
    fn set_or_insert(&mut self, key: &[u8], value: Vec<u8>) -> Result<()>;

    /// Gets a value for a key, if it exists.
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Deletes a key, doing nothing if it does not exist.
    fn delete(&mut self, key: &[u8]) -> Result<()>;

    // /// Iterates over an ordered range of key/value pairs.
    // fn scan(&self, range: Range) -> Scan;

    // /// Flushes any buffered data to the underlying storage medium.
    // fn flush(&mut self) -> Result<()>;
}