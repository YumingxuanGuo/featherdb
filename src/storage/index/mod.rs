pub mod b_plus_tree;

use std::fmt::Display;
use crate::error::Result;

/// A key/value store.
pub trait Store: Send + Sync {
    /// Gets a value for a key, if it exists.
    fn get_value() -> Result<Option<Vec<u8>>>;

    /// Sets the value for a key, replacing existing value if any.
    fn insert(&self, key: &String, value: &Vec<u8>) -> Result<()>;

    /// Deletes a key, or does nothing if it does not exists.
    fn remove() -> Result<()>;
}