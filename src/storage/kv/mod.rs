pub mod b_plus_tree;
pub mod page;

use std::fmt::Display;
use crate::error::Result;

/// A key/value store.
pub trait Store: Display + Send + Sync {
    /// Gets a value for a key, if it exists.
    fn get() -> Result<Option<Vec<u8>>>;

    /// Sets the value for a key, replacing existing value if any.
    fn set() -> Result<()>;

    /// Deletes a key, or does nothing if it does not exists.
    fn delete() -> Result<()>;
}