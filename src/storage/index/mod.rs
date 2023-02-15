pub mod b_plus_tree;

use crate::{error::Result, common::rid::RID};

type KeyType = i32;
type ValueType = RID;

/// A key/value store.
// pub trait Store: Send + Sync {
pub trait Store: {
    /// Gets a value for a key, if it exists.
    fn get_value(&mut self, key: KeyType) -> Option<ValueType>;

    /// Sets the value for a key, replacing existing value if any.
    fn insert(&mut self, key: &String, value: &Vec<u8>) -> Result<()>;

    /// Deletes a key, or does nothing if it does not exists.
    fn remove() -> Result<()>;
}