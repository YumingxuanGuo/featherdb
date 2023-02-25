use serde::{Serialize, Deserialize};

use crate::error::Result;

pub mod rid;

pub type KeyType = Vec<u8>;
pub type ValueType = Vec<u8>;

pub fn serialize<V: Serialize>(value: &V) -> Result<Vec<u8>> {
    Ok(bincode::serialize(value)?)
}

pub fn deserialize<'a, V: Deserialize<'a>>(bytes: &'a [u8]) -> Result<V> {
    Ok(bincode::deserialize(bytes)?)
}