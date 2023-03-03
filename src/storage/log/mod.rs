use std::fmt::Display;

use crate::error::Result;



/// A log store. Entry indexes are 1-based, to match Raft semantics.
pub trait Store: Display + Sync + Send {
    /// Returns the size of the log, in bytes.
    fn size(&self) -> u64;

    /// Sets a metadata value.
    fn set_metadata(&mut self, key: &[u8], value: Vec<u8>) -> Result<()>;
}