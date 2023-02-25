pub mod mvcc;
pub mod transaction;
mod snapshot;
mod txnkey;
mod encoding;
mod scan;

use serde_derive::{Serialize, Deserialize};

/// An MVCC transaction mode.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum Mode {
    /// A read-write transaction.
    ReadWrite,
    /// A read-only transaction.
    ReadOnly,
    /// A read-only transaction running in a snapshot of a given version.
    ///
    /// The version must refer to a committed transaction ID. Any changes visible to the original
    /// transaction will be visible in the snapshot (i.e. transactions that had not committed before
    /// the snapshot transaction started will not be visible, even though they have a lower version).
    Snapshot { version: u64 },
}

impl Mode {
    /// Checks whether the transaction mode can mutate data.
    pub fn allow_writing(&self) -> bool {
        match self {
            Mode::ReadWrite => true,
            _otherwise => false,
        }
    }
}