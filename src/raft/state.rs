use crate::error::Result;

use super::{log::Entry, Address};

/// A Raft-managed state machine.
pub trait State: Send {
    /// Returns the last applied index from the state machine, used when initializing the driver.
    fn get_applied_index(&self) -> u64;

    /// Mutates the state machine. If the state machine returns Error::Internal, the Raft node
    /// halts. For any other error, the state is applied and the error propagated to the caller.
    fn mutate(&mut self, index: u64, command: Vec<u8>) -> Result<Vec<u8>>;

    /// Queries the state machine. All errors are propagated to the caller.
    fn query(&self, command: Vec<u8>) -> Result<Vec<u8>>;
}

#[derive(Debug, PartialEq)]
/// A driver instruction.
pub enum Instruction {
    /// Abort all pending operations, e.g. due to leader change.
    Abort,
    /// Apply a log entry.
    Apply { entry: Entry },
    /// Votes for queries at the given term and commit index. TODO: What is this?
    Vote { term: u64, index: u64, address: Address },
}