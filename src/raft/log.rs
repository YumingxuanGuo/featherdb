use std::ops::RangeBounds;

use serde_derive::{Serialize, Deserialize};

use crate::{storage::log, error::Result};

/// A log scan
pub type Scan<'a> = Box<dyn Iterator<Item = Result<Entry>> + 'a>;

/// The replicated Raft log
pub struct Log {
    /// The underlying log store.
    pub(super) store: Box<dyn log::Store>,
    /// The index of the last stored entry.
    pub(super) last_index: u64,
    /// The term of the last stored entry.
    pub(super) last_term: u64,
    /// The last entry known to be committed.
    pub(super) commit_index: u64,
    /// The term of the last committed entry.
    pub(super) commit_term: u64,
}

impl Log {
    /// Saves information about the most recent term.
    pub fn save_term(&mut self, term: u64, voted_for: Option<&str>) -> Result<()> {
        todo!()
    }

    /// Appends a command to the log, returning the entry.
    pub fn append(&mut self, term: u64, command: Option<Vec<u8>>) -> Result<Entry> {
        todo!()
    }

    /// Fetches an entry at an index
    pub fn get(&self, index: u64) -> Result<Option<Entry>> {
        todo!()
    }
    
    /// Checks if the log contains an entry
    pub fn has(&self, index: u64, term: u64) -> Result<bool> {
        todo!()
    }

    /// Iterates over log entries
    pub fn scan(&self, range: impl RangeBounds<u64>) -> Scan {
        todo!()
    }

    /// Splices a set of entries onto an offset. The entries must be contiguous, and the first entry
    /// must be at most last_index+1. If an entry does not exist, append it. If an existing entry
    /// has a term mismatch, replace it and all following entries.
    pub fn splice(&mut self, entries: Vec<Entry>) -> Result<u64> {
        todo!()
    }
}


/// A replicated log entry
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Entry {
    /// The index of the entry.
    pub index: u64,
    /// The term in which the entry was added.
    pub term: u64,
    /// The state machine command. None is used to commit noops during leader election.
    pub command: Option<Vec<u8>>,
}