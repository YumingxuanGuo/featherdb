use serde::{Deserialize, Serialize};

use crate::{storage::log::LogStore, error::{Result, Error}};

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

pub struct Log {
    /// The underlying log store.
    pub(super) store: Box<dyn LogStore>,
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
    /// Creates a new log, using a LogStore for storage.
    pub fn new(store: Box<dyn LogStore>) -> Result<Log> {
        let (commit_index, commit_term) = match store.commit_index() {
            0 => (0, 0),
            index => store
                .get(index)?
                .map(|v| Self::deserialize::<Entry>(&v))
                .transpose()?
                .map(|e| (e.index, e.term))
                .ok_or_else(|| Error::Internal("Committed entry not found".into()))?,
        };
        let (last_index, last_term) = match store.len() {
            0 => (0, 0),
            index => store
                .get(index)?
                .map(|v| Self::deserialize::<Entry>(&v))
                .transpose()?
                .map(|e| (e.index, e.term))
                .ok_or_else(|| Error::Internal("Last entry not found".into()))?,
        };
        Ok(Log { store, last_index, last_term, commit_index, commit_term })
    }

    /// Appends a command to the log, returning the entry.
    pub fn append(&mut self, term: u64, command: Option<Vec<u8>>) -> Result<Entry> {
        let entry = Entry { index: self.last_index + 1, term, command };
        self.store.append(Self::serialize(&entry)?)?;
        self.last_index = entry.index;
        self.last_term = entry.term;
        Ok(entry)
    }

    /// Fetches an entry at an index.
    pub fn get(&self, index: u64) -> Result<Option<Entry>> {
        self.store.get(index)?.map(|v| Self::deserialize(&v)).transpose()
    }

    /// Serializes a value for the log store.
    fn serialize<V: Serialize>(value: &V) -> Result<Vec<u8>> {
        Ok(bincode::serialize(value)?)
    }

    /// Deserializes a value from the log store.
    fn deserialize<'a, V: Deserialize<'a>>(bytes: &'a [u8]) -> Result<V> {
        Ok(bincode::deserialize(bytes)?)
    }
}