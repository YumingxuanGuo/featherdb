use std::ops::RangeBounds;

use serde::{Deserialize, Serialize};

use crate::{storage::log::{LogStore, Range}, error::{Result, Error}};

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

pub type Scan<'a> = Box<dyn Iterator<Item = Result<Entry>> + 'a>;

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

    /// Commits entries up to and including an index.
    pub fn commit(&mut self, index: u64) -> Result<u64> {
        todo!()
    }

    /// Fetches an entry at an index.
    pub fn get(&self, index: u64) -> Result<Option<Entry>> {
        self.store.get(index)?.map(|v| Self::deserialize(&v)).transpose()
    }

    /// Iterates over log entries
    pub fn scan(&self, range: impl RangeBounds<u64>) -> Scan {
        Box::new(
            self.store
                .scan(Range::from(range))
                .map(|r| 
                    r.and_then(|v| Self::deserialize(&v))
                )
        )
    }

    /// Splices a set of entries onto an offset. The entries must be contiguous, and the first entry
    /// must be at most `last_index + 1`. If an entry does not exist, append it. If an existing entry
    /// has a term mismatch, replace it and all following entries.
    pub fn splice(&mut self, entries: Vec<Entry>) -> Result<u64> {
        for i in 0..entries.len() {
            if i == 0 && entries.get(i).unwrap().index > self.last_index + 1 {
                return Err(Error::Internal("Spliced entries cannot begin past last index".into()));
            }
            if entries.get(i).unwrap().index != entries.get(0).unwrap().index + i as u64 {
                return Err(Error::Internal("Spliced entries must be contiguous".into()));
            }
        }
        for entry in entries {
            if let Some(ref current) = self.get(entry.index)? {
                if current.term == entry.term {
                    continue;
                }
                self.truncate(entry.index - 1)?;
            }
            self.append(entry.term, entry.command)?;
        }
        Ok(self.last_index)
    }

    /// Truncates the log such that its last item is at most index.
    /// Refuses to remove entries that have been applied or committed.
    pub fn truncate(&mut self, index: u64) -> Result<u64> {
        let (index, term) = match self.store.truncate(index)? {
            0 => (0, 0),
            i => self
                .store
                .get(i)?
                .map(|v| Self::deserialize::<Entry>(&v))
                .transpose()?
                .map(|e| (e.index, e.term))
                .ok_or_else(|| Error::Internal(format!("Entry {} not found", index)))?,
        };
        self.last_index = index;
        self.last_term = term;
        Ok(index)
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