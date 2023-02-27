use crate::{storage::log, error::Result};

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
}