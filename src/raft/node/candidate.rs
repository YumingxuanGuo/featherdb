use rand::Rng;

use super::{ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX};



pub struct Candidate {
    /// Ticks elapsed since election start.
    election_ticks: u64,
    /// Election timeout, in ticks.
    election_timeout: u64,
    /// Votes received (including ourself).
    vote_count: u64,
}

impl Candidate {
    pub fn new() -> Self {
        Self { 
            vote_count: 1,
            election_ticks: 0, 
            election_timeout: rand::thread_rng()
                .gen_range(ELECTION_TIMEOUT_MIN..=ELECTION_TIMEOUT_MAX), 
        }
    }
}