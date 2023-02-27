use log::{info, warn};
use rand::Rng;

use crate::error::Error;
use crate::raft::Message;
use crate::{error::Result};
use super::super::{Address, Event};

use super::{ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX, RoleNode, Candidate, Node};



// A follower replicates state from a leader.
#[derive(Debug)]
pub struct Follower {
    /// The leader, or None if just initialized.
    leader: Option<String>,
    /// The number of ticks since the last message from the leader.
    leader_seen_ticks: u64,
    /// The timeout before triggering an election.
    leader_seen_timeout: u64,
    /// The node we voted for in the current term, if any.
    voted_for: Option<String>,
}

impl Follower {
    /// Creates a new follower role.
    pub fn new(leader: Option<&str>, voted_for: Option<&str>) -> Self {
        Self {
            voted_for: voted_for.map(String::from),
            leader: leader.map(String::from),
            leader_seen_ticks: 0,
            leader_seen_timeout: rand::thread_rng()
                .gen_range(ELECTION_TIMEOUT_MIN..=ELECTION_TIMEOUT_MAX),
        }
    }
}

impl RoleNode<Follower> {
    /// Transforms the node into a candidate.
    fn become_candidate(self) -> Result<RoleNode<Candidate>> {
        info!("Starting election for term {}", self.term + 1);
        let mut node = self.become_role(Candidate::new())?;
        node.term += 1;
        node.log.save_term(node.term, None)?;
        node.send(
            Address::Peers, 
            Event::SolicitVote { last_log_index: node.log.last_index, last_log_term: node.log.last_term },
        )?;
        Ok(node)
    }

    /// Transforms the node into a follower for a new leader.
    fn become_follower(mut self, leader: &str, term: u64) -> Result<RoleNode<Follower>> {
        let mut voted_for = None;
        if term > self.term {
            info!("Discovered new term {}, following leader {}", term, leader);
            self.term = term;
            self.log.save_term(term, None)?;
        } else if self.role.leader.is_none() {
            info!("Discovered leader {}, following", leader);
            voted_for = self.role.voted_for;
        } else {
            return Err(Error::Internal("Wrong time to become follower.".into()))
        }
        self.role = Follower::new(Some(leader), voted_for.as_deref());
        self.abort_proxied()?;
        self.forward_queued(Address::Peer(leader.to_string()))?;
        Ok(self)
    }

    /// Checks if an address is the current leader.
    fn is_my_leader(&self, src: &Address) -> bool {
        matches!(
            (&self.role.leader, src), 
            (Some(leader), Address::Peer(src)) if leader == src
        )
    }

    /// Process a message.
    pub fn step(mut self, msg: Message) -> Result<Node> {
        // Pre-processing when receiving a message.
        if let Err(err) = self.validate(&msg) {
            warn!("Ignoring invalid message: {}", err);
            return Ok(self.into());
        }
        if let Address::Peer(src) = &msg.src_addr {
            if msg.term > self.term || self.role.leader.is_none() {
                return self.become_follower(src, msg.term)?.step(msg);
            }
        }
        if self.is_my_leader(&msg.src_addr) {
            self.role.leader_seen_ticks = 0;
        }

        // Processes based on message event type.
        match msg.event {
            Event::Heartbeat { commit_index, commit_term } => {
                todo!()
            }
            Event::SolicitVote { last_log_index, last_log_term } => {
                // Refuses to vote if the candidate's term is smaller...
                if msg.term < self.term {
                    return Ok(self.into());
                }
                // ...or my `voted_for` is not null or the candidate's id...
                if let Some(voted_for) = &self.role.voted_for {
                    if Address::Peer(voted_for.clone()) != msg.src_addr {
                        return Ok(self.into());
                    }
                }
                // ...or the candidate's log is less up-to-date than mine.
                if (last_log_term < self.log.last_term) ||
                    (last_log_term == self.log.last_term && last_log_index < self.log.last_index) {
                    return Ok(self.into());
                }
                
                // Grants vote.
                if let Address::Peer(src) = msg.src_addr {
                    info!("Voting for {} in term {} election", src, self.term);
                    self.send(Address::Peer(src.clone()), Event::GrantVote)?;
                    self.role.voted_for = Some(src.clone());
                    self.log.save_term(self.term, Some(&src))?;
                }
            },

            // Ignore votes which are usually strays from the previous election that we lost.
            Event::GrantVote => {}
        }

        Ok(self.into())
    }

    /// Processes a logical clock tick.
    pub fn tick(mut self) -> Result<Node> {
        self.role.leader_seen_ticks += 1;
        if self.role.leader_seen_ticks >= self.role.leader_seen_timeout {
            Ok(self.become_candidate()?.into())
        } else {
            Ok(self.into())
        }
    }
}