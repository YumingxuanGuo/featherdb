use log::{info, warn, debug};
use rand::Rng;

use crate::{error::Result, raft::{Address, Event, Message, Response}};

use super::{ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX, RoleNode, Follower, Leader, Node};



/// A candidate is campaigning to become a leader.
#[derive(Debug)]
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

impl RoleNode<Candidate> {
    /// Transition to follower role.
    fn become_follower(mut self, term: u64, leader: &str) -> Result<RoleNode<Follower>> {
        info!("Discovered leader {} for term {}, following", leader, term);
        self.term = term;
        self.log.save_term(term, None)?;
        let mut node = self.become_role(Follower::new(Some(leader), None))?;
        node.abort_proxied()?;
        node.forward_queued(Address::Peer(leader.to_string()))?;
        Ok(node)
    }

    /// Transition to leader role.
    fn become_leader(self) -> Result<RoleNode<Leader>> {
        info!("Won election for term {}, becoming leader", self.term);
        let peers = self.peers.clone();
        let last_index = self.log.last_index;
        let mut node = self.become_role(Leader::new(peers, last_index))?;
        node.send(
            Address::Peers,
            Event::Heartbeat {
                commit_index: node.log.commit_index,
                commit_term: node.log.commit_term,
            },
        )?;
        node.append(None)?;
        node.abort_proxied()?;
        Ok(node)
    }

    /// Processes a message.
    pub fn step(mut self, msg: Message) -> Result<Node> {
        // Pre-processing when receiving a message.
        if let Err(err) = self.validate(&msg) {
            warn!("Ignoring invalid message: {}", err);
            return Ok(self.into());
        }
        if msg.term > self.term {
            if let Address::Peer(src) = &msg.src_addr {
                return self.become_follower(msg.term, src)?.step(msg);
            }
        }

        match msg.event {
            Event::Heartbeat { .. } => {
                if let Address::Peer(src) = &msg.src_addr {
                    return self.become_follower(msg.term, src)?.step(msg);
                }
            },

            Event::GrantVote => {
                debug!("Received term {} vote from {:?}", self.term, msg.src_addr);
                self.role.vote_count += 1;
                if self.role.vote_count >= self.quorum() {
                    let queued = std::mem::take(&mut self.queued_reqs);
                    let mut node: Node = self.become_leader()?.into();
                    for (src_addr, event) in queued {
                        node = node.step(Message { term: 0, src_addr, dst_addr: Address::Local, event })?;
                    }
                    return Ok(node);
                }
            },
            
            Event::ClientRequest { .. } => {
                todo!()
            }

            Event::ClientResponse { id, mut response } => {
                if let Ok(Response::Status(ref mut status)) = response {
                    status.server = self.id.clone();
                }
                self.proxied_reqs.remove(&id);
                self.send(Address::Client, Event::ClientResponse { id, response })?;
            }

            // Ignores other candidates when we are in an election.
            Event::SolicitVote { .. } => {},

            Event::ConfirmLeader { .. }
            | Event::ReplicateEntries { .. }
            | Event::AcceptEntries { .. } 
            | Event::RejectEntries { .. } => warn!("Received unexpected message {:?}", msg),
        }

        Ok(self.into())
    }

     /// Processes a logical clock tick.
     pub fn tick(mut self) -> Result<Node> {
        self.role.election_ticks += 1;

        // If election times out, start a new election for the next term.
        if self.role.election_ticks >= self.role.election_timeout {
            info!("Election timed out, starting new election for term {}", self.term + 1);
            self.term += 1;
            self.log.save_term(self.term, None)?;
            self.role = Candidate::new();
            self.send(
                Address::Peers, 
                Event::SolicitVote {
                    last_log_index: self.log.last_index,
                    last_log_term: self.log.last_term,
                }
            )?;
        }

        Ok(self.into())
     }
}