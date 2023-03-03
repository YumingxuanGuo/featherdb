use std::collections::HashMap;

use log::{info, debug, warn};

use super::{Follower, Node, RoleNode, HEARTBEAT_INTERVAL, Status};
use crate::{error::{Result, Error}, raft::{Instruction, Address, Event, Message, Response, Request}};



// A leader serves requests and replicates the log to followers.
#[derive(Debug)]
pub struct Leader {
    /// Number of ticks since last heartbeat.
    heartbeat_ticks: u64,
    /// The next index to replicate to a peer.
    peer_next_index: HashMap<String, u64>,
    /// The last index known to be replicated on a peer.
    peer_match_index: HashMap<String, u64>,
}

impl Leader {
    /// Creates a new leader role.
    pub fn new(peers: Vec<String>, last_index: u64) -> Self {
        let mut leader = Self {
            heartbeat_ticks: 0,
            peer_next_index: HashMap::new(),
            peer_match_index: HashMap::new(),
        };
        for peer in peers {
            leader.peer_next_index.insert(peer.clone(), last_index + 1);
            leader.peer_match_index.insert(peer.clone(), 0);
        }
        leader
    }
}

impl RoleNode<Leader> {
    /// Transforms the leader into a follower
    fn become_follower(mut self, term: u64, leader: &str) -> Result<RoleNode<Follower>> {
        info!("Discovered new leader {} for term {}, following", leader, term);
        self.term = term;
        self.log.save_term(term, None)?;
        self.state_tx.send(Instruction::Abort)?;
        self.become_role(Follower::new(Some(leader), None))
    }
    
    /// Appends an entry to the log and replicates it to peers.
    pub fn append(&mut self, command: Option<Vec<u8>>) -> Result<u64> {
        let entry = self.log.append(self.term, command)?;
        for peer in self.peers.iter() {
            self.replicate(peer)?;
        }
        Ok(entry.index)
    }

    /// Commits any pending log entries.
    fn commit(&mut self) -> Result<u64> {
        let mut match_indexes = vec![self.log.last_index];
        match_indexes.extend(self.role.peer_match_index.values());
        match_indexes.sort_unstable();
        match_indexes.reverse();
        let quorum_index = match_indexes[self.quorum() as usize - 1];
        
        // We can only safely commit up to an entry from our own term, see figure 8 in Raft paper.
        if quorum_index > self.log.commit_index {
            if let Some(entry) = self.log.get(quorum_index)? {
                if entry.term == self.term {
                    let commit_index = self.log.commit_index;
                    self.log.commit(quorum_index)?;
                    let mut scan = self.log.scan((commit_index + 1)..=self.log.commit_index);
                    while let Some(entry) = scan.next().transpose()? {
                        self.state_tx.send(Instruction::Apply { entry })?;
                    }
                }
            }
        }
        Ok(self.log.commit_index)
    }

    /// Replicates the log to a peer.
    fn replicate(&self, peer: &str) -> Result<()> {
        let next_index = self
            .role
            .peer_next_index
            .get(peer)
            .cloned()
            .ok_or_else(|| Error::Internal(format!("Unknown peer {}", peer)))?;
        let prev_index = if next_index > 0 { next_index - 1 } else { 0 };
        let prev_term = match self.log.get(prev_index)? {
            Some(prev_entry) => prev_entry.term,
            None if prev_index == 0 => 0,
            None => return Err(Error::Internal(format!("Missing previous entry {}", prev_index))),
        };
        let entries = self.log.scan(next_index..).collect::<Result<Vec<_>>>()?;
        debug!("Replicating {} entries since {} to {}", entries.len(), prev_index, peer);
        self.send(
            Address::Peer(peer.to_string()),
            Event::ReplicateEntries { prev_index, prev_term, entries },
        )?;
        Ok(())
    }

    /// Process a message.
    pub fn step(mut self, msg: Message) -> Result<Node> {
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
            Event::ConfirmLeader { commit_index, has_committed } => {
                if let Address::Peer(src) = msg.src_addr.clone() {
                    self.state_tx.send(Instruction::Vote {
                        term: msg.term,
                        index: commit_index,
                        address: msg.src_addr,
                    })?;
                    if !has_committed {
                        self.replicate(&src)?;
                    }
                }
            },

            Event::AcceptEntries { match_index } => {
                if let Address::Peer(src) = msg.src_addr {
                    self.role.peer_next_index.insert(src.clone(), match_index + 1);
                    self.role.peer_match_index.insert(src, match_index);
                }
                self.commit()?;
            },

            Event::RejectEntries => {
                // TODO: Possible optimization.
                if let Address::Peer(src) = msg.src_addr {
                    self.role.peer_next_index
                        .entry(src.clone())
                        .and_modify(|i| {
                            if *i > 1 { *i -= 1; }
                        });
                    self.replicate(&src)?;
                }
            },
            
            Event::ClientRequest { id, request: Request::Query(command) } => {
                self.state_tx.send(Instruction::Query {
                    id,
                    address: msg.src_addr,
                    command,
                    term: self.term,
                    index: self.log.commit_index,
                    quorum: self.quorum(),
                })?;
                // TODO: What is this?
                self.state_tx.send(Instruction::Vote {
                    term: self.term,
                    index: self.log.commit_index,
                    address: Address::Local,
                })?;
                if !self.peers.is_empty() {
                    self.send(
                        Address::Peers,
                        Event::Heartbeat {
                            commit_index: self.log.commit_index,
                            commit_term: self.log.commit_term,
                        },
                    )?;
                }
            },

            Event::ClientRequest { id, request: Request::Mutate(command) } => {
                let index = self.append(Some(command))?;
                self.state_tx.send(Instruction::Notify { id, address: msg.src_addr, index })?;
                if self.peers.is_empty() {
                    self.commit()?;
                }
            },

            Event::ClientRequest { id, request: Request::Status } => {
                let mut status = Box::new(Status {
                    server: self.id.clone(),
                    leader: self.id.clone(),
                    term: self.term,
                    node_match_index: self.role.peer_match_index.clone(),
                    commit_index: self.log.commit_index,
                    apply_index: 0,  // TODO: why?
                    storage: self.log.store.to_string(),
                    storage_size: self.log.store.size(),
                });
                status.node_match_index.insert(self.id.clone(), self.log.last_index);
                self.state_tx.send(Instruction::Status { id, address: msg.src_addr, status })?;
            },

            Event::ClientResponse { id, mut response } => {
                if let Ok(Response::Status(ref mut status)) = response {
                    status.server = self.id.clone();
                }
                self.send(Address::Client, Event::ClientResponse { id, response })?;
            },

            // We ignore these messages, since they are typically additional votes from the previous
            // election that we won after a quorum.
            Event::SolicitVote { .. }
            | Event::GrantVote => {},

            Event::Heartbeat { .. }
            | Event::ReplicateEntries { .. } => warn!("Received unexpected message {:?}", msg),
        }

        Ok(self.into())
    }

    /// Processes a logical clock tick.
    pub fn tick(mut self) -> Result<Node> {
        if !self.peers.is_empty() {
            self.role.heartbeat_ticks += 1;
            if self.role.heartbeat_ticks >= HEARTBEAT_INTERVAL {
                self.role.heartbeat_ticks = 0;
                self.send(
                    Address::Peers, 
                    Event::Heartbeat {
                        commit_index: self.log.commit_index,
                        commit_term: self.log.commit_term,
                    },
                )?;
            }
        }
        Ok(self.into())
    }
}