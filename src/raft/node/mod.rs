mod candidate;
mod follower;

use std::collections::HashMap;

use serde_derive::{Serialize, Deserialize};
use tokio::sync::mpsc;

use crate::error::Result;
use follower::Follower;
use candidate::Candidate;

use super::{Log, Message, Instruction, Address, Event};

/// The interval between leader heartbeats, in ticks.
const HEARTBEAT_INTERVAL: u64 = 1;

/// The minimum election timeout, in ticks.
const ELECTION_TIMEOUT_MIN: u64 = 8 * HEARTBEAT_INTERVAL;

/// The maximum election timeout, in ticks.
const ELECTION_TIMEOUT_MAX: u64 = 15 * HEARTBEAT_INTERVAL;

/// Node status
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Status {
    pub server: String,
    pub leader: String,
    pub term: u64,
    pub node_last_index: HashMap<String, u64>,
    pub commit_index: u64,
    pub apply_index: u64,
    pub storage: String,
    pub storage_size: u64,
}

/// The local Raft node state machine.
pub enum Node {
    Candidate(),
    Follower(RoleNode<Follower>),
    Leader(),
}

impl From<RoleNode<Follower>> for Node {
    fn from(rn: RoleNode<Follower>) -> Self {
        Node::Follower(rn)
    }
}

// A Raft node with role R
pub struct RoleNode<R> {
    id: String,
    peers: Vec<String>,
    term: u64,
    log: Log,
    node_tx: mpsc::UnboundedSender<Message>,
    state_tx: mpsc::UnboundedSender<Instruction>,
    /// Keeps track of queued client requests received e.g. during elections.
    queued_reqs: Vec<(Address, Event)>,
    /// Keeps track of proxied client requests, to abort on new leader election.
    proxied_reqs: HashMap<Vec<u8>, Address>,
    role: R,
}

impl<R> RoleNode<R> {
    /// Transforms the node into another role.
    fn become_role<T>(self, role: T) -> Result<RoleNode<T>> {
        Ok(RoleNode { 
            id: self.id, 
            peers: self.peers, 
            term: self.term, 
            log: self.log, 
            node_tx: self.node_tx, 
            state_tx: self.state_tx, 
            queued_reqs: self.queued_reqs, 
            proxied_reqs: self.proxied_reqs, 
            role,
        })
    }

    /// Aborts any proxied requests.
    fn abort_proxied(&mut self) -> Result<()> {
        todo!()
    }

    /// Sends any queued requests to the given leader.
    fn forward_queued(&mut self, leader: Address) -> Result<()> {
        todo!()
    }

    /// Sends an event
    fn send(&self, dst: Address, event: Event) -> Result<()> {
        todo!()
    }

    /// Validates a message
    fn validate(&self, msg: &Message) -> Result<()> {
        todo!()
    }
}