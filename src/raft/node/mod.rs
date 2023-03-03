mod candidate;
mod follower;
mod leader;

use std::collections::HashMap;

use log::{debug, info};
use serde_derive::{Serialize, Deserialize};
use tokio::sync::mpsc;

use crate::error::{Result, Error};
use follower::Follower;
use candidate::Candidate;
use leader::Leader;

use super::{Log, Message, Instruction, Address, Event, State, state::Driver};

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
    Candidate(RoleNode<Candidate>),
    Follower(RoleNode<Follower>),
    Leader(RoleNode<Leader>),
}

impl Node {
    /// Creates a new Raft node, starting as a follower, or leader if no peers.
    pub async fn new(
        id: &str,
        peers: Vec<String>,
        log: Log,
        mut state: Box<dyn State>,
        node_tx: mpsc::UnboundedSender<Message>,
    ) -> Result<Self> {
        // Assert that the applied index is no greater than the commit index
        let applied_index = state.get_applied_index();
        if applied_index > log.commit_index {
            return Err(Error::Internal(format!(
                "State machine applied index {} greater than log committed index {}",
                applied_index, log.commit_index
            )));
        }

        // Apply committed entries that has not yet been applied.
        let (state_tx, state_rx) = mpsc::unbounded_channel();
        let mut driver = Driver::new(state_rx, node_tx.clone());
        if log.commit_index > applied_index {
            info!("Replaying log entries {} to {}", applied_index + 1, log.commit_index);
            driver.replay(&mut *state, log.scan((applied_index + 1)..=log.commit_index))?;
        }
        tokio::spawn(driver.drive(state));

        // Construct the node.
        let (term, voted_for) = log.load_term()?;
        let node = RoleNode {
            id: id.to_owned(),
            peers,
            term,
            log,
            node_tx,
            state_tx,
            queued_reqs: Vec::new(),
            proxied_reqs: HashMap::new(),
            role: Follower::new(None, voted_for.as_deref()),
        };
        if node.peers.is_empty() {
            info!("No peers specified, starting as leader");
            let last_index = node.log.last_index;
            Ok(node.become_role(Leader::new(vec![], last_index))?.into())
        } else {
            Ok(node.into())
        }
    }

    /// Processes a message (generic version).
    pub fn step(self, msg: Message) -> Result<Self> {
        debug!("Stepping {:?}", msg);
        match self {
            Node::Candidate(node) => node.step(msg),
            Node::Follower(node) => node.step(msg),
            Node::Leader(node) => node.step(msg),
        }
    }

    /// Moves time forward by a tick (generic version).
    pub fn tick(self) -> Result<Self> {
        match self {
            Node::Candidate(node) => node.tick(),
            Node::Follower(node) => node.tick(),
            Node::Leader(node) => node.tick(),
        }
    }
}

/// Candidate::into() -> Node
impl From<RoleNode<Candidate>> for Node {
    fn from(rn: RoleNode<Candidate>) -> Self {
        Node::Candidate(rn)
    }
}

/// Follower::into() -> Node
impl From<RoleNode<Follower>> for Node {
    fn from(rn: RoleNode<Follower>) -> Self {
        Node::Follower(rn)
    }
}

/// Leader::into() -> Node
impl From<RoleNode<Leader>> for Node {
    fn from(rn: RoleNode<Leader>) -> Self {
        Node::Leader(rn)
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
        for (id, address) in std::mem::take(&mut self.proxied_reqs) {
            self.send(address, Event::ClientResponse { id, response: Err(Error::Abort) })?;
        }
        Ok(())
    }

    /// Sends any queued requests to the given leader.
    fn forward_queued(&mut self, leader: Address) -> Result<()> {
        todo!()
    }

    /// Returns the quorum size of the cluster.
    fn quorum(&self) -> u64 {
        (self.peers.len() as u64 + 1) / 2 + 1
    }

    /// Sends an event.
    fn send(&self, dst: Address, event: Event) -> Result<()> {
        todo!()
    }

    /// Validates a message.
    fn validate(&self, msg: &Message) -> Result<()> {
        todo!()
    }
}