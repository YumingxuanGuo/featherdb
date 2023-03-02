use std::{collections::HashMap, time::Duration};

use tokio::sync::mpsc;

use crate::error::Result;

use super::{Node, Message, Log, State};

/// The duration of a Raft tick, the unit of time for e.g. heartbeats and elections.
const TICK: Duration = Duration::from_millis(100);

/// A Raft server.
pub struct Server {
    node: Node,
    peers: HashMap<String, String>,
    node_rx: mpsc::UnboundedReceiver<Message>,
}

impl Server {
    /// Creates a new Raft cluster
    pub async fn new(
        id: &str,
        peers: HashMap<String, String>,
        log: Log,
        state: Box<dyn State>
    ) -> Result<Self> {
        let (node_tx, node_rx) = mpsc::unbounded_channel();
        Ok(Self { 
            node: Node::new(
                id, 
                peers.iter().map(|(k, _)| k.to_string()).collect(),
                log, 
                state, 
                node_tx
            ).await?, 
            peers, 
            node_rx,
        })
    }
}