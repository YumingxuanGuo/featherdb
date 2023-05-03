use std::{collections::HashMap, sync::{Arc, Mutex}};

use super::Node;

/// A Raft server.
pub struct RaftServer {
    /// The underlying Raft node.
    node: Arc<Node>,
    /// The ongoing sessions.
    sessions: Arc<Mutex<HashMap<u64, RaftSession>>>,
}

/// A Raft session.
pub struct RaftSession {

}