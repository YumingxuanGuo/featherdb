use crate::raft;

/// An SQL engine that wraps a Raft cluster.
#[derive(Clone)]
pub struct Raft {
    client: raft::Client,
}