use tokio::sync::{mpsc, oneshot};

use crate::error::Result;

use super::{Request, Response};

/// A client for a local Raft server.
#[derive(Clone)]
pub struct Client {
    request_tx: mpsc::UnboundedSender<(Request, oneshot::Sender<Result<Response>>)>,
}

impl Client {
    /// Mutates the Raft state machine.
    pub async fn mutate(&self, command: Vec<u8>) -> Result<Vec<u8>> {
        todo!()
    }

    /// Queries the Raft state machine.
    pub async fn query(&self, command: Vec<u8>) -> Result<Vec<u8>> {
        todo!()
    }
}