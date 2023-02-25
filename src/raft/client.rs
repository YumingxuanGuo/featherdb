use tokio::sync::{mpsc, oneshot};

use crate::error::Result;

use super::{Request, Response};

/// A client for a local Raft server.
#[derive(Clone)]
pub struct Client {
    request_tx: mpsc::UnboundedSender<(Request, oneshot::Sender<Result<Response>>)>,
}
