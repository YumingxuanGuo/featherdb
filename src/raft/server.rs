use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use futures_util::FutureExt;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::error::Result;
use super::{Node, Driver, ResponseMsg, State};

/// A Raft server.
pub struct RaftServer {
    /// The underlying Raft node.
    node : Node,
    /// The state machine driver.
    driver: Driver,
    /// The session dispatcher.
    dispatcher: Dispatcher,
    /// The ongoing sessions.
    sessions: Arc<Mutex<HashMap<u64, mpsc::UnboundedSender<ResponseMsg>>>>,
}

impl RaftServer {
    /// Creates a new Raft server.
    pub async fn new(
        state: Box<dyn State>,
    ) -> Result<Self> {
        let (apply_tx, apply_rx) = mpsc::unbounded_channel();
        let (dispatcher_tx, dispatcher_rx) = mpsc::unbounded_channel();
        Ok(Self {
            node: Node::new(0, vec![], apply_tx).await?,
            driver: Driver::new(state, apply_rx, dispatcher_tx),
            dispatcher: Dispatcher::new(dispatcher_rx),
            sessions: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// Starts the Raft server.
    pub async fn serve(self) -> Result<()> {
        let (task, driver) = self.driver.drive().remote_handle();
        tokio::spawn(task);

        let (task, node) = self.node.serve().remote_handle();
        tokio::spawn(task);

        let sessions = self.sessions.clone();
        let (task, dispatcher) = self.dispatcher.dispatch(sessions).remote_handle();
        tokio::spawn(task);
        
        tokio::try_join!(driver, node, dispatcher)?;
        Ok(())
    }
}

/// A session dispatcher, taking operations from `dispatcher_rx` and sending results via `sessions`.
pub struct Dispatcher {
    /// The channel to receive state machine results from.
    dispatcher_rx: UnboundedReceiverStream<ResponseMsg>,
}

impl Dispatcher {
    /// Creates a new session dispatcher.
    pub fn new(dispatcher_rx: mpsc::UnboundedReceiver<ResponseMsg>) -> Self {
        Self { dispatcher_rx: UnboundedReceiverStream::new(dispatcher_rx) }
    }

    /// Starts the session dispatcher.
    pub async fn dispatch(mut self, sessions: Arc<Mutex<HashMap<u64, mpsc::UnboundedSender<ResponseMsg>>>>) -> Result<()> {
        while let Some(msg) = self.dispatcher_rx.next().await {
            match msg {
                ResponseMsg::Command { session_id, log_index, result } => {
                    let mut sessions = sessions.lock().unwrap();
                    if let Some(session) = sessions.get_mut(&session_id) {
                        session.send(ResponseMsg::Command { session_id, log_index, result })?;
                    }
                    // If session does not exist in this server (i.e., not leader), we just drop the result.
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// A Raft session command.
pub struct Command {
    pub session_id: u64,
    pub operation: Vec<u8>,
}

/// A Raft session.
pub struct RaftSession {

}