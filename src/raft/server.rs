use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use futures_util::FutureExt;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tonic::{Request, Response, Status};

use crate::error::{Result, RpcResult};
use crate::proto::raft_server::{RegistrationRequest, RegistrationReply, ExecutionReply, ExecutionRequest};
use crate::proto::raft_server::raft_server_server::RaftServer;
use super::{Node, Driver, State};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// A Raft session command.
pub enum Command {
    Operation {
        session_id: u64,
        sequence_number: u64,
        operation: Vec<u8>,
    },
    Registration {
        session_id: u64,
    },
}

/// A Raft-based key-value server.
pub struct KvServer {
    /// The underlying Raft node.
    node: Node,
    /// The state machine driver.
    driver: Driver,
    /// The next session ID, incrementing from 1.
    next_session_id: Arc<Mutex<u64>>,
    /// The channel to receive registration result from.
    registration_status: Arc<Mutex<HashMap<u64, mpsc::UnboundedSender<()>>>>,
    /// The ongoing sessions.
    sessions: Arc<Mutex<HashMap<u64, mpsc::UnboundedSender<Result<Vec<u8>>>>>>,
}

impl KvServer {
    /// Creates a new Raft server.
    pub async fn new(
        state: Box<dyn State>,
    ) -> Result<Self> {
        let (apply_tx, apply_rx) = mpsc::unbounded_channel();
        let registration_status = Arc::new(Mutex::new(HashMap::new()));
        let node = Node::new(0, vec![], apply_tx).await?;   // TODO: Better signature for Node::new()
        Ok(Self {
            driver: Driver::new(node.clone(), state, apply_rx, registration_status.clone()),
            next_session_id: Arc::new(Mutex::new(1)),
            registration_status,
            sessions: Arc::new(Mutex::new(HashMap::new())),
            node,
        })
    }

    /// Starts the Raft server.
    pub async fn serve(self) -> Result<()> {
        let (task, driver) = self.driver.drive().remote_handle();
        tokio::spawn(task);

        let (task, node) = self.node.serve().remote_handle();
        tokio::spawn(task);
        
        tokio::try_join!(driver, node)?;
        Ok(())
    }
}

#[tonic::async_trait]
impl RaftServer for KvServer {
    async fn register(&self, request: Request<RegistrationRequest>) -> RpcResult<RegistrationReply> {
        // TODO: Better code structure
        if !self.node.is_leader()? {
            return Err(Status::internal("[NotLeader]"));
        }

        let session_id = {
            let mut guard = self.next_session_id.lock().unwrap();
            *guard += 1;
            *guard - 1
        };

        let (tx, mut rx) = mpsc::unbounded_channel();
        {
            self.registration_status.lock().unwrap().insert(session_id, tx);
        }

        self.node.start(Command::Registration { session_id })?;

        // TODO: Timeout and retry
        rx.recv().await;

        return Ok(Response::new(RegistrationReply { status: vec![], client_id: session_id, leader_hint: 0 }));
    }

    async fn execute(&self, request: Request<ExecutionRequest>) -> RpcResult<ExecutionReply> {
        todo!()
    }
}

/// A task for a key-value session.
pub struct Task {

}

/// A Raft-based key-value session.
pub struct KvSession {
    /// The underlying Raft node.
    node: Node,
    /// The session ID.
    session_id: u64,
    /// The channel to receive task from.
    task_rx: mpsc::UnboundedReceiver<Task>,
    /// The channel to receive state machine result from.
    result_rx: mpsc::UnboundedReceiver<Result<Vec<u8>>>,
}

impl KvSession {
    /// Creates a new Raft session.
    pub fn new(
        node: Node,
        session_id: u64,
        task_rx: mpsc::UnboundedReceiver<Task>,
        result_rx: mpsc::UnboundedReceiver<Result<Vec<u8>>>,
    ) -> Self {
        Self {
            node,
            session_id,
            task_rx,
            result_rx,
        }
    }

    /// Starts the Raft session.
    pub async fn serve(mut self) -> Result<()> {
        while let Some(task) = self.task_rx.recv().await {
            todo!()
        }

        Ok(())
    }
}