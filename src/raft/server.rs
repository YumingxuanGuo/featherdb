use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};
use tonic::{Request, Response};

use crate::error::{Result, RpcResult, Error};
use crate::proto::featherkv::{FeatherKv, RegistrationRequest, RegistrationReply, ExecutionReply, ExecutionRequest};
use crate::storage::log::LogStore;
use super::{Node, Driver, State, ApplyResult};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// A Raft session command.
pub enum Command {
    Mutation {
        session_id: u64,
        sequence_number: u64,
        mutation: Vec<u8>,
    },
    Query {
        session_id: u64,
        sequence_number: u64,
        query: Vec<u8>,
    },
    Registration {
        session_id: u64,
    },
}

#[derive(Serialize, Deserialize)]
pub enum RpcStatus {
    Ok,
    NotLeader,
    SessionExpired,
}

/// A Raft-based FeatherKV.
pub struct FeatherKV {
    /// The underlying Raft node.
    node: Node,
    /// The next session ID, incrementing from 1.
    next_session_id: Arc<Mutex<u64>>,
    /// The channel to receive registration result from.
    registration_status: Arc<Mutex<HashMap<u64, mpsc::UnboundedSender<mpsc::UnboundedSender<Task>>>>>,
    /// The sending channels of the ongoing sessions.
    session_txs: Arc<Mutex<HashMap<u64, mpsc::UnboundedSender<Task>>>>,
}

impl FeatherKV {
    /// Creates a new Raft FeatherKV. Spawns one background task to serve the Raft node and one to
    /// drive the state machine. Assumes that the caller will be long running.
    pub async fn new(
        me: u64,
        peers: Vec<String>,
        state: Box<dyn State>,
        log_store: Box<dyn LogStore>,
    ) -> Result<Self> {
        let (apply_tx, apply_rx) = mpsc::unbounded_channel();
        let registration_status = Arc::new(Mutex::new(HashMap::new()));

        let node = Node::new(me, peers, apply_tx, log_store).await?;
        let driver = Driver::new(node.clone(), state, apply_rx, registration_status.clone());

        tokio::spawn(driver.drive());
        tokio::spawn(node.clone().serve());
        
        Ok(Self {
            node,
            next_session_id: Arc::new(Mutex::new(1)),
            registration_status,
            session_txs: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// Serializes a value for the Raft FeatherKV.
    fn serialize<V: Serialize>(value: &V) -> Result<Vec<u8>> {
        Ok(bincode::serialize(value)?)
    }

    /// Deserializes a value from the Raft FeatherKV.
    fn deserialize<'a, V: Deserialize<'a>>(bytes: &'a [u8]) -> Result<V> {
        Ok(bincode::deserialize(bytes)?)
    }
}

#[tonic::async_trait]
impl FeatherKv for FeatherKV {
    async fn register(&self, _request: Request<RegistrationRequest>) -> RpcResult<RegistrationReply> {
        let not_leader_reply = RegistrationReply {
            status: Self::serialize(&RpcStatus::NotLeader)?,
            session_id: 0,
            leader_hint: self.node.leader_id()?,
        };

        // If the node is not the leader, returns `NotLeader` to the client.
        if !self.node.is_leader()? {
            return Ok(Response::new(not_leader_reply));
        }

        // Fetches and increments the next session ID.
        let session_id = {
            let mut guard = self.next_session_id.lock().unwrap();
            *guard += 1;
            *guard - 1
        };

        // The channel to receive the registration result from the driver.
        let (task_ch_tx, mut task_ch_rx) = mpsc::unbounded_channel();
        {
            self.registration_status.lock().unwrap().insert(session_id, task_ch_tx);
        }

        // Starts the command. If the node has lost leadership, replies `NotLeader`.
        // Returns other errors to the client as internal errors.
        match self.node.start(Command::Registration { session_id }) {
            Err(Error::NotLeader) => {
                return Ok(Response::new(not_leader_reply));
            },
            Err(e) => return Err(e.into()),
            Ok(_) => { },
        }

        // Waits for the replica group to apply the registration command. Retries if timeout.
        loop {
            tokio::select! {
                task_tx = task_ch_rx.recv() => {
                    let task_tx = task_tx.unwrap();
                    self.session_txs.lock().unwrap().insert(session_id, task_tx);

                    // Session registered successfully. Returns `Ok` to the client.
                    let ok_reply = RegistrationReply {
                        status: Self::serialize(&RpcStatus::Ok)?,
                        session_id,
                        leader_hint: self.node.leader_id()?,
                    };
                    return Ok(Response::new(ok_reply));
                },

                _ = tokio::time::sleep(tokio::time::Duration::from_secs(1)) => {
                    match self.node.start(Command::Registration { session_id }) {
                        Err(Error::NotLeader) => return Ok(Response::new(not_leader_reply)),
                        Err(e) => return Err(e.into()),
                        Ok(_) => { },
                    }
                }
            }
        }
    }

    async fn mutate(&self, request: Request<ExecutionRequest>) -> RpcResult<ExecutionReply> {
        let ExecutionRequest { session_id, sequence_number, operation } = request.into_inner();
        let (reply_tx, reply_rx) = oneshot::channel();
        let task = Task {
            reply_tx,
            command: Command::Mutation {
                session_id,
                sequence_number,
                mutation: operation,
            },
        };

        {
            // Sends the task to the corresponding session.
            let mut session_txs = self.session_txs.lock().unwrap();
            match session_txs.get_mut(&session_id) {
                Some(session_tx) => {
                    session_tx.send(task).unwrap();
                },
                None => {
                    let reply = ExecutionReply {
                        status: Self::serialize(&RpcStatus::SessionExpired)?,
                        response: vec![],
                        leader_hint: self.node.leader_id()?,
                    };
                    return Ok(Response::new(reply));
                },
            }
        }
        
        // Waits for the session to reply.
        let reply = reply_rx.await.unwrap();
        Ok(Response::new(reply))
    }

    async fn query(&self, request: Request<ExecutionRequest>) -> RpcResult<ExecutionReply> {
        let ExecutionRequest { session_id, sequence_number, operation } = request.into_inner();
        let (reply_tx, reply_rx) = oneshot::channel();
        let task = Task {
            reply_tx,
            command: Command::Query {
                session_id,
                sequence_number,
                query: operation,
            },
        };

        {
            // Sends the task to the corresponding session.
            let mut session_txs = self.session_txs.lock().unwrap();
            match session_txs.get_mut(&session_id) {
                Some(session_tx) => {
                    session_tx.send(task).unwrap();
                },
                None => {
                    let reply = ExecutionReply {
                        status: Self::serialize(&RpcStatus::SessionExpired)?,
                        response: vec![],
                        leader_hint: self.node.leader_id()?,
                    };
                    return Ok(Response::new(reply));
                },
            }
        }
        
        // Waits for the session to reply.
        let reply = reply_rx.await.unwrap();
        Ok(Response::new(reply))
    }
}

#[derive(Debug)]
/// A task for a key-value session.
pub struct Task {
    reply_tx: oneshot::Sender<ExecutionReply>,
    command: Command,
}

/// A Raft-based key-value session.
pub struct Session {
    /// The underlying Raft node.
    node: Node,
    /// The session ID.
    session_id: u64,
    /// The last applied sequence number.
    last_applied_sequence_number: u64,
    /// The result of the last operation.
    stored_result: Option<Result<Vec<u8>>>,
    /// The channel to receive task from.
    task_rx: mpsc::UnboundedReceiver<Task>,
    /// The channel to receive state machine result from.
    result_rx: mpsc::UnboundedReceiver<ApplyResult>,
}

impl Session {
    /// Creates a new Raft session.
    pub fn new(
        node: Node,
        session_id: u64,
        task_rx: mpsc::UnboundedReceiver<Task>,
        result_rx: mpsc::UnboundedReceiver<ApplyResult>,
    ) -> Self {
        Self {
            node,
            session_id,
            last_applied_sequence_number: 0,
            stored_result: None,
            task_rx,
            result_rx,
        }
    }

    /// Starts the Raft session.
    pub async fn serve(mut self) -> Result<()> {
        while let Some(Task { reply_tx, command }) = self.task_rx.recv().await {
            match command {
                Command::Mutation { session_id, sequence_number, .. } 
                | Command::Query { session_id, sequence_number, .. } => {
                    assert!(sequence_number >= self.last_applied_sequence_number);
                    assert_eq!(session_id, self.session_id);

                    let not_leader_reply = ExecutionReply {
                        status: Self::serialize(&RpcStatus::NotLeader)?,
                        response: vec![],
                        leader_hint: self.node.leader_id()?,
                    };

                    // If the command is already executed, returns the stored result.
                    if sequence_number == self.last_applied_sequence_number {
                        let reply = ExecutionReply {
                            status: Self::serialize(&RpcStatus::Ok)?,
                            response: Self::serialize(&self.stored_result
                                .clone()
                                .ok_or_else(|| Error::Internal(format!(
                                    "No stored result for session {}",
                                    session_id,
                                )))?)?,
                            leader_hint: self.node.leader_id()?,
                        };
                        reply_tx.send(reply).unwrap();
                        continue;
                    }

                    // Starts the command. If the node has lost leadership, replies `NotLeader`.
                    // Returns other errors to the client as internal errors.
                    match self.node.start(command.clone()) {
                        Err(Error::NotLeader) => {
                            reply_tx.send(not_leader_reply).unwrap();
                            break;
                        },
                        Err(e) => return Err(e),
                        Ok(_) => { },
                    }

                    // Waits for the replica group to apply the execution command. Retries if timeout.
                    loop {
                        tokio::select! {
                            Some(apply_result) = self.result_rx.recv() => {
                                // Discards the result if it is outdated.
                                if apply_result.sequence_number < sequence_number {
                                    continue;
                                }

                                // Updates the corresponding metadata.
                                self.stored_result = Some(apply_result.result);
                                self.last_applied_sequence_number = sequence_number;

                                // Sends the reply to the server's RPC handler.
                                let reply = ExecutionReply {
                                    status: Self::serialize(&RpcStatus::Ok)?,
                                    response: Self::serialize(&self.stored_result.clone().unwrap())?,
                                    leader_hint: self.node.leader_id()?,
                                };
                                reply_tx.send(reply).unwrap();
                                break;
                            },

                            _ = tokio::time::sleep(tokio::time::Duration::from_secs(1)) => {
                                match self.node.start(command.clone()) {
                                    Err(Error::NotLeader) => {
                                        reply_tx.send(not_leader_reply).unwrap();
                                        break;
                                    },
                                    Err(e) => return Err(e),
                                    Ok(_) => { },
                                }
                            }
                        }
                    }
                },

                Command::Registration { .. } => {
                    return Err(Error::Internal(format!(
                        "Unexpected registration command {:?}",
                        command,
                    )));
                }
            }
        }

        Ok(())
    }

    /// Serializes a value for the Raft session.
    fn serialize<V: Serialize>(value: &V) -> Result<Vec<u8>> {
        Ok(bincode::serialize(value)?)
    }

    /// Deserializes a value from the Raft session.
    fn deserialize<'a, V: Deserialize<'a>>(bytes: &'a [u8]) -> Result<V> {
        Ok(bincode::deserialize(bytes)?)
    }
}