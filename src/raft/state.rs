use std::{collections::HashMap, sync::{Mutex, Arc}};

use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::error::{Result, Error};
use super::{Command, Node, KvSession, Task};

/// A Raft-managed state machine.
pub trait State: Send + Sync {
    /// Returns the last applied index from the state machine, used when initializing the driver.
    fn applied_index(&self) -> u64;

    /// Executes the given operation. If the state machine returns Error::Internal, the Raft node
    /// halts. For any other error, the state is applied and the error propagated to the caller.
    fn execute(&mut self, index: u64, operation: Vec<u8>) -> Result<Vec<u8>>;
}

/// A Raft state machine apply message.
#[derive(Debug)]
pub struct ApplyMsg {
    pub log_index: u64,
    pub command: Command,
}

/// The meta-info for a client session.
pub struct SesstionMeta {
    pub session_id: u64,
    pub last_applied_sequence_number: u64,
    pub stored_result: Option<Result<Vec<u8>>>,
    pub task_tx: mpsc::UnboundedSender<Task>,
    pub result_tx: mpsc::UnboundedSender<Result<Vec<u8>>>,
}

/// Drives a state machine, taking operations from `apply_rx` and sending results via `dispatcher_tx`.
pub struct Driver {
    /// The underlying Raft node.
    node: Node,
    /// The state machine.
    state: Box<dyn State>,
    /// The channel to receive state machine operations from.
    apply_rx: UnboundedReceiverStream<ApplyMsg>,
    /// The channel to send registration results to.
    registration_status: Arc<Mutex<HashMap<u64, mpsc::UnboundedSender<()>>>>,
    /// The ongoing sessions.
    sessions: HashMap<u64, SesstionMeta>,
}

impl Driver {
    /// Creates a new state machine driver.
    pub fn new(
        node: Node,
        state: Box<dyn State>,
        apply_rx: mpsc::UnboundedReceiver<ApplyMsg>,
        registration_status: Arc<Mutex<HashMap<u64, mpsc::UnboundedSender<()>>>>,
    ) -> Self {
        Self {
            node,
            state,
            apply_rx: UnboundedReceiverStream::new(apply_rx),
            registration_status,
            sessions: HashMap::new(),
        }
    }

    /// Drives a state machine.
    pub async fn drive(mut self) -> Result<()> {
        while let Some(msg) = self.apply_rx.next().await {
            if let Err(e) = self.execute(msg) {
                return Err(e);
            }
        }
        Ok(())
    }

    /// Executes a state machine apply message.
    fn execute(&mut self, apply_msg: ApplyMsg) -> Result<()> {
        let ApplyMsg { log_index, command } = apply_msg;
        match command {
            Command::Operation { session_id, operation, sequence_number } => {
                let session_meta = self.sessions.get_mut(&session_id)
                    .ok_or_else(|| Error::Internal(format!("Session {} not found", session_id)))?;

                let result = 
                    // If the operation has already been applied, returns the stored result.
                    if session_meta.last_applied_sequence_number == sequence_number {
                        session_meta.stored_result
                            .clone()
                            .ok_or_else(|| Error::Internal(format!(
                                "No stored result for session {}",
                                session_id,
                            )))?
                    }
                    // If the operation has not been applied, applies it and stores the result.
                    else if session_meta.last_applied_sequence_number < sequence_number {
                        let result = self.state.execute(log_index, operation);
                        session_meta.stored_result = Some(result.clone());
                        session_meta.last_applied_sequence_number = sequence_number;
                        result
                    }
                    // If the sequence number is smaller, returns an error.
                    else {
                        return Err(Error::Internal(format!(
                            "Sequence number {} is smaller than last applied sequence number {}",
                            sequence_number,
                            session_meta.last_applied_sequence_number,
                        )));
                    };

                // Sends the result to the corresponding session.
                session_meta.result_tx.send(result)?;
            },

            Command::Registration { session_id } => {
                // Records the meta-data of the session. TODO: Possible de-duplication.
                let (task_tx, task_rx) = mpsc::unbounded_channel();
                let (result_tx, result_rx) = mpsc::unbounded_channel();
                self.sessions.insert(session_id, SesstionMeta {
                    session_id,
                    last_applied_sequence_number: 0,
                    stored_result: None,
                    task_tx,
                    result_tx,
                });

                // Spawns a new session.
                let node = self.node.clone();
                tokio::spawn(async move {
                    KvSession::new(node, session_id, task_rx, result_rx)
                        .serve()
                        .await
                        .unwrap();
                });

                // Notifies the server that the session has been registered.
                let mut registration_status = self.registration_status.lock()?;
                let registration_tx = registration_status.remove(&session_id)
                    .ok_or_else(|| Error::Internal(format!("Registration {} not found", session_id)))?;
                registration_tx.send(())?;
            },
        }

        Ok(())
    }
}