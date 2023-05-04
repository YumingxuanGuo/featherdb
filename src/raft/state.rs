use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::error::Result;

use super::Command;

/// A Raft-managed state machine.
pub trait State: Send {
    /// Returns the last applied index from the state machine, used when initializing the driver.
    fn applied_index(&self) -> u64;

    /// Executes the given operation. If the state machine returns Error::Internal, the Raft node
    /// halts. For any other error, the state is applied and the error propagated to the caller.
    fn execute(&mut self, index: u64, operation: Vec<u8>) -> Result<Vec<u8>>;
}

/// A Raft state machine apply message.
#[derive(Debug)]
pub enum ApplyMsg {
    /// A command to apply.
    Command {
        log_index: u64,
        command: Command,
    },
}

/// A Raft state machine response message.
#[derive(Debug)]
pub enum ResponseMsg {
    /// A response to a command.
    Command {
        log_index: u64,
        session_id: u64,
        result: Result<Vec<u8>>
    },
}

/// Drives a state machine, taking operations from `apply_rx` and sending results via `dispatcher_tx`.
pub struct Driver {
    /// The state machine.
    state: Box<dyn State>,
    /// The channel to receive state machine operations from.
    apply_rx: UnboundedReceiverStream<ApplyMsg>,
    /// The channel to send state machine results to.
    dispatcher_tx: mpsc::UnboundedSender<ResponseMsg>,
}

impl Driver {
    /// Creates a new state machine driver.
    pub fn new(
        state: Box<dyn State>,
        apply_rx: mpsc::UnboundedReceiver<ApplyMsg>,
        dispatcher_tx: mpsc::UnboundedSender<ResponseMsg>,
    ) -> Self {
        Self {
            state,
            apply_rx: UnboundedReceiverStream::new(apply_rx),
            dispatcher_tx,
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

    /// Executes a state machine apply message and sends the result to the dispatcher.
    fn execute(&mut self, apply_msg: ApplyMsg) -> Result<()> {
        match apply_msg {
            ApplyMsg::Command { log_index, command } => {
                let Command { session_id, operation } = command;
                let result = self.state.execute(log_index, operation);
                self.dispatcher_tx.send(ResponseMsg::Command { log_index, session_id, result })?;
            },
        }
        Ok(())
    }
}