use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::error::{Error, Result};
use super::Entry;

/// A Raft-managed state machine.
pub trait State: Send {
    /// Returns the last applied index from the state machine, used when initializing the driver.
    fn applied_index(&self) -> u64;

    /// Executes the given command. If the state machine returns Error::Internal, the Raft node
    /// halts. For any other error, the state is applied and the error propagated to the caller.
    fn execute(&mut self, index: u64, command: Vec<u8>) -> Result<Vec<u8>>;
}

/// A Raft state machine apply message.
#[derive(Debug)]
pub enum ApplyMsg {
    /// A command to apply.
    Command {
        session_id: u64,
        log_index: u64,
        command: Vec<u8>
    },
}

/// A Raft state machine response message.
#[derive(Debug)]
pub enum ResponseMsg {
    /// A response to a command.
    Command {
        session_id: u64,
        log_index: u64,
        result: Result<Vec<u8>>
    },
}

/// Drives a state machine, taking operations from state_rx and sending results via node_tx.
pub struct Driver {
    /// The state machine to drive.
    state: Box<dyn State>,
    /// The channel to receive state machine operations from.
    state_rx: mpsc::UnboundedReceiver<ApplyMsg>,
    /// The channel to send state machine results to.
    node_tx: mpsc::UnboundedSender<ResponseMsg>,
}