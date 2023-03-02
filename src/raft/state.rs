use std::collections::{HashMap, BTreeMap, HashSet};

use log::{debug, error};
use tokio::sync::mpsc;
use tokio_stream::{wrappers::UnboundedReceiverStream, StreamExt};

use crate::error::{Result, Error};

use super::{log::{Entry, LogScan}, Address, Message};

/// A Raft-managed state machine.
pub trait State: Send {
    /// Returns the last applied index from the state machine, used when initializing the driver.
    fn get_applied_index(&self) -> u64;

    /// Mutates the state machine. If the state machine returns Error::Internal, the Raft node
    /// halts. For any other error, the state is applied and the error propagated to the caller.
    fn mutate(&mut self, index: u64, command: Vec<u8>) -> Result<Vec<u8>>;

    /// Queries the state machine. All errors are propagated to the caller.
    fn query(&self, command: Vec<u8>) -> Result<Vec<u8>>;
}

#[derive(Debug, PartialEq)]
/// A driver instruction.
pub enum Instruction {
    /// Abort all pending operations, e.g. due to leader change.
    Abort,
    /// Apply a log entry.
    Apply { entry: Entry },
    /// Votes for queries at the given term and commit index. TODO: What is this?
    Vote { term: u64, index: u64, address: Address },
}

/// A driver query.
struct Query {
    id: Vec<u8>,
    term: u64,
    address: Address,
    command: Vec<u8>,
    quorum: u64,
    votes: HashSet<Address>,
}

/// Drives a state machine, taking operations from state_rx and sending results via node_tx.
pub struct Driver {
    state_rx: UnboundedReceiverStream<Instruction>,
    node_tx: mpsc::UnboundedSender<Message>,
    applied_index: u64,
    /// Notify clients when their mutation is applied. <index, (client, id)>
    notify: HashMap<u64, (Address, Vec<u8>)>,
    queries: BTreeMap<u64, BTreeMap<Vec<u8>, Query>>,
}

impl Driver {
    /// Creates a new state machine driver.
    pub fn new(
        state_rx: mpsc::UnboundedReceiver<Instruction>,
        node_tx: mpsc::UnboundedSender<Message>,
    ) -> Self {
        Self {
            state_rx: UnboundedReceiverStream::new(state_rx),
            node_tx,
            applied_index: 0,
            notify: HashMap::new(),
            queries: BTreeMap::new(),
        }
    }

    /// Drives a state machine.
    pub async fn drive(mut self, mut state: Box<dyn State>) -> Result<()> {
        debug!("Starting state machine driver");
        while let Some(instruction) = self.state_rx.next().await {
            if let Err(error) = self.execute(instruction, &mut *state).await {
                error!("Halting state machine due to error: {}", error);
                return Err(error);
            }
        }
        debug!("Stopping state machine driver");
        Ok(())
    }

    /// Synchronously (re)plays a set of log entries, for initial sync.
    pub fn replay<'a>(&mut self, state: &mut dyn State, mut scan: LogScan<'a>) -> Result<()> {
        while let Some(entry) = scan.next().transpose()? {
            debug!("Replaying {:?}", entry);
            if let Some(command) = entry.command {
                match state.mutate(entry.index, command) {
                    Err(error @ Error::Internal(_)) => return Err(error),
                    _ => self.applied_index = entry.index,
                }
            }
        }
        Ok(())
    }

    /// Executes a state machine instruction.
    pub async fn execute(&mut self, i: Instruction, state: &mut dyn State) -> Result<()> {
        todo!()
    }
}