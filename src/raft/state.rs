use std::collections::{HashMap, BTreeMap, HashSet};

use log::{debug, error};
use tokio::sync::mpsc;
use tokio_stream::{wrappers::UnboundedReceiverStream, StreamExt};

use crate::{error::{Result, Error}, raft::Response};

use super::{log::{Entry, LogScan}, Address, Message, Status, Event};

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
    /// Notify the given address with the result of applying the entry at the given index.
    Notify { id: Vec<u8>, address: Address, index: u64 },
    /// Query the state machine when the given term and index has been confirmed by vote.
    Query { id: Vec<u8>, address: Address, command: Vec<u8>, term: u64, index: u64, quorum: u64 },
    /// Extend the given server status and return it to the given address.
    Status { id: Vec<u8>, address: Address, status: Box<Status> },
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
    pub async fn execute(&mut self, instruction: Instruction, state: &mut dyn State) -> Result<()> {
        debug!("Executing {:?}", instruction);
        match instruction {
            Instruction::Abort => {
                self.notify_abort()?;
                self.query_abort()?;
            },
            
            Instruction::Apply { entry: Entry { index, command, .. } } => {
                if let Some(command) = command {
                    debug!("Applying state machine command {}: {:?}", index, command);
                    match tokio::task::block_in_place(|| state.mutate(index, command)) {
                        Err(error @ Error::Internal(_)) => return Err(error),
                        result => self.notify_applied(index, result)?,
                    };
                }
                // We have to track applied_index here, separately from the state machine, because
                // no-op log entries are significant for whether a query should be executed.
                self.applied_index = index;
                // Try to execute any pending queries, since they may have been submitted for a
                // commit_index which hadn't been applied yet.
                self.query_execute(state)?;
            },

            Instruction::Notify { id, address, index } => {
                todo!()
            },

            Instruction::Query { id, address, command, index, term, quorum } => {
                todo!()
            },

            Instruction::Status { id, address, mut status } => {
                todo!()
            },

            Instruction::Vote { term, index, address } => {
                todo!()
            },
        }

        Ok(())
    }

    /// Aborts all pending notifications.
    fn notify_abort(&mut self) -> Result<()> {
        todo!()
    }

    /// Notifies a client about an applied log entry, if any.
    fn notify_applied(&mut self, index: u64, result: Result<Vec<u8>>) -> Result<()> {
        todo!()
    }

    /// Aborts all pending queries.
    fn query_abort(&mut self) -> Result<()> {
        todo!()
    }

    /// Executes any queries that are ready.
    fn query_execute(&mut self, state: &mut dyn State) -> Result<()> {
        for query in self.query_ready(self.applied_index) {
            debug!("Executing query {:?}", query.command);
            let result = state.query(query.command);
            if let Err(error @ Error::Internal(_)) = result {
                return Err(error);
            }
            self.send(
                query.address,
                Event::ClientResponse { id: query.id, response: result.map(Response::State) }
            )?
        }
        Ok(())
    }

    /// Fetches and removes any ready queries, where index <= applied_index.
    fn query_ready(&mut self, applied_index: u64) -> Vec<Query> {
        let mut ready = Vec::new();
        let mut empty = Vec::new();
        for (index, queires) in self.queries.range_mut(..=applied_index) {
            let mut ready_ids = Vec::new();
            for (id, query) in queires.iter_mut() {
                if query.votes.len() as u64 >= query.quorum {
                    ready_ids.push(id.clone());
                }
            }
            for id in ready_ids {
                if let Some(query) = queires.remove(&id) {
                    ready.push(query);
                }
            }
            if queires.is_empty() {
                empty.push(*index);
            }
        }
        for index in empty {
            self.queries.remove(&index);
        }
        ready
    }

    /// Votes for queries up to and including a given commit index for a term by an address.
    fn query_vote(&mut self, term: u64, commit_index: u64, address: Address) {
        todo!()
    }

    /// Sends a message.
    fn send(&self, to: Address, event: Event) -> Result<()> {
        todo!()
    }
}