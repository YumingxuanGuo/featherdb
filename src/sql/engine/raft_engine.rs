use serde_derive::{Serialize, Deserialize};

use crate::{raft, storage::kv, error::{Result, Error}, common::{deserialize, serialize}, sql::{types::{Row, Value}, schema::{Table, Catalog}}};
use super::{Engine as _, SqlTxn as _, Mode, kv_engine::KvEngine};

/// An SQL engine that wraps a Raft cluster.
#[derive(Clone)]
pub struct RaftEngine {
    client: raft::Client,
}

impl RaftEngine {
    /// Creates a new Raft SQL engine.
    pub fn new(client: raft::Client) -> Self {
        Self { client }
    }

    /// Creates an underlying state machine for a Raft engine.
    pub fn new_state(kv: kv::MVCC) -> Result<StateMachine> {
        StateMachine::new(kv)
    }
}

/// The Raft state machine for the Raft-based SQL engine, using a KV SQL engine
pub struct StateMachine {
    /// The underlying KV SQL engine
    engine: KvEngine,
    /// The last applied index
    applied_index: u64,
}

impl StateMachine {
    /// Creates a new Raft state maching using the given MVCC key/value store
    pub fn new(store: kv::MVCC) -> Result<Self> {
        let engine = KvEngine::new(store);
        let applied_index = engine
            .get_metadata(b"applied_index")?
            .map(|b| deserialize(&b))
            .unwrap_or(Ok(0))?;
        Ok(StateMachine { engine, applied_index })
    }

    /// Applies a state machine mutation
    fn apply(&mut self, mutation: Mutation) -> Result<Vec<u8>> {
        match mutation {
            Mutation::Begin(mode) => serialize(&self.engine.begin(mode)?.get_id()),
            Mutation::Commit(txn_id) => serialize(&self.engine.resume(txn_id)?.commit()?),
            Mutation::Rollback(txn_id) => serialize(&self.engine.resume(txn_id)?.rollback()?),

            Mutation::Create { txn_id, table, row } => {
                serialize(&self.engine.resume(txn_id)?.create(&table, row)?)
            }
            Mutation::Delete { txn_id, table, id } => {
                serialize(&self.engine.resume(txn_id)?.delete(&table, &id)?)
            }
            Mutation::Update { txn_id, table, id, row } => {
                serialize(&self.engine.resume(txn_id)?.update(&table, &id, row)?)
            }

            Mutation::CreateTable { txn_id, schema } => {
                serialize(&self.engine.resume(txn_id)?.create_table(schema)?)
            }
            Mutation::DeleteTable { txn_id, table } => {
                serialize(&self.engine.resume(txn_id)?.delete_table(&table)?)
            }
        }
    }
}

impl raft::State for StateMachine {
    fn get_applied_index(&self) -> u64 {
        self.applied_index
    }

    fn mutate(&mut self, index: u64, command: Vec<u8>) -> Result<Vec<u8>> {
        // We don't check that index == applied_index + 1, since the Raft log commits no-op
        // entries during leader election which we need to ignore.
        match self.apply(deserialize(&command)?) {
            error @ Err(Error::Internal(_)) => error,
            result => {
                self.engine.set_metadata(b"applied_index", serialize(&index)?)?;
                self.applied_index = index;
                result
            }
        }
    }

    fn query(&self, command: Vec<u8>) -> Result<Vec<u8>> {
        todo!()
    }
}

/// A Raft state machine mutation
#[derive(Clone, Serialize, Deserialize)]
enum Mutation {
    /// Begins a transaction in the given mode
    Begin(Mode),
    /// Commits the transaction with the given ID
    Commit(u64),
    /// Rolls back the transaction with the given ID
    Rollback(u64),

    /// Creates a new row
    Create { txn_id: u64, table: String, row: Row },
    /// Deletes a row
    Delete { txn_id: u64, table: String, id: Value },
    /// Updates a row
    Update { txn_id: u64, table: String, id: Value, row: Row },

    /// Creates a table
    CreateTable { txn_id: u64, schema: Table },
    /// Deletes a table
    DeleteTable { txn_id: u64, table: String },
}