use std::collections::HashSet;
use serde::{Deserialize, Serialize};

use crate::concurrency::MVCC;
use crate::error::Result;
use crate::raft;
use crate::sql::schema::{Catalog, Table, Tables};
use crate::sql::types::{Row, Value, Expression};
use super::{SqlEngine, Mode, SqlTxn, RowScan, IndexScan};

/// An SQL engine that wraps a Raft cluster.
#[derive(Clone)]
pub struct RaftSqlEngine {

}

impl RaftSqlEngine {
    pub fn new() -> Self {
        Self {}
    }

    /// Serializes a command for the Raft SQL state machine.
    fn serialize<V: Serialize>(value: &V) -> Result<Vec<u8>> {
        Ok(bincode::serialize(value)?)
    }

    /// Deserializes a command for the Raft SQL state machine.
    fn deserialize<'a, V: Deserialize<'a>>(bytes: &'a [u8]) -> Result<V> {
        Ok(bincode::deserialize(bytes)?)
    }
}

impl SqlEngine for RaftSqlEngine {
    type EngineTxn = RaftSqlTxn;

    fn begin(&self, mode: Mode) -> Result<Self::EngineTxn> {
        todo!()
    }

    fn resume(&self, id: u64) -> Result<Self::EngineTxn> {
        todo!()
    }
}

/// A Raft-based SQL transaction
#[derive(Clone)]
pub struct RaftSqlTxn {

}

impl SqlTxn for RaftSqlTxn {
    fn id(&self) -> u64 {
        todo!()
    }

    fn mode(&self) -> Mode {
        todo!()
    }

    fn commit(self) -> Result<()> {
        todo!()
    }

    fn rollback(self) -> Result<()> {
        todo!()
    }

    fn create(&mut self, table: &str, row: Row) -> Result<()> {
        todo!()
    }

    fn read(&self, table: &str, id: &Value) -> Result<Option<Row>> {
        todo!()
    }

    fn update(&mut self, table: &str, id: &Value, row: Row) -> Result<()> {
        todo!()
    }

    fn delete(&mut self, table: &str, id: &Value) -> Result<()> {
        todo!()
    }

    fn read_index(&self, table: &str, column: &str, value: &Value) -> Result<HashSet<Value>> {
        todo!()
    }

    fn scan(&self, table: &str, filter: Option<Expression>) -> Result<RowScan> {
        todo!()
    }

    fn scan_index(&self, table: &str, column: &str) -> Result<IndexScan> {
        todo!()
    }
}

impl Catalog for RaftSqlTxn {
    fn create_table(&mut self, table: Table) -> Result<()> {
        todo!()
    }

    fn read_table(&self, table: &str) -> Result<Option<Table>> {
        todo!()
    }

    fn delete_table(&mut self, table: &str) -> Result<()> {
        todo!()
    }

    fn scan_tables(&self) -> Result<Tables> {
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

/// A Raft state machine query
#[derive(Clone, Serialize, Deserialize)]
enum Query {
    /// Fetches engine status
    Status,
    /// Resumes the active transaction with the given ID
    Resume(u64),

    /// Reads a row
    Read { txn_id: u64, table: String, id: Value },
    /// Reads an index entry
    ReadIndex { txn_id: u64, table: String, column: String, value: Value },
    /// Scans a table's rows
    Scan { txn_id: u64, table: String, filter: Option<Expression> },
    /// Scans an index
    ScanIndex { txn_id: u64, table: String, column: String },

    /// Scans the tables
    ScanTables { txn_id: u64 },
    /// Reads a table
    ReadTable { txn_id: u64, table: String },
}

/// The Raft state machine for the Raft-based SQL engine, using a KV SQL engine
pub struct StateMachine {
    /// The underlying KV SQL engine
    engine: super::KvSqlEngine,
    /// The last applied index
    applied_index: u64,
}

impl StateMachine {
    pub fn new(store: MVCC) -> Result<Self> {
        let engine = super::KvSqlEngine::new(store);
        let applied_index = engine
            .get_metadata(b"applied_index")?
            .map(|bytes| RaftSqlEngine::deserialize(&bytes))
            .unwrap_or(Ok(0))?;
        Ok(StateMachine { engine, applied_index })
    }
}

impl raft::State for StateMachine {
    fn applied_index(&self) -> u64 {
        self.applied_index
    }

    fn execute(&mut self, index: u64, command: Vec<u8>) -> Result<Vec<u8>> {
        todo!()
    }
}