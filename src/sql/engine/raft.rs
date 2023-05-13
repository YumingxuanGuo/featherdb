use std::collections::HashSet;
use serde::{Deserialize, Serialize};

use crate::concurrency::MVCC;
use crate::error::{Result, Error};
use crate::raft;
use crate::sql::schema::{Catalog, Table, Tables};
use crate::sql::types::{Row, Value, Expression};
use super::{SqlEngine, Mode, SqlTxn, RowScan, IndexScan};

/// A Raft state machine Operation
#[derive(Clone, Serialize, Deserialize)]
enum Operation {
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
    // /// Fetches engine status
    // Status,
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

/// An SQL engine that wraps a Raft cluster.
#[derive(Clone)]
pub struct RaftSqlEngine {
    client: raft::KvClient,
}

impl RaftSqlEngine {
    /// Creates a new Raft SQL engine.
    pub fn new(client: raft::KvClient) -> Self {
        Self { client }
    }

    /// Creates an underlying state machine for a Raft engine.
    pub fn new_state(kv: MVCC) -> Result<StateMachine> {
        StateMachine::new(kv)
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
        RaftSqlTxn::begin(self.client.clone(), mode)
    }

    fn resume(&self, id: u64) -> Result<Self::EngineTxn> {
        RaftSqlTxn::resume(self.client.clone(), id)
    }
}

/// A Raft-based SQL transaction
#[derive(Clone)]
pub struct RaftSqlTxn {
    /// The underlying Raft cluster
    client: raft::KvClient,
    /// The transaction ID
    id: u64,
    /// The transaction mode
    mode: Mode,
}

impl RaftSqlTxn {
    /// Begins a new transaction.
    pub fn begin(mut client: raft::KvClient, mode: Mode) -> Result<Self> {
        let id = RaftSqlEngine::deserialize(&futures::executor::block_on(
            client.execute(RaftSqlEngine::serialize(&Operation::Begin(mode))?)
        )?)?;
        Ok(Self { client, id, mode })
    }

    /// Resumes an active transaction.
    pub fn resume(client: raft::KvClient, id: u64) -> Result<Self> {
        let (id, mode) = RaftSqlEngine::deserialize(&futures::executor::block_on(
            client.query(RaftSqlEngine::serialize(&Query::Resume(id))?)
        )?)?;
        Ok(Self { client, id, mode })
    }

    /// Executes an mutation.
    fn execute(&mut self, op: Operation) -> Result<Vec<u8>> {
        futures::executor::block_on(self.client.execute(RaftSqlEngine::serialize(&op)?))
    }

    /// Executes an query.
    fn query(&self, query: Query) -> Result<Vec<u8>> {
        futures::executor::block_on(self.client.query(RaftSqlEngine::serialize(&query)?))
    }
}

impl SqlTxn for RaftSqlTxn {
    fn id(&self) -> u64 {
        self.id
    }

    fn mode(&self) -> Mode {
        self.mode
    }

    fn commit(mut self) -> Result<()> {
        RaftSqlEngine::deserialize(&self.execute(Operation::Commit(self.id))?)
    }

    fn rollback(mut self) -> Result<()> {
        RaftSqlEngine::deserialize(&self.execute(Operation::Rollback(self.id))?)
    }

    fn create(&mut self, table: &str, row: Row) -> Result<()> {
        RaftSqlEngine::deserialize(&self.execute(
            Operation::Create {
                txn_id: self.id,
                table: table.to_string(),
                row,
            }
        )?)
    }

    fn read(&self, table: &str, id: &Value) -> Result<Option<Row>> {
        RaftSqlEngine::deserialize(&self.query(
            Query::Read {
                txn_id: self.id,
                table: table.to_string(),
                id: id.clone(),
            }
        )?)
    }

    fn update(&mut self, table: &str, id: &Value, row: Row) -> Result<()> {
        RaftSqlEngine::deserialize(&self.execute(
            Operation::Update {
                txn_id: self.id,
                table: table.to_string(),
                id: id.clone(),
                row,
            }
        )?)
    }

    fn delete(&mut self, table: &str, id: &Value) -> Result<()> {
        RaftSqlEngine::deserialize(&self.execute(
            Operation::Delete {
                txn_id: self.id,
                table: table.to_string(),
                id: id.clone(),
            }
        )?)
    }

    fn read_index(&self, table: &str, column: &str, value: &Value) -> Result<HashSet<Value>> {
        RaftSqlEngine::deserialize(&self.query(
            Query::ReadIndex {
                txn_id: self.id,
                table: table.to_string(),
                column: column.to_string(),
                value: value.clone(),
            }
        )?)
    }

    fn scan(&self, table: &str, filter: Option<Expression>) -> Result<RowScan> {
        Ok(Box::new(
            RaftSqlEngine::deserialize::<Vec<_>>(&self.query(
                Query::Scan {
                    txn_id: self.id,
                    table: table.to_string(),
                    filter,
                }
            )?)?
            .into_iter()
            .map(Ok)
        ))
    }

    fn scan_index(&self, table: &str, column: &str) -> Result<IndexScan> {
        Ok(Box::new(
            RaftSqlEngine::deserialize::<Vec<_>>(&self.query(
                Query::ScanIndex {
                    txn_id: self.id,
                    table: table.to_string(),
                    column: column.to_string(),
                }
            )?)?
            .into_iter()
            .map(Ok)
        ))
    }
}

impl Catalog for RaftSqlTxn {
    fn create_table(&mut self, table: Table) -> Result<()> {
        RaftSqlEngine::deserialize(&self.execute(
            Operation::CreateTable {
                txn_id: self.id,
                schema: table,
            }
        )?)
    }

    fn read_table(&self, table: &str) -> Result<Option<Table>> {
        RaftSqlEngine::deserialize(&self.query(
            Query::ReadTable {
                txn_id: self.id,
                table: table.to_string(),
            }
        )?)
    }

    fn delete_table(&mut self, table: &str) -> Result<()> {
        RaftSqlEngine::deserialize(&self.execute(
            Operation::DeleteTable {
                txn_id: self.id,
                table: table.to_string(),
            }
        )?)
    }

    fn scan_tables(&self) -> Result<Tables> {
        Ok(Box::new(
            RaftSqlEngine::deserialize::<Vec<_>>(&self.query(
                Query::ScanTables {
                    txn_id: self.id,
                }
            )?)?
            .into_iter()
        ))
    }
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

    fn execute(&mut self, index: u64, operation: Vec<u8>) -> Result<Vec<u8>> {
        let result = match RaftSqlEngine::deserialize(&operation)? {
            Operation::Begin(mode) => RaftSqlEngine::serialize(&self.engine.begin(mode)?.id()),
            Operation::Commit(txn_id) => RaftSqlEngine::serialize(&self.engine.resume(txn_id)?.commit()?),
            Operation::Rollback(txn_id) => RaftSqlEngine::serialize(&self.engine.resume(txn_id)?.rollback()?),

            Operation::Create { txn_id, table, row } => {
                RaftSqlEngine::serialize(&self.engine.resume(txn_id)?.create(&table, row)?)
            }
            Operation::Delete { txn_id, table, id } => {
                RaftSqlEngine::serialize(&self.engine.resume(txn_id)?.delete(&table, &id)?)
            }
            Operation::Update { txn_id, table, id, row } => {
                RaftSqlEngine::serialize(&self.engine.resume(txn_id)?.update(&table, &id, row)?)
            }

            Operation::CreateTable { txn_id, schema } => {
                RaftSqlEngine::serialize(&self.engine.resume(txn_id)?.create_table(schema)?)
            }
            Operation::DeleteTable { txn_id, table } => {
                RaftSqlEngine::serialize(&self.engine.resume(txn_id)?.delete_table(&table)?)
            }
        };

        // We don't check that index == applied_index + 1, since the Raft log commits no-op
        // entries during leader election which we need to ignore.
        match result {
            error @ Err(Error::Internal(_)) => error,
            result => {
                self.engine.set_metadata(b"applied_index", RaftSqlEngine::serialize(&(index))?)?;
                self.applied_index = index;
                result
            }
        }
    }

    fn query(&self, command: Vec<u8>) -> Result<Vec<u8>> {
        match RaftSqlEngine::deserialize(&command)? {
            Query::Resume(id) => {
                let txn = self.engine.resume(id)?;
                RaftSqlEngine::serialize(&(txn.id(), txn.mode()))
            },
            Query::Read { txn_id, table, id } => {
                RaftSqlEngine::serialize(&self.engine.resume(txn_id)?.read(&table, &id)?)
            },
            Query::ReadIndex { txn_id, table, column, value } => {
                RaftSqlEngine::serialize(&self.engine.resume(txn_id)?.read_index(&table, &column, &value)?)
            },
            // FIXME: These need to stream rows somehow
            Query::Scan { txn_id, table, filter } => RaftSqlEngine::serialize(
                &self.engine.resume(txn_id)?.scan(&table, filter)?.collect::<Result<Vec<_>>>()?,
            ),
            Query::ScanIndex { txn_id, table, column } => RaftSqlEngine::serialize(
                &self
                    .engine
                    .resume(txn_id)?
                    .scan_index(&table, &column)?
                    .collect::<Result<Vec<_>>>()?,
            ),
            // Query::Status => RaftSqlEngine::serialize(&self.engine.kv.status()?),

            Query::ReadTable { txn_id, table } => {
                RaftSqlEngine::serialize(&self.engine.resume(txn_id)?.read_table(&table)?)
            },
            Query::ScanTables { txn_id } => {
                RaftSqlEngine::serialize(&self.engine.resume(txn_id)?.scan_tables()?.collect::<Vec<_>>())
            },
        }
    }
}