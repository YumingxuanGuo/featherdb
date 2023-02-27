use std::collections::HashSet;

use serde_derive::{Serialize, Deserialize};

use crate::{raft, storage::kv, error::{Result, Error}, common::{deserialize, serialize}, sql::{types::{Row, Value, Expression}, schema::{Table, Catalog}}};
use super::{Engine as _, SqlTxn as _, Mode, kv_engine::KvEngine, RowScan, IndexScan};

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

impl super::Engine for RaftEngine {
    type Txn = RaftTxn;

    fn begin(&self, mode: Mode) -> Result<Self::Txn> {
        RaftTxn::begin(self.client.clone(), mode)
    }

    fn resume(&self, id: u64) -> Result<Self::Txn> {
        RaftTxn::resume(self.client.clone(), id)
    }
}

/// A Raft-based SQL transaction
#[derive(Clone)]
pub struct RaftTxn {
    /// The underlying Raft cluster
    client: raft::Client,
    /// The transaction ID
    id: u64,
    /// The transaction mode
    mode: Mode,
}

impl RaftTxn {
    /// Starts a transaction in the given mode.
    fn begin(client: raft::Client, mode: Mode) -> Result<Self> {
        let id = deserialize(&futures::executor::block_on(
            client.mutate(serialize(&Mutation::Begin(mode))?)
        )?)?;
        Ok(Self { client, id, mode })
    }

    /// Resumes an active transaction.
    fn resume(client: raft::Client, id: u64) -> Result<Self> {
        let (id, mode) = deserialize(&futures::executor::block_on(
            client.query(serialize(&Query::Resume(id))?)
        )?)?;
        Ok(Self { client, id, mode })
    }

    /// Executes a mutation
    fn mutate(&self, mutation: Mutation) -> Result<Vec<u8>> {
        futures::executor::block_on(self.client.mutate(serialize(&mutation)?))
    }

    /// Executes a query
    fn query(&self, query: Query) -> Result<Vec<u8>> {
        futures::executor::block_on(self.client.query(serialize(&query)?))
    }
}

impl super::SqlTxn for RaftTxn {
    fn get_id(&self) -> u64 {
        self.id
    }

    fn get_mode(&self) -> Mode {
        self.mode
    }

    fn commit(self) -> Result<()> {
        deserialize(&self.mutate(Mutation::Commit(self.id))?)
    }

    fn rollback(self) -> Result<()> {
        deserialize(&self.mutate(Mutation::Rollback(self.id))?)
    }

    fn create(&mut self, table_name: &str, row: Row) -> Result<()> {
        todo!()
    }

    fn read(&self, table_name: &str, primary_key: &Value) -> Result<Option<Row>> {
        todo!()
    }

    fn update(&mut self, table_name: &str, primary_key: &Value, row: Row) -> Result<()> {
        todo!()
    }

    fn delete(&mut self, table_name: &str, primary_key: &Value) -> Result<()> {
        todo!()
    }

    fn scan_row(&self, table_name: &str, filter: Option<Expression>) -> Result<RowScan> {
        todo!()
    }

    fn scan_index(&self, table_name: &str, column_name: &str) -> Result<IndexScan> {
        todo!()
    }

    fn read_index(&self, table_name: &str, column_name: &str, value: &Value) -> Result<HashSet<Value>> {
        todo!()
    }
}

impl Catalog for RaftTxn {
    fn create_table(&mut self, table: Table) -> Result<()> {
        todo!()
    }

    fn read_table(&self, table_name: &str) -> Result<Option<Table>> {
        todo!()
    }

    fn delete_table(&mut self, table_name: &str) -> Result<()> {
        todo!()
    }

    fn scan_tables(&self) -> Result<crate::sql::schema::Tables> {
        todo!()
    }

    fn read_table_or_error(&self, table_name: &str) -> Result<Table> {
        todo!()
    }

    fn create_index(&mut self, table_name: &str, column_name: &str) -> Result<()> {
        todo!()
    }

    fn get_references(&self, table_name: &str, with_self: bool) -> Result<Vec<(String, Vec<String>)>> {
        todo!()
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

            Mutation::Create { txn_id, table_name, row } => {
                serialize(&self.engine.resume(txn_id)?.create(&table_name, row)?)
            }
            Mutation::Delete { txn_id, table_name, primary_key } => {
                serialize(&self.engine.resume(txn_id)?.delete(&table_name, &primary_key)?)
            }
            Mutation::Update { txn_id, table_name, primary_key, row } => {
                serialize(&self.engine.resume(txn_id)?.update(&table_name, &primary_key, row)?)
            }

            Mutation::CreateTable { txn_id, schema } => {
                serialize(&self.engine.resume(txn_id)?.create_table(schema)?)
            }
            Mutation::DeleteTable { txn_id, table_name } => {
                serialize(&self.engine.resume(txn_id)?.delete_table(&table_name)?)
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
        match deserialize(&command)? {
            Query::Resume(id) => {
                let txn = self.engine.resume(id)?;
                serialize(&(txn.get_id(), txn.get_mode()))
            },
            Query::Read { txn_id, table_name, primary_key } => {
                serialize(&self.engine.resume(txn_id)?.read(&table_name, &primary_key)?)
            },
            Query::ReadIndex { txn_id, table_name, column_name, value } => {
                serialize(&self.engine.resume(txn_id)?.read_index(&table_name, &column_name, &value)?)
            },
            Query::Scan { txn_id, table_name, filter } => serialize(
                &self.engine.resume(txn_id)?.scan_row(&table_name, filter)?.collect::<Result<Vec<_>>>()?
            ),
            Query::ScanIndex { txn_id, table_name, column_name } => serialize(
                &self
                    .engine
                    .resume(txn_id)?
                    .scan_index(&table_name, &column_name)?
                    .collect::<Result<Vec<_>>>()?
            ),
            Query::Status => serialize(&self.engine.kv.get_status()?),
            Query::ReadTable { txn_id, table_name } => {
                serialize(&self.engine.resume(txn_id)?.read_table(&table_name)?)
            },
            Query::ScanTables { txn_id } => {
                serialize(&self.engine.resume(txn_id)?.scan_tables()?.collect::<Vec<_>>())
            }
        }
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
    Create { txn_id: u64, table_name: String, row: Row },
    /// Deletes a row
    Delete { txn_id: u64, table_name: String, primary_key: Value },
    /// Updates a row
    Update { txn_id: u64, table_name: String, primary_key: Value, row: Row },

    /// Creates a table
    CreateTable { txn_id: u64, schema: Table },
    /// Deletes a table
    DeleteTable { txn_id: u64, table_name: String },
}

/// A Raft state machine query
#[derive(Clone, Serialize, Deserialize)]
enum Query {
    /// Fetches engine status
    Status,
    /// Resumes the active transaction with the given ID
    Resume(u64),

    /// Reads a row
    Read { txn_id: u64, table_name: String, primary_key: Value },
    /// Reads an index entry
    ReadIndex { txn_id: u64, table_name: String, column_name: String, value: Value },
    /// Scans a table's rows
    Scan { txn_id: u64, table_name: String, filter: Option<Expression> },
    /// Scans an index
    ScanIndex { txn_id: u64, table_name: String, column_name: String },

    /// Scans the tables
    ScanTables { txn_id: u64 },
    /// Reads a table
    ReadTable { txn_id: u64, table_name: String },
}