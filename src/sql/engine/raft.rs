use std::collections::HashSet;

use crate::error::Result;
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