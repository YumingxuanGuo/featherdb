#![allow(dead_code)]
use std::borrow::Cow;
use std::collections::HashSet;

use crate::concurrency::{MVCC, Transaction, Mode};
use crate::error::{Error, Result};
use crate::sql::schema::{Catalog, Table};
use crate::sql::types::{Row, Value, Expression};

use super::{SqlTxn, SqlEngine, RowScan, IndexScan};


/// A SQL engine based on an underlying MVCC key/value store
#[derive(Clone)]
pub struct KvSqlEngine {
    /// The underlying key/value store
    pub(super) kv: MVCC,
}

impl KvSqlEngine {
    /// Creates a new SQL engine.
    pub fn new(kv: MVCC) -> Self {
        Self { kv }
    }

    /// Fetches an unversioned metadata value.
    pub fn get_metadata(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.kv.get_metadata(key)
    }

    /// Sets an unversioned metadata value.
    pub fn set_metadata(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.kv.set_metadata(key, value)
    }
}

impl SqlEngine for KvSqlEngine {
    type EngineTxn = KvSqlTxn;

    fn begin(&self, mode: Mode) -> Result<Self::EngineTxn> {
        Ok(KvSqlTxn { txn: self.kv.begin_with_mode(mode)? })
    }

    fn resume(&self, id: u64) -> Result<Self::EngineTxn> {
        Ok(KvSqlTxn { txn: self.kv.resume(id)? })
    }
}

/// An SQL transaction based on an MVCC key/value transaction
pub struct KvSqlTxn {
    txn: Transaction,
}

impl SqlTxn for KvSqlTxn {
    fn id(&self) -> u64 {
        self.txn.id()
    }

    fn mode(&self) -> Mode {
        self.txn.mode()
    }

    fn commit(self) -> Result<()> {
        self.txn.commit()
    }

    fn rollback(self) -> Result<()> {
        self.txn.rollback()
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

impl Catalog for KvSqlTxn {
    fn create_table(&mut self, table: Table) -> Result<()> {
        todo!()
    }

    fn read_table(&self, table: &str) -> Result<Option<Table>> {
        todo!()
    }

    fn assert_read_table(&self, table: &str) -> Result<Table> {
        todo!()
    }

    fn delete_table(&mut self, table: &str) -> Result<()> {
        todo!()
    }

    fn scan_tables(&self) -> Result<crate::sql::schema::Tables> {
        todo!()
    }
}