#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]

// The SQL engine provides fundamental CRUD storage operations.
mod kv;

use std::collections::HashSet;

use crate::concurrency::Mode;
use crate::error::{Error, Result};
use super::execution::ResultSet;
use super::parser::Parser;
use super::parser::ast;
use super::plan::Plan;
use super::schema::Catalog;
use super::types::{Row, Value, Expression};


/// The SQL engine interface
pub trait SqlEngine: Clone {
    /// The engine transaction type.
    type EngineTxn: SqlTxn;

    /// Begins a transaction in the given mode
    fn begin(&self, mode: Mode) -> Result<Self::EngineTxn>;

    /// Begins a session for executing individual statements
    fn session(&self) -> Result<SqlSession<Self>> {
        Ok(SqlSession { engine: self.clone(), txn: None })
    }

    /// Resumes an active transaction with the given ID
    fn resume(&self, id: u64) -> Result<Self::EngineTxn>;
}

/// An SQL transaction
pub trait SqlTxn: Catalog {
    /// The transaction ID.
    fn id(&self) -> u64;
    /// The transaction mode.
    fn mode(&self) -> Mode;
    /// Commits the transaction
    fn commit(self) -> Result<()>;
    /// Rolls back the transaction
    fn rollback(self) -> Result<()>;

    /// Creates a new table row.
    fn create(&mut self, table: &str, row: Row) -> Result<()>;
    /// Deletes a table row
    fn delete(&mut self, table: &str, id: &Value) -> Result<()>;
    /// Reads a table row, if it exists
    fn read(&self, table: &str, id: &Value) -> Result<Option<Row>>;
    /// Reads an index entry, if it exists
    fn read_index(&self, table: &str, column: &str, value: &Value) -> Result<HashSet<Value>>;
    /// Scans a table's rows
    fn scan(&self, table: &str, filter: Option<Expression>) -> Result<RowScan>;
    /// Scans a column's index entries
    fn scan_index(&self, table: &str, column: &str) -> Result<IndexScan>;
    /// Updates a table row
    fn update(&mut self, table: &str, id: &Value, row: Row) -> Result<()>;
}

/// An SQL session, which handles transaction control and simplified query execution
pub struct SqlSession<E: SqlEngine> {
    /// The underlying engine
    engine: E,
    /// The current session transaction, if any
    txn: Option<E::EngineTxn>,
}

impl <E: SqlEngine + 'static> SqlSession<E> {
    /// Executes a query, managing transaction status for the session.
    pub fn execute(&mut self, query: &str) -> Result<ResultSet> {
        match Parser::new(query).parse()? {
            ast::Statement::Begin { .. } if self.txn.is_none() => {
                Err(Error::Value("Already in a transaction".into()))
            },
            ast::Statement::Begin { read_only: true, version: None } => {
                let txn = self.engine.begin(Mode::ReadOnly)?;
                let result = ResultSet::Begin { id: txn.id(), mode: txn.mode() };
                self.txn = Some(txn);
                Ok(result)
            },
            ast::Statement::Begin { read_only: false, version: None } => {
                let txn = self.engine.begin(Mode::ReadWrite)?;
                let result = ResultSet::Begin { id: txn.id(), mode: txn.mode() };
                self.txn = Some(txn);
                Ok(result)
            },
            ast::Statement::Begin { read_only: true, version: Some(version) } => {
                let txn = self.engine.begin(Mode::Snapshot { version })?;
                let result = ResultSet::Begin { id: txn.id(), mode: txn.mode() };
                self.txn = Some(txn);
                Ok(result)
            },
            ast::Statement::Begin { read_only: false, version: Some(_) } => {
                Err(Error::Value("Cannot specify version for read-write transactions".into()))
            },

            ast::Statement::Commit { .. } if self.txn.is_none() => {
                Err(Error::Value("Not in a transaction".into()))
            },
            ast::Statement::Commit { .. } => {
                let txn = self.txn.take().unwrap();
                let id = txn.id();
                // If the commit fails, we try to recover the transaction.
                if let Err(err) = txn.commit() {
                    if let Ok(t) = self.engine.resume(id) {
                        self.txn = Some(t);
                    }
                    return Err(err);
                }
                Ok(ResultSet::Commit { id })
            },

            ast::Statement::Rollback if self.txn.is_none() => {
                Err(Error::Value("Not in a transaction".into()))
            },
            ast::Statement::Rollback => {
                let txn = self.txn.take().unwrap();
                let id = txn.id();
                // If the rollback fails, we try to recover the transaction.
                if let Err(err) = txn.rollback() {
                    if let Ok(t) = self.engine.resume(id) {
                        self.txn = Some(t);
                    }
                    return Err(err);
                }
                Ok(ResultSet::Rollback { id })
            },

            statement if self.txn.is_some() => {
                Plan::build(statement, self.txn.as_mut().unwrap())?
                    .execute(self.txn.as_mut().unwrap())
            },
            statement => {
                let mut txn = self.engine.begin(Mode::ReadWrite)?;
                match Plan::build(statement, &mut txn)?.execute(&mut txn) {
                    Ok(result) => {
                        txn.commit()?;
                        Ok(result)
                    },
                    Err(err) => {
                        txn.rollback()?;
                        Err(err)
                    }
                }
            },

            #[allow(unreachable_patterns)]
            _ => unreachable!()
        }
    }
}

/// A row scan iterator
pub type RowScan = Box<dyn DoubleEndedIterator<Item = Result<Row>> + Send>;

/// An index scan iterator
pub type IndexScan = Box<dyn DoubleEndedIterator<Item = Result<(Value, HashSet<Value>)>> + Send>;