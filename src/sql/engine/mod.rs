#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]

// The SQL engine provides fundamental CRUD storage operations.
mod kv;
pub mod raft;
pub use kv::KvSqlEngine;
pub use raft::{RaftSqlEngine, StateMachine};
pub use crate::concurrency::Mode;

use std::collections::HashSet;
use std::sync::Arc;
use parking_lot::Mutex;

use crate::error::{Error, Result};
use super::execution::ResultSet;
use super::parser::{Parser, ast};
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
        Ok(SqlSession { engine: self.clone(), txn: Arc::new(Mutex::new(None)) })
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
    txn: Arc<Mutex<Option<E::EngineTxn>>>,
}

impl <E: SqlEngine + 'static> SqlSession<E> {
    /// Executes a query, managing transaction status for the session.
    pub fn execute(&self, query: &str) -> Result<ResultSet> {
        let mut guard = self.txn.lock();
        match Parser::new(query).parse()? {
            ast::Statement::Begin { .. } if guard.is_some() => {
                Err(Error::Value("Already in a transaction".into()))
            },
            ast::Statement::Begin { read_only: true, version: None } => {
                let txn = self.engine.begin(Mode::ReadOnly)?;
                let result = ResultSet::Begin { id: txn.id(), mode: txn.mode() };
                *guard = Some(txn);
                Ok(result)
            },
            ast::Statement::Begin { read_only: false, version: None } => {
                let txn = self.engine.begin(Mode::ReadWrite)?;
                let result = ResultSet::Begin { id: txn.id(), mode: txn.mode() };
                *guard = Some(txn);
                Ok(result)
            },
            ast::Statement::Begin { read_only: true, version: Some(version) } => {
                let txn = self.engine.begin(Mode::Snapshot { version })?;
                let result = ResultSet::Begin { id: txn.id(), mode: txn.mode() };
                *guard = Some(txn);
                Ok(result)
            },
            ast::Statement::Begin { read_only: false, version: Some(_) } => {
                Err(Error::Value("Cannot specify version for read-write transactions".into()))
            },

            ast::Statement::Commit { .. } if guard.is_none() => {
                Err(Error::Value("Not in a transaction".into()))
            },
            ast::Statement::Commit { .. } => {
                let txn = guard.take().unwrap();
                let id = txn.id();
                // If the commit fails, we try to recover the transaction.
                if let Err(err) = txn.commit() {
                    if let Ok(t) = self.engine.resume(id) {
                        *guard = Some(t);
                    }
                    return Err(err);
                }
                Ok(ResultSet::Commit { id })
            },

            ast::Statement::Rollback if guard.is_none() => {
                Err(Error::Value("Not in a transaction".into()))
            },
            ast::Statement::Rollback => {
                let txn = guard.take().unwrap();
                let id = txn.id();
                // If the rollback fails, we try to recover the transaction.
                if let Err(err) = txn.rollback() {
                    if let Ok(t) = self.engine.resume(id) {
                        *guard = Some(t);
                    }
                    return Err(err);
                }
                Ok(ResultSet::Rollback { id })
            },

            statement if guard.is_some() => {
                Plan::build(statement, guard.as_mut().unwrap())?
                    .execute(guard.as_mut().unwrap())
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

    /// Runs a closure in the session's transaction, or a new transaction if none is active.
    pub fn with_txn<R, F>(&self, mode: Mode, func: F) -> Result<R>
    where
        F: FnOnce(&mut E::EngineTxn) -> Result<R>,
    {
        let mut guard = self.txn.lock();
        if let Some(ref mut txn) = *guard {
            if !txn.mode().satisfies(&mode) {
                return Err(Error::Value(
                    "The operation cannot run in the current transaction".into(),
                ));
            }
            return func(txn);
        }
        let mut txn = self.engine.begin(mode)?;
        let result = func(&mut txn);
        txn.rollback()?;
        result
    }
}

/// A row scan iterator
pub type RowScan = Box<dyn DoubleEndedIterator<Item = Result<Row>> + Send>;

/// An index scan iterator
pub type IndexScan = Box<dyn DoubleEndedIterator<Item = Result<(Value, HashSet<Value>)>> + Send>;