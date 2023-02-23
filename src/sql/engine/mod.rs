// The SQL engine provides fundamental CRUD storage operations.

use crate::{concurrency::Mode, error::Result};

use super::{schema::Catalog, types::Row};



/// The SQL engine interface
pub trait Engine: Clone {
    type Txn: SqlTxn;

    /// Begins a transaction in the given mode.
    fn begin(&self, mode: Mode) -> Result<Self::Txn>;

    /// Begins a session for executing individual statements.
    fn session(&self) -> Result<Session<Self>> {
        Ok(Session { engine: self.clone(), txn: None })
    }

    /// Resumes an active transaction with the given ID.
    fn resume(&self, id: u64) -> Result<Self::Txn>;
}

/// An SQL transaction
pub trait SqlTxn: Catalog {
    /// Gets the txn ID.
    fn get_id(&self) -> u64;
    /// Gets the txn mode.
    fn get_mode(&self) -> Mode;
    /// Commits the txn.
    fn commit(self) -> Result<()>;
    /// Rolls back the txn.
    fn roolback(self) -> Result<()>;

    /// Creates a new table row.
    fn create() -> Result<()>;
    /// Reads a table row, if it exists.
    fn read() -> Result<Option<Row>>;
    /// Updates a table row
    fn update() -> Result<()>;
    /// Deletes a table row.
    fn delete() -> Result<()>;
}

/// An SQL session, which handles transaction control and simplified query execution
pub struct Session<E: Engine> {
    /// The underlying engine
    engine: E,
    /// The current session transaction, if any
    txn: Option<E::Txn>,
}