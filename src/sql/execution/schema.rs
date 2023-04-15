use crate::error::Result;
use crate::sql::engine::SqlTxn;
use crate::sql::schema::Table;
use super::{Executor, ResultSet};

/// A CREATE TABLE executor
pub struct CreateTableExec {
    table: Table,
}

impl CreateTableExec {
    pub fn new(table: Table) -> Box<Self> {
        Box::new(Self { table })
    }
}

impl<T: SqlTxn> Executor<T> for CreateTableExec {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        let name = self.table.name.clone();
        txn.create_table(self.table)?;
        Ok(ResultSet::CreateTable { name })
    }
}

/// A DROP TABLE executor
pub struct DropTableExec {
    table: String,
}

impl DropTableExec {
    pub fn new(table: String) -> Box<Self> {
        Box::new(Self { table })
    }
}

impl<T: SqlTxn> Executor<T> for DropTableExec {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        txn.delete_table(&self.table)?;
        Ok(ResultSet::DropTable { name: self.table })
    }
}