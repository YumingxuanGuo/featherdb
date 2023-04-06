use crate::error::Result;
use crate::sql::engine::SqlTxn;
use crate::sql::types::{ResColumn, Expression, Row, Value};
use super::{Executor, ResultSet};

use std::collections::HashSet;

/// A table scan executor
pub struct Scan {
    table: String,
    filter: Option<Expression>,
}

impl Scan {
    pub fn new(table: String, filter: Option<Expression>) -> Box<Self> {
        Box::new(Self { table, filter })
    }
}

impl<T: SqlTxn> Executor<T> for Scan {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        let table = txn.assert_read_table(&self.table)?;
        Ok(ResultSet::Query {
            columns: table.columns.iter().map(|c| ResColumn { name: Some(c.name.clone()) }).collect(),
            rows: Box::new(txn.scan(&table.name, self.filter)?),
        })
    }
}

/// A primary key loop-up executor
pub struct KeyLookupExec {
    table: String,
    keys: Vec<Value>,
}

impl KeyLookupExec {
    pub fn new(table: String, keys: Vec<Value>) -> Box<Self> {
        Box::new(Self { table, keys })
    }
}

impl<T: SqlTxn> Executor<T> for KeyLookupExec {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        let table = txn.assert_read_table(&self.table)?;

        // FIXME Is there a way to pass the txn into an iterator closure instead?
        let rows = self
            .keys
            .into_iter()
            .filter_map(|key| txn.read(&table.name, &key).transpose())
            .collect::<Result<Vec<Row>>>()?;

        Ok(ResultSet::Query {
            columns: table.columns.iter().map(|c| ResColumn { name: Some(c.name.clone()) }).collect(),
            rows: Box::new(rows.into_iter().map(Ok)),
        })
    }
}