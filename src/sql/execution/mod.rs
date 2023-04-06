#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]

mod mutation;
mod query;
mod schema;
mod source;

use derivative::Derivative;
use serde_derive::{Deserialize, Serialize};

use crate::concurrency::Mode;
use crate::error::{Result, Error};
use self::mutation::{InsertExec, UpdateExec, DeleteExec};
use self::schema::{CreateTableExec, DropTableExec};
use self::source::{KeyLookupExec, Scan};

use super::engine::SqlTxn;
use super::plan::Node;
use super::types::{Rows, Columns, Value, Row};

/// A plan executor.
pub trait Executor<T: SqlTxn> {
    /// Executes the executor, consuming it and returning a result set.
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet>;
}

impl<T: SqlTxn + 'static> dyn Executor<T> {
    /// Builds an executor for a plan node, consuming it.
    pub fn build(node: Node) -> Box<dyn Executor<T>> {
        match node {
            Node::CreateTable { schema } => CreateTableExec::new(schema),
            Node::DropTable { table } => DropTableExec::new(table),

            Node::Insert { table, columns, expression } => {
                InsertExec::new(table, columns, expression)
            },
            Node::KeyLookup { table, alias, keys } => {
                KeyLookupExec::new(table, keys)
            },
            Node::Update { table, source, expressions } => UpdateExec::new(
                table,
                Self::build(*source),
                expressions.into_iter().map(|(i, _, e)| (i, e)).collect(),
            ),
            Node::Delete { table, source } => DeleteExec::new(table, Self::build(*source)),

            Node::Scan { table, filter, alias: _ } => Scan::new(table, filter),
        }
    }
}

#[derive(Derivative, Serialize, Deserialize)]
#[derivative(Debug, PartialEq)]
pub enum ResultSet {
    /// Transaction started
    Begin { id: u64, mode: Mode },
    /// Transaction committed
    Commit { id: u64 },
    /// Transaction rolled back
    Rollback { id: u64 },

    /// Rows created
    Create { count: u64 },
    /// Rows updated
    Update { count: u64 },
    /// Rows deleted
    Delete { count: u64 },
    /// Query results
    Query {
        columns: Columns, 
        #[derivative(Debug = "ignore")]
        #[derivative(PartialEq = "ignore")]
        #[serde(skip, default = "ResultSet::empty_rows")]
        rows: Rows,
    },

    /// Table created
    CreateTable { name: String },
    /// Table dropped
    DropTable { name: String },

    /// Explain result
    Explain(Node),
}

impl ResultSet {
    fn empty_rows() -> Rows {
        Box::new(std::iter::empty())
    }

    /// Converts the ResultSet into a row, or errors if not a query result with rows.
    pub fn into_row(self) -> Result<Row> {
        if let ResultSet::Query { mut rows, .. } = self {
            rows.next().transpose()?.ok_or_else(|| Error::Value("No rows returned".into()))
        } else {
            Err(Error::Value(format!("Not a query result: {:?}", self)))
        }
    }

    /// Converts the ResultSet into a value, if possible. TODO: Read.
    pub fn into_value(self) -> Result<Value> {
        self.into_row()?.into_iter().next().ok_or_else(|| Error::Value("No value returned".into()))
    }
}



#[cfg(test)]
mod tests {
    use crate::sql::schema::Column;
    use crate::sql::types::{DataType, Value};
    
    pub fn set_up_column(name: String, default: Option<Value>, is_primary_key: bool) -> Column {
        Column {
            name,
            datatype: DataType::Integer,
            default,
            is_indexed: false,
            is_nullable: false,
            is_primary_key,
            is_unique: true,
            references: None,
        }
    }
}