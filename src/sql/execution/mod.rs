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
use crate::error::Result;
use self::mutation::{InsertExec, UpdateExec, DeleteExec};
use self::schema::{CreateTableExec, DropTableExec};
use self::source::KeyLookupExec;

use super::engine::SqlTxn;
use super::plan::Node;
use super::types::{Rows, Columns};

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
            Node::DropTable { name } => DropTableExec::new(name),

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