#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]
#![allow(unused_mut)]

use std::fmt::{self, Display};
use serde_derive::{Deserialize, Serialize};

use crate::error::Result;
use super::engine::SqlTxn;
use super::execution::{Executor, ResultSet};
use super::parser::ast;
use super::schema::{Table, Catalog};
use super::types::{Expression, Value};

/// A query plan
#[derive(Debug)]
pub struct Plan{
    pub node: Node
}

impl Plan {
    /// Builds a plan from a AST statement.
    pub fn build<C: Catalog>(statement: ast::Statement, catalog: &mut C) -> Result<Self> {
        todo!()
    }

    /// Executes the plan, consuming it and returning a result set.
    pub fn execute<T: SqlTxn + 'static>(self, txn: &mut T) -> Result<ResultSet> {
        <dyn Executor<T>>::build(self.node).execute(txn)
    }
}

impl Display for Plan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.node.to_string())
    }
}


/// A plan node
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum Node {
    CreateTable { schema: Table },
    DropTable { name: String },

    Insert {
        table: String,
        columns: Vec<String>,
        expression: Vec<Vec<Expression>>,
    },
    KeyLookup {
        table: String,
        alias: Option<String>,
        keys: Vec<Value>,
    },
    Update {
        table: String,
        source: Box<Node>,
        expressions: Vec<(usize, Option<String>, Expression)>,
    },
    Delete {
        table: String,
        source: Box<Node>,
    },
}

impl Node {
    // Displays the node, where prefix gives the node prefix.
    pub fn format(&self, mut indent: String, root: bool, last: bool) -> String {
        todo!()
    }
}

impl Display for Node {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.format("".into(), true, true))
    }
}