use serde_derive::{Deserialize, Serialize};

use super::schema::Table;
use super::types::{Expression, Value};


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
}