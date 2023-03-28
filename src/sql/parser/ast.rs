use std::collections::BTreeMap;

use crate::sql::schema::Column;
use crate::sql::types::Expression;

pub enum Statement {
    Begin {
        readonly: bool,
        version: Option<u64>,
    },
    Commit,
    Rollback,

    CreateTable {
        name: String,
        columns: Vec<Column>,
    },
    DropTable(String),

    Insert {
        table: String,
        columns: Option<Vec<String>>,
        values: Vec<Vec<Expression>>,
    },
    Update {
        table: String,
        set: BTreeMap<String, Expression>,
        r#where: Option<Expression>,
    },
    Delete {
        table: String,
        r#where: Option<Expression>,
    },
}