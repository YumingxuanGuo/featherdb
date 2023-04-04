use std::collections::BTreeMap;

use crate::sql::types::DataType;

#[derive(Clone, Debug, PartialEq)]
pub enum Statement {
    Begin {
        read_only: bool,
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

/// A column
#[derive(Clone, Debug, PartialEq)]
pub struct Column {
    pub name: String,
    pub datatype: DataType,
    pub is_primary_key: bool,
    pub is_nullable: Option<bool>,
    pub default: Option<Expression>,
    pub is_unique: bool,
    pub is_indexed: bool,
    pub references: Option<String>,
}

/// Expressions
#[derive(Clone, Debug, PartialEq)]
pub enum Expression {
    Field(Option<String>, String),
    Column(usize), // only used during plan building to break off expression subtrees
    Literal(Literal),
    Function(String, Vec<Expression>),
    Operation(Operation),
}

impl From<Literal> for Expression {
    fn from(literal: Literal) -> Self {
        Self::Literal(literal)
    }
}

impl From<Operation> for Expression {
    fn from(op: Operation) -> Self {
        Self::Operation(op)
    }
}

/// Literals
#[derive(Clone, Debug, PartialEq)]
pub enum Literal {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
}

/// Operations (done by operators)
#[derive(Clone, Debug, PartialEq)]
pub enum Operation {
    // Logical operators
    And(Box<Expression>, Box<Expression>),
    Not(Box<Expression>),
    Or(Box<Expression>, Box<Expression>),

    // Comparison operators
    Equal(Box<Expression>, Box<Expression>),
    GreaterThan(Box<Expression>, Box<Expression>),
    GreaterThanOrEqual(Box<Expression>, Box<Expression>),
    IsNull(Box<Expression>),
    LessThan(Box<Expression>, Box<Expression>),
    LessThanOrEqual(Box<Expression>, Box<Expression>),
    NotEqual(Box<Expression>, Box<Expression>),

    // Mathematical operators
    Add(Box<Expression>, Box<Expression>),
    Assert(Box<Expression>),
    Divide(Box<Expression>, Box<Expression>),
    Exponentiate(Box<Expression>, Box<Expression>),
    Factorial(Box<Expression>),
    Modulo(Box<Expression>, Box<Expression>),
    Multiply(Box<Expression>, Box<Expression>),
    Negate(Box<Expression>),
    Subtract(Box<Expression>, Box<Expression>),

    // String operators
    Like(Box<Expression>, Box<Expression>),
}