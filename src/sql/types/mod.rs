
use std::{hash::{Hash, Hasher}, borrow::Cow};

use serde_derive::{Deserialize, Serialize};

use crate::error::Result;

#[derive(Clone, Debug, Hash, PartialEq, Serialize, Deserialize)]
pub enum DataType {
    Boolean,
    Integer,
    Float,
    String,
}

/// A specific value of a data type
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
}

impl std::cmp::Eq for Value {}

#[allow(clippy::derive_hash_xor_eq)]
impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.datatype().hash(state);
        match self {
            Value::Null => self.hash(state),
            Value::Boolean(v) => v.hash(state),
            Value::Integer(v) => v.hash(state),
            Value::Float(v) => v.to_be_bytes().hash(state),
            Value::String(v) => v.hash(state),
        }
    }
}

impl<'a> From<Value> for Cow<'a, Value> {
    fn from(v: Value) -> Self {
        Cow::Owned(v)
    }
}

impl<'a> From<&'a Value> for Cow<'a, Value> {
    fn from(v: &'a Value) -> Self {
        Cow::Borrowed(v)
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(
            match self {
                Self::Null => "NULL".to_string(),
                Self::Boolean(b) if *b => "TRUE".to_string(),
                Self::Boolean(_) => "FALSE".to_string(),
                Self::Integer(i) => i.to_string(),
                Self::Float(f) => f.to_string(),
                Self::String(s) => s.clone(),
            }
            .as_ref(),
        )
    }
}

impl Value {
    /// Returns the value's datatype, or None for null values
    pub fn datatype(&self) -> Option<DataType> {
        match self {
            Self::Null => None,
            Self::Boolean(_) => Some(DataType::Boolean),
            Self::Integer(_) => Some(DataType::Integer),
            Self::Float(_) => Some(DataType::Float),
            Self::String(_) => Some(DataType::String),
        }
    }
}

/// A row of values
pub type Row = Vec<Value>;

/// A row iterator
pub type Rows = Box<dyn Iterator<Item = Result<Row>> + Send>;