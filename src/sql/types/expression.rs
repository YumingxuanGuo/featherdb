use serde_derive::{Serialize, Deserialize};

use crate::error::Result;

use super::{Row, Value};



/// An expression, made up of constants and operations
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Expression {
    
}

impl Expression {
    /// Evaluates an expression to a value, given an environment
    pub fn evaluate(&self, row: Option<&Row>) -> Result<Value> {
        Ok(Value::Null)
    }
}