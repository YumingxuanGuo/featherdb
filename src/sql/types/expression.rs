use crate::error::Result;

use super::{Row, Value};



pub enum Expression {
    
}

impl Expression {
    /// Evaluates an expression to a value, given an environment
    pub fn evaluate(&self, row: Option<&Row>) -> Result<Value> {
        Ok(Value::Null)
    }
}