use serde_derive::{Deserialize, Serialize};

use crate::error::{Error, Result};

use super::{Row, Value};

/// An expression, made up of constants and operations
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Expression {

}

impl Expression {
    /// Evaluates an expression to a value, given an environment.
    pub fn evaluate(&self, env: Option<&Row>) -> Result<Value> {
        todo!()
    }
}