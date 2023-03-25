use serde_derive::{Deserialize, Serialize};

use crate::error::{Error, Result};

/// An expression, made up of constants and operations
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Expression {

}