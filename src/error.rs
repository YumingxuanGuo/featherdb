use serde_derive::{Deserialize, Serialize};

/// Result returning Error
pub type Result<T> = std::result::Result<T, Error>;

/// toyDB errors. All except Internal are considered user-facing.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Error {
    Abort,
    Config(String),
    Internal(String),
    Parse(String),
    ReadOnly,
    Serialization,
    Value(String),
}