
use crate::error::{Error, Result};
use serde_derive::{Deserialize, Serialize};

use super::types::{DataType, Value};

/// The catalog stores schema information
pub trait Catalog {
    /// Creates a new table.
    fn create_table(&mut self, table: Table) -> Result<()>;

    /// Reads a table, if it exists.
    fn read_table(&self, table_name: &str) -> Result<Option<Table>>;

    /// Deletes an existing table, or errors if it does not exist.
    fn delete_table(&mut self, table_name: &str) -> Result<()>;

    /// Iterates over all tables.
    fn scan_tables(&self) -> Result<Tables>;

    /// Creates an index on a table.
    fn create_index(&mut self, table_name: &str, column_name: &str) -> Result<()>;

    /// Reads a table, or errors if it does not exist.
    fn read_table_or_error(&self, table_name: &str) -> Result<Table> {
        self.read_table(table_name)?
            .ok_or_else(|| Error::Value(format!("Table {} does not exist", table_name)))
    }
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
}

pub type Tables = Box<dyn DoubleEndedIterator<Item = Table> + Send>;

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct Column {
    /// Column name
    pub name: String,
    /// Column datatype
    pub datatype: DataType,
    /// Whether the column is the primary key
    pub is_primary: bool,
    /// Whether the column allows null values
    pub is_nullable: bool,
    /// The default value of the column
    pub default: Option<Value>,
    /// Whether the column should only take unique values
    pub is_unique: bool,
    /// The table which is referenced by this foreign key
    pub references: Option<String>,
    /// Whether this column is indexed
    pub is_indexed: bool,
}
