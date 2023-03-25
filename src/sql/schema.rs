#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]

use serde_derive::{Deserialize, Serialize};

use crate::error::{Error, Result};
use super::types::DataType;

/// The catalog stores schema information
pub trait Catalog {
    /// Creates a new table.
    fn create_table(&mut self, table: Table) -> Result<()>;
    /// Deletes an existing table, or errors if it does not exist.
    fn delete_table(&mut self, table: &str) -> Result<()>;
    /// Reads a table, or returns None if it does not exist.
    fn read_table(&self, table: &str) -> Result<Option<Table>>;
    /// Iterates over all tables.
    fn scan_tables(&self) -> Result<Tables>;

    /// Reads a table, and errors if it does not exist.
    fn assert_read_table(&self, table: &str) -> Result<Table> {
        self.read_table(table)?.ok_or_else(
            || Error::Value(format!("Table {} does not exist", table))
        )
    }

    /// Returns all references to a table, as (table,column) pairs.
    fn table_references(&self, table: &str, include_self: bool) -> Result<Vec<(String, Vec<String>)>> {
        Ok(self
            .scan_tables()?
            .filter(|t| include_self || t.name != table)
            .map(|t| {
                let references = t.columns
                    .iter()
                    .filter(|column| column.references.as_deref() == Some(table))
                    .map(|column| column.name.clone())
                    .collect::<Vec<_>>();
                (t.name, references)
            })
            .filter(|(_, references)| !references.is_empty())
            .collect()
        )
    }
}

/// A table schema
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
}

impl Table {
    
}

/// A table scan iterator
pub type Tables = Box<dyn DoubleEndedIterator<Item = Table> + Send>;

/// A table column schema
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct Column {
    /// Column name
    pub name: String,
    /// Column datatype
    pub datatype: DataType,
    /// Whether the column is a primary key
    pub is_primary_key: bool,
    /// Whether the column allows null values
    pub nullable: bool,
    // /// The default value of the column
    // pub default: Option<Value>,
    /// Whether the column should only take unique values
    pub unique: bool,
    /// The table which is referenced by this foreign key
    pub references: Option<String>,
    /// Whether the column should be indexed
    pub index: bool,
}

impl Column {
    
}