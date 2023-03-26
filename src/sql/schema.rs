#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]

use serde_derive::{Deserialize, Serialize};

use crate::error::{Error, Result};
use super::types::{DataType, Value};
use super::engine::SqlTxn;

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
    /// Creates a new table schema.
    pub fn new(name: String, columns: Vec<Column>) -> Result<Self> {
        Ok(Self { name, columns })
    }

    /// Fetches a column index by name.
    pub fn get_column_index(&self, name: &str) -> Result<usize> {
        self.columns.iter().position(|column| column.name == name).ok_or_else(|| 
            Error::Value(format!("Column {} does not exist in table {}", name, self.name))
        )
    }   

    /// Returns the primary key value of a row.
    pub fn get_row_key(&self, row: &[Value]) -> Result<Value> {
        todo!()
    }

    /// Validates a table schema.
    pub fn validate(&self, txn: &mut dyn SqlTxn) -> Result<()> {
        if self.columns.is_empty() {
            return Err(Error::Value(format!("Table {} has no columns", self.name)));
        }
        match self.columns.iter().filter(|column| column.is_primary_key).count() {
            1 => { },
            0 => return Err(Error::Value(format!("Table {} has no primary key", self.name))),
            _ => return Err(Error::Value(format!("Table {} has multiple primary keys", self.name))),
        };
        for column in &self.columns {
            column.validate(self, txn)?;
        }
        Ok(())
    }

    /// Validates a row.
    pub fn validate_row(&self, row: &[Value], txn: &mut dyn SqlTxn) -> Result<()> {
        if row.len() != self.columns.len() {
            return Err(Error::Value(format!("Row has {} columns, expected {}", row.len(), self.columns.len())));
        }
        let primary_key = self.get_row_key(row)?;
        for (column, value) in self.columns.iter().zip(row.iter()) {
            column.validate_value(value, &primary_key, self, txn)?;
        }
        Ok(())
    }
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
    pub is_nullable: bool,
    /// The default value of the column
    pub default: Option<Value>,
    /// Whether the column should only take unique values
    pub is_unique: bool,
    /// The table which is referenced by this foreign key
    pub references: Option<String>,
    /// Whether the column should be indexed
    pub is_indexed: bool,
}

impl Column {
    /// Validates a column schema.
    pub fn validate(&self, table: &Table, txn: &mut dyn SqlTxn) -> Result<()> {
        todo!()
    }

    /// Validates a column value.
    pub fn validate_value(
        &self, 
        value: &Value, 
        primary_key: &Value,
        table: &Table, 
        txn: &mut dyn SqlTxn
    ) -> Result<()> {
        todo!()
    }
}