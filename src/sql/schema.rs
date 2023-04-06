#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]

use std::fmt::{self, Display};

use serde_derive::{Deserialize, Serialize};

use crate::error::{Error, Result};
use super::engine::SqlTxn;
use super::parser::format_ident;
use super::types::{DataType, Value};

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

    /// Fetches a column by name.
    pub fn get_column(&self, name: &str) -> Result<&Column> {
        self.columns.iter().find(|column| column.name == name).ok_or_else(|| 
            Error::Value(format!("Column {} does not exist in table {}", name, self.name))
        )
    }

    /// Fetches a column index by name.
    pub fn get_column_index(&self, name: &str) -> Result<usize> {
        self.columns.iter().position(|column| column.name == name).ok_or_else(|| 
            Error::Value(format!("Column {} does not exist in table {}", name, self.name))
        )
    }

    /// Returns the primary key column of the table.
    pub fn get_primary_key(&self) -> Result<&Column> {
        self.columns.iter().find(|column| column.is_primary_key).ok_or_else(|| 
            Error::Value(format!("Primary key not found for table {}", self.name))
        )
    }

    /// Returns the primary key value of a row.
    pub fn get_row_key(&self, row: &[Value]) -> Result<Value> {
        row.get(
            self.columns.
                iter()
                .position(|column| column.is_primary_key)
                .ok_or_else(|| Error::Value(format!("Primary key not found for table {}", self.name)))?
        )
        .cloned()
        .ok_or_else(|| Error::Value(format!("Primary key value not found for row")))
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

impl Display for Table {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "CREATE TABLE {} (\n{}\n)",
            format_ident(&self.name),
            self.columns.iter().map(|c| format!("  {}", c)).collect::<Vec<String>>().join(",\n")
        )
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
    /// Validates a column schema. TODO: Read.
    pub fn validate(&self, table: &Table, txn: &mut dyn SqlTxn) -> Result<()> {
        // Validate primary key
        if self.is_primary_key && self.is_nullable {
            return Err(Error::Value(format!("Primary key {} cannot be nullable", self.name)));
        }
        if self.is_primary_key && !self.is_unique {
            return Err(Error::Value(format!("Primary key {} must be unique", self.name)));
        }

        // Validate default value
        if let Some(default) = &self.default {
            if let Some(datatype) = default.datatype() {
                if datatype != self.datatype {
                    return Err(Error::Value(format!(
                        "Default value for column {} has datatype {}, must be {}",
                        self.name, datatype, self.datatype
                    )));
                }
            } else if !self.is_nullable {
                return Err(Error::Value(format!(
                    "Can't use NULL as default value for non-nullable column {}",
                    self.name
                )));
            }
        } else if self.is_nullable {
            return Err(Error::Value(format!(
                "Nullable column {} must have a default value",
                self.name
            )));
        }

        // Validate references
        if let Some(reference) = &self.references {
            let target = if reference == &table.name {
                table.clone()
            } else if let Some(table) = txn.read_table(reference)? {
                table
            } else {
                return Err(Error::Value(format!(
                    "Table {} referenced by column {} does not exist",
                    reference, self.name
                )));
            };
            if self.datatype != target.get_primary_key()?.datatype {
                return Err(Error::Value(format!(
                    "Can't reference {} primary key of table {} from {} column {}",
                    target.get_primary_key()?.datatype,
                    target.name,
                    self.datatype,
                    self.name
                )));
            }
        }

        Ok(())
    }

    /// Validates a column value. TODO: Read.
    pub fn validate_value(
        &self, 
        value: &Value, 
        primary_key: &Value,
        table: &Table, 
        txn: &mut dyn SqlTxn
    ) -> Result<()> {
        // Validate datatype
        match value.datatype() {
            None if self.is_nullable => Ok(()),
            None => Err(Error::Value(format!("NULL value not allowed for column {}", self.name))),
            Some(ref datatype) if datatype != &self.datatype => Err(Error::Value(format!(
                "Invalid datatype {} for {} column {}",
                datatype, self.datatype, self.name
            ))),
            _ => Ok(()),
        }?;

        // Validate value
        match value {
            Value::String(s) if s.len() > 1024 => {
                Err(Error::Value("Strings cannot be more than 1024 bytes".into()))
            }
            _ => Ok(()),
        }?;

        // Validate outgoing references
        if let Some(target) = &self.references {
            match value {
                Value::Null => Ok(()),
                Value::Float(f) if f.is_nan() => Ok(()),
                v if target == &table.name && v == primary_key => Ok(()),
                v if txn.read(target, v)?.is_none() => Err(Error::Value(format!(
                    "Referenced primary key {} in table {} does not exist",
                    v, target,
                ))),
                _ => Ok(()),
            }?;
        }

        // Validate uniqueness constraints
        if self.is_unique && !self.is_primary_key && value != &Value::Null {
            let index = table.get_column_index(&self.name)?;
            let mut scan = txn.scan(&table.name, None)?;
            while let Some(row) = scan.next().transpose()? {
                if row.get(index).unwrap_or(&Value::Null) == value
                    && &table.get_row_key(&row)? != primary_key
                {
                    return Err(Error::Value(format!(
                        "Unique value {} already exists for column {}",
                        value, self.name
                    )));
                }
            }
        }

        Ok(())
    }
}

impl Display for Column {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut sql = format_ident(&self.name);
        sql += &format!(" {}", self.datatype);
        if self.is_primary_key {
            sql += " PRIMARY KEY";
        }
        if !self.is_nullable && !self.is_primary_key {
            sql += " NOT NULL";
        }
        if let Some(default) = &self.default {
            sql += &format!(" DEFAULT {}", default);
        }
        if self.is_unique && !self.is_primary_key {
            sql += " UNIQUE";
        }
        if let Some(reference) = &self.references {
            sql += &format!(" REFERENCES {}", reference);
        }
        if self.is_indexed {
            sql += " INDEX";
        }
        write!(f, "{}", sql)
    }
}