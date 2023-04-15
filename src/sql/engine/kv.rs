#![allow(dead_code)]
use std::borrow::Cow;
use std::collections::HashSet;

use serde::{Serialize, Deserialize};

use crate::concurrency::{MVCC, Transaction, Mode};
use crate::error::{Error, Result};
use crate::sql::schema::{Catalog, Table, Tables};
use crate::sql::types::{Row, Value, Expression};
use super::{SqlTxn, SqlEngine, RowScan, IndexScan};


/// A SQL engine based on an underlying MVCC key/value store
#[derive(Clone)]
pub struct KvSqlEngine {
    /// The underlying key/value store
    pub(super) kv: MVCC,
}

impl KvSqlEngine {
    /// Creates a new SQL engine.
    pub fn new(kv: MVCC) -> Self {
        Self { kv }
    }

    /// Fetches an unversioned metadata value.
    pub fn get_metadata(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.kv.get_metadata(key)
    }

    /// Sets an unversioned metadata value.
    pub fn set_metadata(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.kv.set_metadata(key, value)
    }
}

impl SqlEngine for KvSqlEngine {
    type EngineTxn = KvSqlTxn;

    fn begin(&self, mode: Mode) -> Result<Self::EngineTxn> {
        Ok(Self::EngineTxn::new(self.kv.begin_with_mode(mode)?))
    }

    fn resume(&self, id: u64) -> Result<Self::EngineTxn> {
        Ok(Self::EngineTxn::new(self.kv.resume(id)?))
    }
}

/// Serializes SQL metadata.
fn serialize<V: Serialize>(value: &V) -> Result<Vec<u8>> {
    Ok(bincode::serialize(value)?)
}

/// Deserializes SQL metadata.
fn deserialize<'a, V: Deserialize<'a>>(bytes: &'a [u8]) -> Result<V> {
    Ok(bincode::deserialize(bytes)?)
}

/// An SQL transaction based on an MVCC key/value transaction
pub struct KvSqlTxn {
    txn: Transaction,
}

impl KvSqlTxn {
    /// Creates a new SQL transaction from an MVCC transaction.
    pub fn new(txn: Transaction) -> Self {
        Self { txn }
    }

    /// Loads an index entry. TODO: ????
    fn load_index(&self, table: &str, column: &str, value: &Value) -> Result<HashSet<Value>> {
        Ok(self
            .txn
            .get(&SqlKey::Index(table.into(), column.into(), Some(value.into())).encode())?
            .map(|v| deserialize(&v))
            .transpose()?
            .unwrap_or_else(HashSet::new))
    }

    /// Saves an index entry.
    fn save_index(&self, table: &str, column: &str, value: &Value, index: HashSet<Value>) -> Result<()> {
        let key = SqlKey::Index(table.into(), column.into(), Some(value.into())).encode();
        match index.is_empty() {
            true => self.txn.delete(&key),
            false => self.txn.set(&key, serialize(&index)?),
        }
    }
}

impl SqlTxn for KvSqlTxn {
    fn id(&self) -> u64 {
        self.txn.id()
    }

    fn mode(&self) -> Mode {
        self.txn.mode()
    }

    fn commit(self) -> Result<()> {
        self.txn.commit()
    }

    fn rollback(self) -> Result<()> {
        self.txn.rollback()
    }

    fn create(&mut self, table: &str, row: Row) -> Result<()> {
        let table = self.assert_read_table(table)?;
        table.validate_row(&row, self)?;
        let id = table.get_row_key(&row)?;
        if self.read(&table.name, &id)?.is_some() {
            return Err(Error::Value(format!(
                "Primary key {} already exists for table {}",
                id, table.name
            )));
        }
        self.txn.set(
            &SqlKey::Row(Cow::Borrowed(&table.name), Some(Cow::Borrowed(&id))).encode(),
            serialize(&row)?,
        )?;
        
        // Update indexes
        for (i, column) in table.columns.iter().enumerate().filter(|(_, c)| c.is_indexed) {
            let mut indexes = self.load_index(&table.name, &column.name, &row[i])?;
            indexes.insert(id.clone());
            self.save_index(&table.name, &column.name, &row[i], indexes)?;
        }

        Ok(())
    }

    fn read(&self, table: &str, id: &Value) -> Result<Option<Row>> {
        self.txn
            .get(&SqlKey::Row(table.into(), Some(id.into())).encode())?
            .map(|v| deserialize(&v))
            .transpose()
    }

    fn update(&mut self, table: &str, id: &Value, row: Row) -> Result<()> {
        let table = self.assert_read_table(table)?;

        // If the primary key changes, we need to delete the old row and create a new one.
        // Otherwise, we can just update the existing row.
        if id != &table.get_row_key(&row)? {
            self.delete(&table.name, id)?;
            self.create(&table.name, row)?;
            return Ok(());
        }

        // Update indexes.
        let indexes: Vec<_> = table.columns.iter().enumerate().filter(|(_, c)| c.is_indexed).collect();
        if !indexes.is_empty() {
            let old_row = self.read(&table.name, id)?.ok_or_else(|| 
                Error::Value(format!("Row {} does not exist in table {}", id, &table.name))
            )?;
            for (i, column) in indexes {
                // TODO: why twice?
                if old_row[i] != row[i] {
                    let mut index = self.load_index(&table.name, &column.name, &old_row[i])?;
                    index.remove(id);
                    self.save_index(&table.name, &column.name, &old_row[i], index)?;
                    
                    let mut index = self.load_index(&table.name, &column.name, &row[i])?;
                    index.insert(id.clone());
                    self.save_index(&table.name, &column.name, &row[i], index)?;
                }
            }
        }

        table.validate_row(&row, self)?;
        self.txn.set(&SqlKey::Row(table.name.into(), Some(id.into())).encode(), serialize(&row)?)
    }

    fn delete(&mut self, table: &str, id: &Value) -> Result<()> {
        let table = self.assert_read_table(table)?;

        // Check if the value of the to-be-deleted primary key is being referenced.
        for (t, cs) in self.table_references(&table.name, true)? {
            let t = self.assert_read_table(&t)?;
            let cs = cs
                .into_iter()
                .map(|c| Ok((t.get_column_index(&c)?, c)))
                .collect::<Result<Vec<_>>>()?;
            let mut scan = self.scan(&t.name, None)?;
            while let Some(row) = scan.next().transpose()? {
                // There are two invalid deleting senarios:
                // 1. PK's value is being referenced in another table;
                // 2. PK's value is being referenced in the current table, and is not in the same row to be deleted.
                for (i, c) in &cs {
                    if row[*i] == *id && (t.name != table.name || id != &table.get_row_key(&row)?) {
                        return Err(Error::Value(format!(
                            "Cannot delete row {} from table {} because it is referenced by column {} in table {}",
                            id, table.name, c, t.name
                        )));
                    }
                }
            }
        }

        // Delete indexes.
        let indexes: Vec<_> = table.columns.iter().enumerate().filter(|(_, c)| c.is_indexed).collect();
        if !indexes.is_empty() {
            if let Some(row) = self.read(&table.name, id)? {
                for (i, column) in indexes {
                    let mut index = self.load_index(&table.name, &column.name, &row[i])?;
                    index.remove(id);
                    self.save_index(&table.name, &column.name, &row[i], index)?;
                }
            }
        }

        self.txn.delete(&SqlKey::Row(table.name.into(), Some(id.into())).encode())
    }

    fn read_index(&self, table: &str, column: &str, value: &Value) -> Result<HashSet<Value>> {
        if !self.assert_read_table(table)?.get_column(column)?.is_indexed {
            return Err(Error::Value(format!("Column {} in table {} is not indexed",column, table)));
        }
        self.load_index(table, column, value)
    }

    fn scan(&self, table: &str, filter: Option<Expression>) -> Result<RowScan> {
        let table = self.assert_read_table(table)?;
        Ok(Box::new(
            self.txn
                .scan_prefix(&SqlKey::Row((&table.name).into(), None).encode())?
                .map(|r| r.and_then(|(_, v)| deserialize(&v)))
                .filter_map(move |r| match r {
                    Ok(row) => match &filter {
                        Some(filter) => match filter.evaluate(Some(&row)) {
                            Ok(Value::Boolean(true)) => Some(Ok(row)),
                            Ok(Value::Boolean(false)) | Ok(Value::Null) => None,
                            Ok(v) => Some(Err(Error::Value(format!(
                                "Filter returned {}, expected boolean", v
                            )))),
                            Err(err) => Some(Err(err)),
                        },
                        None => Some(Ok(row)),
                    }
                    err => Some(err),
                }),
        ))
    }

    fn scan_index(&self, table: &str, column: &str) -> Result<IndexScan> {
        let table = self.assert_read_table(table)?;
        let column = table.get_column(column)?;
        if !column.is_indexed {
            return Err(Error::Value(format!(
                "Column {} in table {} is not indexed", column.name, table.name
            )));
        }
        Ok(Box::new(
            self.txn
                .scan_prefix(
                    &SqlKey::Index((&table.name).into(), (&column.name).into(), None).encode()
                )?
                .map(|r| {
                    let (k, v) = r?;
                    let value = match SqlKey::decode(&k)? {
                        SqlKey::Index(_, _, Some(value)) => value.into_owned(),
                        _ => return Err(Error::Internal("Invalid index key".into())),
                    };
                    Ok((value, deserialize(&v)?))
                })
        ))
    }
}

impl Catalog for KvSqlTxn {
    fn create_table(&mut self, table: Table) -> Result<()> {
        if self.read_table(&table.name)?.is_some() {
            return Err(Error::Value(format!("Table {} already exists", table.name)));
        }
        table.validate(self)?;
        self.txn.set(&SqlKey::Table(Some((&table.name).into())).encode(), serialize(&table)?)
    }

    fn read_table(&self, table: &str) -> Result<Option<Table>> {
        self.txn.get(&SqlKey::Table(Some(table.into())).encode())?.map(|v| deserialize(&v)).transpose()
    }

    fn delete_table(&mut self, table: &str) -> Result<()> {
        let table = self.assert_read_table(table)?;
        if let Some((t, cs)) = self.table_references(&table.name, false)?.first() {
            return Err(Error::Value(format!(
                "Cannot delete table {} because it is referenced by table {} column {}",
                table.name, t, cs[0]
            )));
        }
        let mut scan = self.scan(&table.name, None)?;
        while let Some(row) = scan.next().transpose()? {
            self.delete(&table.name, &table.get_row_key(&row)?)?;
        }
        self.txn.delete(&SqlKey::Table(Some(table.name.into())).encode())
    }

    fn scan_tables(&self) -> Result<Tables> {
        Ok(Box::new(
            self.txn
                .scan_prefix(&SqlKey::Table(None).encode())?
                .map(|r| r.and_then(|(_, v)| deserialize(&v)))
                .collect::<Result<Vec<_>>>()?
                .into_iter()
        ))
    }
}

/// Encodes SQL keys, using an order-preserving encoding - see kv::encoding for details. Options can
/// be None to get a keyspace prefix. We use table and column names directly as identifiers, to
/// avoid additional indirection and associated overhead. It is not possible to change names, so
/// this is ok. Uses Cows since we want to borrow when encoding but return owned when decoding.
enum SqlKey<'a> {
    /// A table schema key for the given table name
    Table(Option<Cow<'a, str>>),
    /// A key for an index entry
    Index(Cow<'a, str>, Cow<'a, str>, Option<Cow<'a, Value>>),
    /// A key for a row identified by table name and row primary key
    Row(Cow<'a, str>, Option<Cow<'a, Value>>),
}

impl<'a> SqlKey<'a> {
    /// Encodes the key as a byte vector
    fn encode(self) -> Vec<u8> {
        use crate::encoding::*;
        match self {
            Self::Table(None) => vec![0x01],
            Self::Table(Some(name)) => [&[0x01][..], &encode_string(&name)].concat(),
            Self::Index(table, column, None) => {
                [&[0x02][..], &encode_string(&table), &encode_string(&column)].concat()
            }
            Self::Index(table, column, Some(value)) => [
                &[0x02][..],
                &encode_string(&table),
                &encode_string(&column),
                &encode_value(&value),
            ]
            .concat(),
            Self::Row(table, None) => [&[0x03][..], &encode_string(&table)].concat(),
            Self::Row(table, Some(pk)) => {
                [&[0x03][..], &encode_string(&table), &encode_value(&pk)].concat()
            }
        }
    }

    /// Decodes a key from a byte vector
    fn decode(mut bytes: &[u8]) -> Result<Self> {
        use crate::encoding::*;
        let bytes = &mut bytes;
        let key = match take_byte(bytes)? {
            0x01 => Self::Table(Some(take_string(bytes)?.into())),
            0x02 => Self::Index(
                take_string(bytes)?.into(),
                take_string(bytes)?.into(),
                Some(take_value(bytes)?.into()),
            ),
            0x03 => Self::Row(take_string(bytes)?.into(), Some(take_value(bytes)?.into())),
            b => return Err(Error::Internal(format!("Unknown SQL key prefix {:x?}", b))),
        };
        if !bytes.is_empty() {
            return Err(Error::Internal("Unexpected data remaining at end of key".into()));
        }
        Ok(key)
    }
}
