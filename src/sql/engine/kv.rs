#![allow(dead_code)]
use std::borrow::Cow;
use std::collections::HashSet;

use serde::{Serialize, Deserialize};

use crate::concurrency::{MVCC, Transaction, Mode};
use crate::error::{Error, Result};
use crate::sql::schema::{Catalog, Table};
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
            let mut index = self.load_index(&table.name, &column.name, &row[i])?;
            index.insert(id.clone());
            self.save_index(&table.name, &column.name, &row[i], index)?;
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
        todo!()
    }

    fn read_index(&self, table: &str, column: &str, value: &Value) -> Result<HashSet<Value>> {
        todo!()
    }

    fn scan(&self, table: &str, filter: Option<Expression>) -> Result<RowScan> {
        todo!()
    }

    fn scan_index(&self, table: &str, column: &str) -> Result<IndexScan> {
        todo!()
    }
}

impl Catalog for KvSqlTxn {
    fn create_table(&mut self, table: Table) -> Result<()> {
        todo!()
    }

    fn read_table(&self, table: &str) -> Result<Option<Table>> {
        todo!()
    }

    fn assert_read_table(&self, table: &str) -> Result<Table> {
        todo!()
    }

    fn delete_table(&mut self, table: &str) -> Result<()> {
        todo!()
    }

    fn scan_tables(&self) -> Result<crate::sql::schema::Tables> {
        todo!()
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
