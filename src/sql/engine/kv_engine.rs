use std::{borrow::Cow, collections::HashSet};

use crate::{storage::kv, sql::types::Value};
use super::{Engine, SqlTxn, Catalog, Row};
use crate::error::{Error, Result};

use serde::{Deserialize, Serialize};

/// A SQL engine based on an underlying MVCC key/value store
pub struct KvEngine {
    /// The underlying MVCC key/value store
    pub(super) kv: kv::MVCC,
}

impl Clone for KvEngine {
    fn clone(&self) -> Self {
        KvEngine::new(self.kv.clone())
    }
}

impl KvEngine {
    /// Creates a new key/value-based SQL engine.
    pub fn new(kv: kv::MVCC) -> Self {
        Self { kv }
    }
}

impl Engine for KvEngine {
    type Txn = KvTxn;

    fn begin(&self, mode: super::Mode) -> Result<Self::Txn> {
        Ok(Self::Txn::new(self.kv.begin_with_mode(mode)?))
    }

    fn resume(&self, id: u64) -> Result<Self::Txn> {
        Ok(Self::Txn::new(self.kv.resume(id)?))
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
pub struct KvTxn {
    txn: kv::Transaction,
}

impl KvTxn {
    /// Creates a new SQL transaction from an MVCC transaction
    fn new(txn: kv::Transaction) -> Self {
        Self { txn }
    }

    /// Loads an index entry
    fn index_load(&self, table_name: &str, column_name: &str, value: &Value) -> Result<HashSet<Value>> {
        Ok(self
            .txn
            .get(&SqlKey::Index(table_name.into(), column_name.into(), Some(value.into())).encode())?
            .map(|v| deserialize(&v))
            .transpose()?
            .unwrap_or_else(HashSet::new))
    }

    /// Saves an index entry.
    fn index_save(
        &mut self,
        table: &str,
        column: &str,
        value: &Value,
        index: HashSet<Value>,
    ) -> Result<()> {
        let key = SqlKey::Index(table.into(), column.into(), Some(value.into())).encode();
        if index.is_empty() {
            self.txn.delete(&key)
        } else {
            self.txn.set_or_insert(&key, serialize(&index)?)
        }
    }
}

impl SqlTxn for KvTxn {
    fn get_id(&self) -> u64 {
        self.txn.get_id()
    }

    fn get_mode(&self) -> super::Mode {
        self.txn.get_mode()
    }

    fn commit(self) -> Result<()> {
        self.txn.commit()
    }

    fn rollback(self) -> Result<()> {
        self.txn.rollback()
    }

    fn create(&mut self, table_name: &str, row: Row) -> Result<()> {
        let table = self.read_table_or_error(table_name)?;
        table.validate_row(&row, self)?;
        let primary_key = table.get_row_primary_key(&row)?;
        if self.read(table_name, &primary_key)?.is_some() {
            return Err(Error::Value(format!(
                "Primary key {} already exists for table {}",
                primary_key, table.name
            )));
        }
        self.txn.set_or_insert(
            &SqlKey::Row(Cow::Borrowed(&table_name), Some(Cow::Borrowed(&primary_key))).encode(),
            serialize(&row)?,
        )?;

        // Update indexes.
        for (i, column) in table.columns.iter().enumerate().filter(|(_, c)| c.is_indexed) {
            let mut index = self.index_load(table_name, &column.name, &row[i])?;
            index.insert(primary_key.clone());
            self.index_save(table_name, &column.name, &row[i], index)?;
        }
        Ok(())
    }

    fn read(&self, table_name: &str, primary_key: &Value) -> Result<Option<Row>> {
        self.txn
            .get(&SqlKey::Row(table_name.into(), Some(primary_key.into())).encode())?
            .map(|v| deserialize(&v))
            .transpose()
    }

    fn update(&mut self, table_name: &str, primary_key: &Value, row: Row) -> Result<()> {
        let table = self.read_table_or_error(table_name)?;
        // If the primary key changes we do a delete and create, otherwise we replace the row.
        if primary_key != &table.get_row_primary_key(&row)? {
            self.delete()?;
            self.create(table_name, row)?;
        }

        // Update indexes, knowing that the primary key has not changed
        let indexes: Vec<_> = table.columns.iter().enumerate().filter(|(_, c)| c.is_indexed).collect();
        if !indexes.is_empty() {
            let old_row = 
                self.read(table_name, primary_key)?.ok_or_else(|| Error::Value(format!(
                    "Primary key {} does not exist for table {}",
                    primary_key, table.name
            )))?;
            for (i, column) in indexes {
                if old_row[i] == row[i] {
                    continue;
                }
                let mut index = self.index_load(table_name, &column.name, &old_row[i])?;
                index.remove(primary_key);
                self.index_save(table_name, &column.name, &old_row[i], index)?;

                let mut index = self.index_load(table_name, &column.name, &row[i])?;
                index.insert(primary_key.clone());
                self.index_save(table_name, &column.name, &row[i], index)?;
            }
        }

        table.validate_row(&row, self)?;
        self.txn.set_or_insert(
            &SqlKey::Row(table_name.into(), Some(primary_key.into())).encode(), 
            serialize(&row)?,
        )
    }

    fn delete(&mut self) -> Result<()> {
        Ok(())
    }
}

impl Catalog for KvTxn {

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
        vec![0x01]
    }

    /// Decodes a key from a byte vector
    fn decode(mut bytes: &[u8]) -> Result<Self> {
        Err(Error::Internal(format!("Unknown SQL key prefix")))
    }
}
