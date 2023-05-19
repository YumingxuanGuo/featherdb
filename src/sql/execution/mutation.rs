use std::collections::{HashSet, HashMap};

use crate::error::{Result, Error};
use crate::sql::engine::SqlTxn;
use crate::sql::schema::Table;
use crate::sql::types::{Expression, Value, Row};
use super::{Executor, ResultSet};

/// An INSERT executor
pub struct InsertExec {
    table: String,
    columns: Vec<String>,
    rows: Vec<Vec<Expression>>,
}

impl InsertExec {
    /// Creates a new INSERT executor.
    pub fn new(table: String, columns: Vec<String>, rows: Vec<Vec<Expression>>) -> Box<Self> {
        Box::new(Self { table, columns, rows })
    }

    /// Builds a row from a set of column names and values, padding it with default values.
    fn build_row(table: &Table, values: Vec<Value>, columns: &[String]) -> Result<Row> {
        if columns.len() != values.len() {
            return Err(Error::Value("Column and value counts do not match".into()));
        }
        let mut inputs = HashMap::new();
        for (c, v) in columns.iter().zip(values.iter()) {
            let _ = table.get_column(c)?;
            inputs.insert(c.clone(), v.clone()).map(|_| {
                Err::<Value, Error>(Error::Value(format!("Column {} given multiple times", c)))
            }).transpose()?;
        }
        let mut row = Row::new();
        for column in table.columns.iter() {
            if let Some(value) = inputs.get(&column.name) {
                row.push(value.clone());
            } else if let Some(default) = &column.default {
                row.push(default.clone());
            } else {
                return Err(Error::Value(format!(
                    "Column {} not given and has no default value", column.name
                )));
            }
        }
        Ok(row)
    }

    /// Pads a row with default values where possible.
    fn pad_row(table: &Table, mut row: Vec<Value>) -> Result<Row> {
        for column in table.columns.iter().skip(row.len()) {
            if let Some(default) = &column.default {
                row.push(default.clone());
            } else {
                return Err(Error::Value(format!(
                    "Column {} not given and has no default value", column.name
                )));
            }
        }
        Ok(row)
    }
}

impl<T: SqlTxn> Executor<T> for InsertExec {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        let table = txn.assert_read_table(&self.table)?;
        let mut count = 0;
        for expressions in self.rows {
            let mut row = expressions
                .into_iter()
                .map(|expr| expr.evaluate(None))
                .collect::<Result<_>>()?;
            match self.columns.is_empty() {
                true => row = Self::pad_row(&table, row)?,
                false => row = Self::build_row(&table, row, &self.columns)?,
            };
            txn.create(&table.name, row)?;
            count += 1;
        }
        Ok(ResultSet::Create { count })
    }
}

/// An UPDATE executor
pub struct UpdateExec<T: SqlTxn> {
    table: String,
    source: Box<dyn Executor<T>>,
    expressions: Vec<(usize, Expression)>,
}

impl<T: SqlTxn> UpdateExec<T> {
    pub fn new(
        table: String,
        source: Box<dyn Executor<T>>,
        expressions: Vec<(usize, Expression)>,
    ) -> Box<Self> {
        Box::new(Self { table, source, expressions })
    }
}

impl<T: SqlTxn> Executor<T> for UpdateExec<T> {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        match self.source.execute(txn)? {
            ResultSet::Query { buffered_rows, .. } => {
                let table = txn.assert_read_table(&self.table)?;

                // The iterator will see our changes, such that the same item may be iterated over
                // multiple times. We keep track of the primary keys here to avoid that, althought
                // it may cause ballooning memory usage for large updates.
                //
                // FIXME This is not safe for primary key updates, which may still be processed
                // multiple times - it should be possible to come up with a pathological case that
                // loops forever (e.g. UPDATE test SET id = id + 1).
                let mut updated = HashSet::new();
                let mut rows = buffered_rows?.into_iter();
                while let Some(row) = rows.next() {
                    let id = table.get_row_key(&row)?;
                    if updated.contains(&id) {
                        continue;
                    }
                    let mut new = row.clone();
                    for (field, expr) in &self.expressions {
                        new[*field] = expr.evaluate(Some(&row))?;
                    }
                    txn.update(&table.name, &id, new)?;
                    updated.insert(id);
                }
                Ok(ResultSet::Update { count: updated.len() as u64 })
            },
            
            r => Err(Error::Internal(format!("Unexpected response {:?}", r))),
        }
    }
}

/// A DELETE executor
pub struct DeleteExec<T: SqlTxn> {
    table: String,
    source: Box<dyn Executor<T>>,
}

impl<T: SqlTxn> DeleteExec<T> {
    pub fn new(table: String, source: Box<dyn Executor<T>>) -> Box<Self> {
        Box::new(Self { table, source })
    }
}

impl<T: SqlTxn> Executor<T> for DeleteExec<T> {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        let table = txn.assert_read_table(&self.table)?;
        let mut count = 0;
        match self.source.execute(txn)? {
            ResultSet::Query { buffered_rows, .. } => {
                let mut rows = buffered_rows?.into_iter();
                while let Some(row) = rows.next() {
                    txn.delete(&table.name, &table.get_row_key(&row)?)?;
                    count += 1;
                }
                Ok(ResultSet::Delete { count })
            },
            r => Err(Error::Internal(format!("Unexpected result {:?}", r))),
        }
    }
}



#[cfg(test)]
mod tests {
    use crate::storage::kv::LsmStorage;

    use super::super::tests::*;
    use super::*;

    #[test]
    fn test_insert_executor_build_row() -> Result<()> {
        let table = Table::new(
            "test".to_string(),
            vec![
                set_up_column("a".to_string(), None, true),
                set_up_column("b".to_string(), Some(Value::Integer(0)), false),
                set_up_column("c".to_string(), Some(Value::Integer(0)), false),
            ],
        )?;

        let row = InsertExec::build_row(
            &table,
            vec![Value::Integer(1), Value::Integer(2)],
            &vec!["a".into(), "c".into()],
        )?;
        assert_eq!(row, vec![Value::Integer(1), Value::Integer(0), Value::Integer(2)]);

        let result = InsertExec::build_row(
            &table,
            vec![Value::Integer(1), Value::Integer(2)],
            &vec!["a".into(), "a".into()],
        );
        assert_eq!(result, Err(Error::Value(format!("Column {} given multiple times", "a"))));

        let result = InsertExec::build_row(
            &table,
            vec![Value::Integer(1), Value::Integer(2)],
            &vec!["b".into(), "c".into()],
        );
        assert_eq!(result, Err(Error::Value(format!("Column {} not given and has no default value", "a"))));
        
        let result = InsertExec::build_row(
            &table,
            vec![Value::Integer(1)],
            &vec!["a".into(), "c".into()],
        );
        assert_eq!(result, Err(Error::Value("Column and value counts do not match".into())));

        Ok(())
    }

    #[test]
    fn test_insert_executor_pad_row() -> Result<()> {
        let table = Table::new(
            "test".to_string(),
            vec![
                set_up_column("a".to_string(), Some(Value::Integer(0)), true),
                set_up_column("b".to_string(), None, false),
                set_up_column("c".to_string(), Some(Value::Integer(0)), false),
            ],
        )?;

        let row = InsertExec::pad_row(
            &table,
            vec![Value::Integer(1), Value::Integer(2)],
        )?;
        assert_eq!(row, vec![Value::Integer(1), Value::Integer(2), Value::Integer(0)]);
        
        let result = InsertExec::pad_row(
            &table,
            vec![Value::Integer(1)],
        );
        assert_eq!(result, Err(Error::Value("Column b not given and has no default value".into())));

        Ok(())
    }
}