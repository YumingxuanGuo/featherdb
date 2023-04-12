use crate::error::{Result, Error};
use crate::sql::engine::SqlTxn;
use crate::sql::schema::Table;
use crate::sql::types::{Expression, ResColumn, Row, Value};
use super::{Executor, ResultSet};

/// A filter executor
pub struct FilterExec<T: SqlTxn> {
    source: Box<dyn Executor<T>>,
    predicate: Expression,
}

impl<T: SqlTxn> FilterExec<T> {
    pub fn new(source: Box<dyn Executor<T>>, predicate: Expression) -> Box<Self> {
        Box::new(Self { source, predicate })
    }
}

impl<T: SqlTxn> Executor<T> for FilterExec<T> {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        match self.source.execute(txn)? {
            ResultSet::Query { columns, rows, buffered_rows } => {
                let predicate = self.predicate;
                let predicate_buffered = predicate.clone();
                Ok(ResultSet::Query {
                    columns,
                    rows: Box::new(rows.filter_map(move |r| {
                        r.and_then(|row| match predicate.evaluate(Some(&row))? {
                            Value::Boolean(true) => Ok(Some(row)),
                            Value::Boolean(false) => Ok(None),
                            Value::Null => Ok(None),
                            value => Err(Error::Value(format!(
                                "Filter returned {}, expected boolean",
                                value
                            ))),
                        })
                        .transpose()
                    })),
                    buffered_rows: buffered_rows
                        .into_iter()
                        .filter_map(|row| match predicate_buffered.evaluate(Some(&row)) {
                            Ok(Value::Boolean(true)) => Ok(Some(row)),
                            Ok(Value::Boolean(false)) => Ok(None),
                            Ok(Value::Null) => Ok(None),
                            Ok(value) => Err(Error::Value(format!(
                                "Filter returned {}, expected boolean", value
                            ))),
                            Err(e) => Err(e),
                        }.transpose())
                        .collect::<Result<Vec<_>>>()?
                    })
            },
            _ => Err(Error::Internal("Unexpected result".into()))
        }
    }
}