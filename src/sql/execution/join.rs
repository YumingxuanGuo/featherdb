use crate::error::{Result, Error};
use crate::sql::engine::SqlTxn;
use crate::sql::schema::Table;
use crate::sql::types::{Expression, ResColumn, Row, Value, Rows};
use super::{Executor, ResultSet};

use std::collections::HashMap;

/// A nested loop join executor, which checks each row in the left source against every row in
/// the right source using the given predicate.
pub struct NestedLoopJoinExec<T: SqlTxn> {
    left: Box<dyn Executor<T>>,
    right: Box<dyn Executor<T>>,
    predicate: Option<Expression>,
    outer: bool,
}

impl<T: SqlTxn> NestedLoopJoinExec<T> {
    pub fn new(
        left: Box<dyn Executor<T>>,
        right: Box<dyn Executor<T>>,
        predicate: Option<Expression>,
        outer: bool,
    ) -> Box<Self> {
        Box::new(Self { left, right, predicate, outer })
    }
}

impl<T: SqlTxn> Executor<T> for NestedLoopJoinExec<T> {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        match (self.left.execute(txn)?, self.right.execute(txn)?) {
            (
                ResultSet::Query { mut columns, buffered_rows },
                ResultSet::Query {
                    columns: right_columns,
                    // rows: right_rows,
                    buffered_rows: right_buffered_rows,
                }
            ) => {
                let right_width = right_columns.len();
                columns.extend(right_columns.into_iter());
                // FIXME: Since making the iterators or sources clonable is non-trivial (requiring
                // either avoiding Rust standard iterators or making sources generic), we simply
                // fetch the entire right result as a vector.
                return Ok(ResultSet::Query {
                    columns,
                    // rows: Box::new(NestedLoopRows::new(
                    //     rows,
                    //     right_rows.collect::<Result<Vec<_>>>()?,
                    //     right_width,
                    //     self.predicate.clone(),
                    //     self.outer,
                    // )),
                    buffered_rows: NestedLoopRows::new(
                        Box::new(buffered_rows?.into_iter().map(|row| Ok(row))),
                        right_buffered_rows?,
                        right_width,
                        self.predicate,
                        self.outer,
                    ).collect::<Result<Vec<_>>>(),
                });
            },
            _ => Err(Error::Internal("Unexpected result set".into())),
        }
    }
}

struct NestedLoopRows {
    left: Rows,
    left_row: Option<Result<Row>>,
    right: Box<dyn Iterator<Item = Row> + Send>,
    right_vec: Vec<Row>,
    right_empty: Vec<Value>,
    right_hit: bool,
    predicate: Option<Expression>,
    outer: bool,
}

impl NestedLoopRows {
    fn new(
        mut left: Rows,
        right: Vec<Row>,
        right_width: usize,
        predicate: Option<Expression>,
        outer: bool,
    ) -> Self {
        Self {
            left_row: left.next(),
            left,
            right: Box::new(right.clone().into_iter()),
            right_vec: right,
            right_empty: std::iter::repeat(Value::Null).take(right_width).collect(),
            right_hit: false,
            predicate,
            outer,
        }
    }

    // Tries to get the next joined row, with error handling.
    fn try_next(&mut self) -> Result<Option<Row>> {
        // While there is a valid left row, look for a right-hand match to return.
        while let Some(Ok(left_row)) = self.left_row.clone() {
            // If there is a hit in the remaining right rows, return it.
            if let Some(row) = self.try_next_hit(&left_row)? {
                self.right_hit = true;
                return Ok(Some(row));
            }

            // Otherwise, continue with the next left row and reset the right source.
            self.left_row = self.left.next();
            self.right = Box::new(self.right_vec.clone().into_iter());

            // If this is an outer join, when we reach the end of the right items without a hit,
            // we should return a row with nulls for the right fields.
            if self.outer && !self.right_hit {
                let mut row = left_row;
                row.extend(self.right_empty.clone());
                return Ok(Some(row));
            }
            self.right_hit = false;
        }
        self.left_row.clone().transpose()
    }

    /// Tries to find the next combined row that matches the predicate in the remaining right rows.
    fn try_next_hit(&mut self, left_row: &[Value]) -> Result<Option<Row>> {
        for right_row in &mut self.right {
            let mut row = left_row.to_vec();
            row.extend(right_row);
            if let Some(predicate) = &self.predicate {
                match predicate.evaluate(Some(&row))? {
                    Value::Boolean(true) => return Ok(Some(row)),
                    Value::Boolean(false) => {}
                    Value::Null => {}
                    value => {
                        return Err(Error::Value(format!(
                            "Join predicate returned {}, expected boolean",
                            value
                        )))
                    }
                }
            } else {
                return Ok(Some(row));
            }
        }
        Ok(None)
    }
}

impl Iterator for NestedLoopRows {
    type Item = Result<Row>;

    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
    }
}