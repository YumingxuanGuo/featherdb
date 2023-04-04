use std::collections::{HashMap, HashSet};

use crate::sql::types::{Value, Expression};
use crate::error::{Error, Result};
use crate::sql::schema::Table;
use crate::sql::schema::{Catalog, Column};
use crate::sql::parser::ast;

use super::{Plan, Node};



/// A query plan builder.
pub struct Planner<'a, C: Catalog> {
    catalog: &'a mut C,
}

impl<'a, C: Catalog> Planner<'a, C> {
    /// Creates a new planner.
    pub fn new(catalog: &'a mut C) -> Self {
        Self { catalog }
    }

    /// Builds a plan for a AST statement.
    pub fn build(&mut self, statement: ast::Statement) -> Result<Plan> {
        Ok(Plan { node: self.build_statement(statement)? })
    }

    /// Builds a plan node for a AST statement.
    fn build_statement(&self, statement: ast::Statement) -> Result<Node> {
        Ok(match statement {
            // Transaction control and explain statements should have been handled by session.
            ast::Statement::Begin { .. } | ast::Statement::Commit | ast::Statement::Rollback => {
                return Err(Error::Internal(format!(
                    "Unexpected transaction statement {:?}",
                    statement
                )))
            },

            // DDL statements (schema changes).
            ast::Statement::CreateTable { name, columns } => Node::CreateTable {
                schema: Table::new(
                    name,
                    columns
                        .into_iter()
                        .map(|c| {
                            let is_nullable = c.is_nullable.unwrap_or(!c.is_primary_key);
                            let default = match c.default {
                                Some(expr) => Some(self.evaluate_constant(expr)?),
                                None if is_nullable => Some(Value::Null),
                                None => None,
                            };
                            Ok(Column {
                                name: c.name,
                                datatype: c.datatype,
                                is_primary_key: c.is_primary_key,
                                is_nullable,
                                default,
                                is_unique: c.is_unique || c.is_primary_key,
                                is_indexed: c.is_indexed && !c.is_primary_key,
                                references: c.references,
                            })
                        })
                        .collect::<Result<_>>()?,
                )?,
            },
            ast::Statement::DropTable(table) => Node::DropTable { table },

            // DML statements (mutations).
            ast::Statement::Insert { table, columns, values } => Node::Insert {
                table,
                columns: columns.unwrap_or_else(Vec::new),
                expression: values
                    .into_iter()
                    .map(|exprs| {
                        exprs
                            .into_iter()
                            .map(|expr| self.build_expression(&mut Environment::constant(), expr))
                            .collect::<Result<_>>()
                    })
                    .collect::<Result<_>>()?,
            },

            _ => todo!()
        })
    }

    /// Builds an expression from an AST expression
    fn build_expression(&self, environment: &mut Environment, expr: ast::Expression) -> Result<Expression> {
        todo!()
    }

    /// Builds and evaluates a constant AST expression.
    fn evaluate_constant(&self, expr: ast::Expression) -> Result<Value> {
        self.build_expression(&mut Environment::constant(), expr)?.evaluate(None)
    }
}

/// Manages names available to expressions and executors, and maps them onto columns/fields.
#[derive(Clone, Debug)]
pub struct Environment {
    // If true, the environment is constant and cannot contain any variables.
    constant: bool,
    // Currently visible tables, by query name (i.e. alias or actual name).
    tables: HashMap<String, Table>,
    // Column labels, if any (qualified by table name when available)
    columns: Vec<(Option<String>, Option<String>)>,
    // Qualified names to column indexes.
    qualified: HashMap<(String, String), usize>,
    // Unqualified names to column indexes, if unique.
    unqualified: HashMap<String, usize>,
    // Unqialified ambiguous names.
    ambiguous: HashSet<String>,
}

impl Environment {
    /// Creates a new, empty environment.
    fn new() -> Self {
        Self {
            constant: false,
            tables: HashMap::new(),
            columns: Vec::new(),
            qualified: HashMap::new(),
            unqualified: HashMap::new(),
            ambiguous: HashSet::new(),
        }
    }

    /// Creates a constant environment.
    fn constant() -> Self {
        let mut environment = Self::new();
        environment.constant = true;
        environment
    }
}