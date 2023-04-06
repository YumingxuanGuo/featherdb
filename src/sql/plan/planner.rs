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
                            .map(|expr|
                                self.build_expression(&mut Environment::constant(), expr)
                            )
                            .collect::<Result<_>>()
                    })
                    .collect::<Result<_>>()?,
            },
            ast::Statement::Update { table, set, r#where } => {
                let environment = &mut Environment::from_table(
                    self.catalog.assert_read_table(&table)?
                )?;
                Node::Update {
                    table: table.clone(),
                    source: Box::new(Node::Scan {
                        table,
                        alias: None,
                        filter: r#where
                            .map(|expr| self.build_expression(environment, expr))
                            .transpose()?,
                    }),
                    expressions: set
                        .into_iter()
                        .map(|(column, expr)| {
                            Ok((
                                environment.resolve(None, &column)?,
                                Some(column),
                                self.build_expression(environment, expr)?,
                            ))
                        })
                        .collect::<Result<_>>()?,
                }
            },
            ast::Statement::Delete { table, r#where } => {
                let environment = &mut Environment::from_table(
                    self.catalog.assert_read_table(&table)?
                )?;
                Node::Delete {
                    table: table.clone(),
                    source: Box::new(Node::Scan {
                        table,
                        alias: None,
                        filter: r#where
                            .map(|expr| self.build_expression(environment, expr))
                            .transpose()?,
                    }),
                }
            }
        })
    }

    /// Builds an expression from an AST expression. TODO: Read.
    fn build_expression(&self, environment: &mut Environment, expr: ast::Expression) -> Result<Expression> {
        use Expression::*;
        Ok(match expr {
            ast::Expression::Literal(l) => Constant(match l {
                ast::Literal::Null => Value::Null,
                ast::Literal::Boolean(b) => Value::Boolean(b),
                ast::Literal::Integer(i) => Value::Integer(i),
                ast::Literal::Float(f) => Value::Float(f),
                ast::Literal::String(s) => Value::String(s),
            }),
            ast::Expression::Column(i) => Field(i, environment.get_label(i)?),
            ast::Expression::Field(table, name) => {
                Field(environment.resolve(table.as_deref(), &name)?, Some((table, name)))
            }
            ast::Expression::Function(name, _) => {
                return Err(Error::Value(format!("Unknown function {}", name,)))
            }
            ast::Expression::Operation(op) => match op {
                // Logical operators
                ast::Operation::And(lhs, rhs) => And(
                    self.build_expression(environment, *lhs)?.into(),
                    self.build_expression(environment, *rhs)?.into(),
                ),
                ast::Operation::Not(expr) => Not(self.build_expression(environment, *expr)?.into()),
                ast::Operation::Or(lhs, rhs) => Or(
                    self.build_expression(environment, *lhs)?.into(),
                    self.build_expression(environment, *rhs)?.into(),
                ),

                // Comparison operators
                ast::Operation::Equal(lhs, rhs) => Equal(
                    self.build_expression(environment, *lhs)?.into(),
                    self.build_expression(environment, *rhs)?.into(),
                ),
                ast::Operation::GreaterThan(lhs, rhs) => GreaterThan(
                    self.build_expression(environment, *lhs)?.into(),
                    self.build_expression(environment, *rhs)?.into(),
                ),
                ast::Operation::GreaterThanOrEqual(lhs, rhs) => Or(
                    GreaterThan(
                        self.build_expression(environment, *lhs.clone())?.into(),
                        self.build_expression(environment, *rhs.clone())?.into(),
                    )
                    .into(),
                    Equal(
                        self.build_expression(environment, *lhs)?.into(),
                        self.build_expression(environment, *rhs)?.into(),
                    )
                    .into(),
                ),
                ast::Operation::IsNull(expr) => IsNull(self.build_expression(environment, *expr)?.into()),
                ast::Operation::LessThan(lhs, rhs) => LessThan(
                    self.build_expression(environment, *lhs)?.into(),
                    self.build_expression(environment, *rhs)?.into(),
                ),
                ast::Operation::LessThanOrEqual(lhs, rhs) => Or(
                    LessThan(
                        self.build_expression(environment, *lhs.clone())?.into(),
                        self.build_expression(environment, *rhs.clone())?.into(),
                    )
                    .into(),
                    Equal(
                        self.build_expression(environment, *lhs)?.into(),
                        self.build_expression(environment, *rhs)?.into(),
                    )
                    .into(),
                ),
                ast::Operation::Like(lhs, rhs) => Like(
                    self.build_expression(environment, *lhs)?.into(),
                    self.build_expression(environment, *rhs)?.into(),
                ),
                ast::Operation::NotEqual(lhs, rhs) => Not(Equal(
                    self.build_expression(environment, *lhs)?.into(),
                    self.build_expression(environment, *rhs)?.into(),
                )
                .into()),

                // Mathematical operators
                ast::Operation::Assert(expr) => Assert(self.build_expression(environment, *expr)?.into()),
                ast::Operation::Add(lhs, rhs) => Add(
                    self.build_expression(environment, *lhs)?.into(),
                    self.build_expression(environment, *rhs)?.into(),
                ),
                ast::Operation::Divide(lhs, rhs) => Divide(
                    self.build_expression(environment, *lhs)?.into(),
                    self.build_expression(environment, *rhs)?.into(),
                ),
                ast::Operation::Exponentiate(lhs, rhs) => Exponentiate(
                    self.build_expression(environment, *lhs)?.into(),
                    self.build_expression(environment, *rhs)?.into(),
                ),
                ast::Operation::Factorial(expr) => {
                    Factorial(self.build_expression(environment, *expr)?.into())
                }
                ast::Operation::Modulo(lhs, rhs) => Modulo(
                    self.build_expression(environment, *lhs)?.into(),
                    self.build_expression(environment, *rhs)?.into(),
                ),
                ast::Operation::Multiply(lhs, rhs) => Multiply(
                    self.build_expression(environment, *lhs)?.into(),
                    self.build_expression(environment, *rhs)?.into(),
                ),
                ast::Operation::Negate(expr) => Negate(self.build_expression(environment, *expr)?.into()),
                ast::Operation::Subtract(lhs, rhs) => Subtract(
                    self.build_expression(environment, *lhs)?.into(),
                    self.build_expression(environment, *rhs)?.into(),
                ),
            },
        })
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
    is_constant: bool,
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
            is_constant: false,
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
        environment.is_constant = true;
        environment
    }

    /// Creates an environment from a table.
    fn from_table(table: Table) -> Result<Self> {
        let mut environment = Self::new();
        environment.add_table(table.name.clone(), table)?;
        Ok(environment)
    }

    /// Adds a column to the environment. TODO: Read.
    #[allow(clippy::map_entry)]
    fn add_column(&mut self, table: Option<String>, label: Option<String>) {
        if let Some(l) = label.clone() {
            if let Some(t) = table.clone() {
                self.qualified.insert((t, l.clone()), self.columns.len());
            }
            if !self.ambiguous.contains(&l) {
                if !self.unqualified.contains_key(&l) {
                    self.unqualified.insert(l, self.columns.len());
                } else {
                    self.unqualified.remove(&l);
                    self.ambiguous.insert(l);
                }
            }
        }
        self.columns.push((table, label));
    }

    /// Adds a table to the environment. TODO: Read.
    fn add_table(&mut self, label: String, table: Table) -> Result<()> {
        if self.is_constant {
            return Err(Error::Internal("Can't modify constant environment".into()));
        }
        if self.tables.contains_key(&label) {
            return Err(Error::Value(format!("Duplicate table name {}", label)));
        }
        for column in &table.columns {
            self.add_column(Some(label.clone()), Some(column.name.clone()));
        }
        self.tables.insert(label, table);
        Ok(())
    }

    /// Resolves a name, optionally qualified by a table name.
    fn resolve(&self, table: Option<&str>, name: &str) -> Result<usize> {
        todo!()
    }

    /// Fetches a column from the scope by index. TODO: Read.
    fn get_column(&self, index: usize) -> Result<(Option<String>, Option<String>)> {
        if self.is_constant {
            return Err(Error::Value(format!(
                "Expression must be constant, found column {}",
                index
            )));
        }
        self.columns
            .get(index)
            .cloned()
            .ok_or_else(|| Error::Value(format!("Column index {} not found", index)))
    }

    /// Fetches a column label by index, if any. TODO: Read.
    fn get_label(&self, index: usize) -> Result<Option<(Option<String>, String)>> {
        Ok(match self.get_column(index)? {
            (table, Some(name)) => Some((table, name)),
            _ => None,
        })
    }
}