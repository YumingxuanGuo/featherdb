use std::collections::{HashMap, HashSet};

use crate::sql::types::{Value, Expression};
use crate::error::{Error, Result};
use crate::sql::schema::Table;
use crate::sql::schema::{Catalog, Column};
use crate::sql::parser::ast;

use super::{Plan, Node, Aggregate};

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
        Ok(Plan(self.build_statement(statement)?))
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
                            .map(|expr| {
                                self.build_expression(&mut Environment::constant(), expr)
                            })
                            .collect::<Result<_>>()
                    })
                    .collect::<Result<_>>()?,
            },
            // TODO: Read.
            ast::Statement::Select {
                mut select,
                from,
                r#where,
                group_by,
                mut having,
                mut order,
                offset,
                limit,
            } => {
                let environment = &mut Environment::new();

                // Build the FROM clause.
                let mut node = match (from.is_empty(), select.is_empty()) {
                    (false, _) => self.build_from_clause(environment, from)?,
                    (true, false) => Node::Nothing,
                    (true, true) => return Err(Error::Value("Can't select * without a table".into())),
                };

                // Build the WHERE clause.
                if let Some(expr) = r#where {
                    node = Node::Filter {
                        source: Box::new(node),
                        predicate: self.build_expression(environment, expr)?,
                    };
                }

                // Build the SELECT clause.
                let mut hidden = 0;
                if !select.is_empty() {
                    // Inject hidden SELECT columns for fields and aggregates used in ORDER BY and
                    // HAVING expressions but not present in existing SELECT output. These will be
                    // removed again by a later projection.
                    
                    // if let Some(ref mut expr) = having {
                    //     hidden += self.inject_hidden(expr, &mut select)?;
                    // }
                    // for (expr, _) in order.iter_mut() {
                    //     hidden += self.inject_hidden(expr, &mut select)?;
                    // }

                    // Extract any aggregate functions and GROUP BY expressions, replacing them with
                    // Column placeholders. Aggregations are handled by evaluating group expressions
                    // and aggregate function arguments in a pre-projection, passing the results
                    // to an aggregation node, and then evaluating the final SELECT expressions
                    // in the post-projection. For example:
                    //
                    // SELECT (MAX(rating * 100) - MIN(rating * 100)) / 100
                    // FROM movies
                    // GROUP BY released - 2000
                    //
                    // Results in the following nodes:
                    //
                    // - Projection: rating * 100, rating * 100, released - 2000
                    // - Aggregation: max(#0), min(#1) group by #2
                    // - Projection: (#0 - #1) / 100

                    // let aggregates = self.extract_aggregates(&mut select)?;
                    // let groups = self.extract_groups(&mut select, group_by, aggregates.len())?;
                    // if !aggregates.is_empty() || !groups.is_empty() {
                    //     node = self.build_aggregation(environment, node, groups, aggregates)?;
                    // }

                    // Build the remaining non-aggregate projection.

                    // let expressions: Vec<(Expression, Option<String>)> = select
                    //     .into_iter()
                    //     .map(|(e, l)| Ok((self.build_expression(environment, e)?, l)))
                    //     .collect::<Result<_>>()?;
                    // environment.project(&expressions)?;
                    // node = Node::Projection { source: Box::new(node), expressions };
                };

                // TODO: Build HAVING clause.

                // TODO: Build ORDER clause.

                // TODO: Build OFFSET clause.

                // TODO: Build LIMIT clause.

                // TODO: Remove any hidden columns.

                node
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

    /// Builds a FROM clause consisting of several items. Each item is either a single table or a
    /// join of an arbitrary number of tables. All of the items are joined, since e.g. 'SELECT * FROM
    /// a, b' is an implicit join of a and b.
    fn build_from_clause(&self, environment: &mut Environment, from: Vec<ast::FromItem>) -> Result<Node> {
        let base_env = environment.clone();
        let mut items = from.into_iter();
        let mut node = match items.next() {
            Some(item) => self.build_from_item(environment, item)?,
            None => return Err(Error::Value("No from items given".into())),
        };
        for item in items {
            let mut right_env = base_env.clone();
            let right = self.build_from_item(&mut right_env, item)?;
            node = Node::NestedLoopJoin {
                left: Box::new(node),
                left_size: environment.len(),
                right: Box::new(right),
                predicate: None,
                outer: false,
            };
            environment.merge(right_env)?;
        }
        Ok(node)
    }

    /// Builds FROM items, which can either be a single table or a chained join of multiple tables,
    /// e.g. 'SELECT * FROM a LEFT JOIN b ON b.a_id = a.id'. Any tables will be stored in
    /// self.tables keyed by their query name (i.e. alias if given, otherwise name). The table can
    /// only be referenced by the query name (so if alias is given, cannot reference by name).
    fn build_from_item(&self, environment: &mut Environment, item: ast::FromItem) -> Result<Node> {
        Ok(match item {
            ast::FromItem::Table { name, alias } => {
                environment.add_table(
                    alias.clone().unwrap_or_else(|| name.clone()),
                    self.catalog.assert_read_table(&name)?,
                )?;
                Node::Scan { table: name, alias, filter: None }
            }

            ast::FromItem::Join { left, right, r#type, predicate } => {
                // Right outer joins are built as a left outer join with an additional projection
                // to swap the resulting columns.
                let (left, right) = match r#type {
                    ast::JoinType::Right => (right, left),
                    _ => (left, right),
                };
                let left = Box::new(self.build_from_item(environment, *left)?);
                let left_size = environment.len();
                let right = Box::new(self.build_from_item(environment, *right)?);
                let predicate = predicate.map(|e| self.build_expression(environment, e)).transpose()?;
                let outer = match r#type {
                    ast::JoinType::Cross | ast::JoinType::Inner => false,
                    ast::JoinType::Left | ast::JoinType::Right => true,
                };
                let mut node = Node::NestedLoopJoin { left, left_size, right, predicate, outer };
                if matches!(r#type, ast::JoinType::Right) {
                    let expressions = (left_size..environment.len())
                        .chain(0..left_size)
                        .map(|i| Ok((Expression::Field(i, environment.get_label(i)?), None)))
                        .collect::<Result<Vec<_>>>()?;
                    environment.project(&expressions)?;
                    node = Node::Projection { source: Box::new(node), expressions }
                }
                node
            }
        })
    }

    /// Injects hidden expressions into SELECT expressions. This is used for ORDER BY and HAVING, in
    /// order to apply these to fields or aggregates that are not present in the SELECT output, e.g.
    /// to order on a column that is not selected. This is done by replacing the relevant parts of
    /// the given expression with Column references to either existing columns or new, hidden
    /// columns in the select expressions. Returns the number of hidden columns added.
    fn inject_hidden(
        &self,
        expr: &mut ast::Expression,
        select: &mut Vec<(ast::Expression, Option<String>)>,
    ) -> Result<usize> {
        todo!()
    }

    /// Extracts aggregate functions from an AST expression tree. This finds the aggregate
    /// function calls, replaces them with ast::Expression::Column(i), maps the aggregate functions
    /// to aggregates, and returns them along with their argument expressions.
    fn extract_aggregates(
        &self,
        exprs: &mut [(ast::Expression, Option<String>)],
    ) -> Result<Vec<(Aggregate, ast::Expression)>> {
        todo!()
    }

    /// Extracts group by expressions, and replaces them with column references with the given
    /// offset. These can be either an arbitray expression, a reference to a SELECT column, or the
    /// same expression as a SELECT column. The following are all valid:
    ///
    /// SELECT released / 100 AS century, COUNT(*) FROM movies GROUP BY century
    /// SELECT released / 100, COUNT(*) FROM movies GROUP BY released / 100
    /// SELECT COUNT(*) FROM movies GROUP BY released / 100
    fn extract_groups(
        &self,
        exprs: &mut Vec<(ast::Expression, Option<String>)>,
        group_by: Vec<ast::Expression>,
        offset: usize,
    ) -> Result<Vec<(ast::Expression, Option<String>)>> {
        todo!()
    }

    /// Builds an aggregation node. All aggregate parameters and GROUP BY expressions are evaluated
    /// in a pre-projection, whose results are fed into an Aggregate node. This node computes the
    /// aggregates for the given groups, passing the group values through directly.
    fn build_aggregation(
        &self,
        environment: &mut Environment,
        source: Node,
        groups: Vec<(ast::Expression, Option<String>)>,
        aggregations: Vec<(Aggregate, ast::Expression)>,
    ) -> Result<Node> {
        todo!()
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

    /// Merges two scopes, by appending the given scope to self.
    fn merge(&mut self, scope: Environment) -> Result<()> {
        if self.is_constant {
            return Err(Error::Internal("Can't modify constant scope".into()));
        }
        for (label, table) in scope.tables {
            if self.tables.contains_key(&label) {
                return Err(Error::Value(format!("Duplicate table name {}", label)));
            }
            self.tables.insert(label, table);
        }
        for (table, label) in scope.columns {
            self.add_column(table, label);
        }
        Ok(())
    }

    /// Resolves a name, optionally qualified by a table name.
    fn resolve(&self, table: Option<&str>, name: &str) -> Result<usize> {
        if self.is_constant {
            return Err(Error::Value(format!(
                "Expression must be constant, found field {}",
                if let Some(table) = table { format!("{}.{}", table, name) } else { name.into() }
            )));
        }
        if let Some(table) = table {
            if !self.tables.contains_key(table) {
                return Err(Error::Value(format!("Unknown table {}", table)));
            }
            self.qualified
                .get(&(table.into(), name.into()))
                .copied()
                .ok_or_else(|| Error::Value(format!("Unknown field {}.{}", table, name)))
        } else if self.ambiguous.contains(name) {
            Err(Error::Value(format!("Ambiguous field {}", name)))
        } else {
            self.unqualified
                .get(name)
                .copied()
                .ok_or_else(|| Error::Value(format!("Unknown field {}", name)))
        }
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

    /// Number of columns in the current scope.
    fn len(&self) -> usize {
        self.columns.len()
    }

    /// Projects the scope. This takes a set of expressions and labels in the current scope,
    /// and returns a new scope for the projection.
    fn project(&mut self, projection: &[(Expression, Option<String>)]) -> Result<()> {
        todo!()
    }
}