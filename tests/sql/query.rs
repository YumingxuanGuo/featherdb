///! Tests for the SQL query engine. Runs SQL queries against an in-memory database,
///! and compares the results with golden files stored under tests/sql/query/
use featherdb::error::{Error, Result};
use featherdb::sql::engine::{SqlEngine as _, Mode, SqlTxn as _};
use featherdb::sql::execution::ResultSet;
use featherdb::sql::parser::Parser;
use featherdb::sql::plan::Plan;
use featherdb::sql::types::Row;

use goldenfile::Mint;
use std::io::Write;

macro_rules! test_query {
    ( $( $name:ident: $query:expr, )* ) => {
        $(
            test_query! { with []; $name: $query, }
        )*
    };
    ( with $setup:expr; $( $name:ident: $query:expr, )* ) => {
    $(
        #[test]
        fn $name() -> Result<()> {
            let mut setup = $setup.to_vec();
            setup.extend(vec![
                "CREATE TABLE countries (
                    id STRING PRIMARY KEY,
                    name STRING NOT NULL
                )",
                "INSERT INTO countries VALUES
                    ('fr', 'France'),
                    ('ru', 'Russia'),
                    ('us', 'United States of America')",
                "CREATE TABLE genres (
                    id INTEGER PRIMARY KEY,
                    name STRING NOT NULL
                )",
                "INSERT INTO genres VALUES
                    (1, 'Science Fiction'),
                    (2, 'Action'),
                    (3, 'Comedy')",
                "CREATE TABLE studios (
                    id INTEGER PRIMARY KEY,
                    name STRING NOT NULL,
                    country_id STRING INDEX REFERENCES countries
                )",
                "INSERT INTO studios VALUES
                    (1, 'Mosfilm', 'ru'),
                    (2, 'Lionsgate', 'us'),
                    (3, 'StudioCanal', 'fr'),
                    (4, 'Warner Bros', 'us')",
                "CREATE TABLE movies (
                    id INTEGER PRIMARY KEY,
                    title STRING NOT NULL,
                    studio_id INTEGER NOT NULL INDEX REFERENCES studios,
                    genre_id INTEGER NOT NULL INDEX REFERENCES genres,
                    released INTEGER NOT NULL,
                    rating FLOAT,
                    ultrahd BOOLEAN
                )",
                "INSERT INTO movies VALUES
                    (1, 'Stalker', 1, 1, 1979, 8.2, NULL),
                    (2, 'Sicario', 2, 2, 2015, 7.6, TRUE),
                    (3, 'Primer', 3, 1, 2004, 6.9, NULL),
                    (4, 'Heat', 4, 2, 1995, 8.2, TRUE),
                    (5, 'The Fountain', 4, 1, 2006, 7.2, FALSE),
                    (6, 'Solaris', 1, 1, 1972, 8.1, NULL),
                    (7, 'Gravity', 4, 1, 2013, 7.7, TRUE),
                    (8, 'Blindspotting', 2, 3, 2018, 7.4, TRUE),
                    (9, 'Birdman', 4, 3, 2014, 7.7, TRUE),
                    (10, 'Inception', 4, 1, 2010, 8.8, TRUE)",
            ]);
            let engine = super::setup(setup)?;

            let mut mint = Mint::new("tests/sql/query");
            let mut f = mint.new_goldenfile(format!("{}", stringify!($name)))?;

            write!(f, "Query: {}\n\n", $query)?;

            let mut txn = engine.begin(Mode::ReadWrite)?;

            // First, just try to generate a plan and execute it
            let result = Parser::new($query).parse()
                .and_then(|ast| Plan::build(ast, &mut txn))
                // .and_then(|plan| plan.optimize(&mut txn))
                .and_then(|plan| {
                    write!(f, "Explain:\n{}\n\n", plan)?;
                    plan.execute(&mut txn)
                });

            match result {
                Ok(ResultSet::Query{columns, buffered_rows}) => {
                    // TODO: `buffered_rows` should be wrapped with Result.
                    let rows: Vec<Row> = match buffered_rows {
                        Ok(rows) => rows,
                        Err(err) => {
                            write!(f, " {:?}", err)?;
                            return Ok(())
                        }
                    };
                    write!(f, "Result:")?;
                    if !columns.is_empty() || !rows.is_empty() {
                        write!(f, " {:?}\n", columns
                            .into_iter()
                            .map(|c| c.name.unwrap_or_else(|| "?".to_string()))
                            .collect::<Vec<_>>())?;
                        for row in rows {
                            write!(f, "{:?}\n", row)?;
                        }
                    } else {
                        write!(f, " <none>\n")?;
                    }
                }
                Ok(r) => return Err(Error::Internal(format!("Unexpected result {:?}\n", r))),
                Err(err) => {
                    write!(f, "Error: {}\n", err)?;
                }
            }
            write!(f, "\n")?;

            // Then output some parse and plan trees, for debugging.
            write!(f, "AST: ")?;
            let ast = match Parser::new($query).parse() {
                Ok(ast) => ast,
                Err(err) => {
                    write!(f, "{:?}", err)?;
                    return Ok(())
                }
            };
            write!(f, "{:#?}\n\n", ast)?;

            write!(f, "Plan: ")?;
            let plan = match Plan::build(ast, &mut txn) {
                Ok(plan) => plan,
                Err(err) => {
                    write!(f, "{:?}", err)?;
                    return Ok(())
                }
            };
            write!(f, "{:#?}\n\n", plan)?;

            write!(f, "Optimized plan: ")?;
            let plan = match plan.optimize(&mut txn) {
                Ok(plan) => plan,
                Err(err) => {
                    write!(f, "{:?}", err)?;
                    return Ok(())
                }
            };
            write!(f, "{:#?}\n\n", plan)?;

            txn.commit()?;
            Ok(())
        }
    )*
    }
}

test_query! {
    all: "SELECT * FROM movies",
    bare: "SELECT",
    trailing_comma: "SELECT 1,",
    lowercase: "select 1",
}