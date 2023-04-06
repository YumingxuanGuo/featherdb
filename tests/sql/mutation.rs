///! Mutation tests, using an in-memory database against golden files in tests/sql/mutation/
use featherdb::error::Result;
use featherdb::sql::engine::{SqlEngine as _, Mode, SqlTxn as _};
use featherdb::sql::schema::Catalog as _;

use goldenfile::Mint;
use std::io::Write;

macro_rules! test_mutation {
    ( $( $name:ident: $query:expr, )* ) => {
        $(
            test_schema! { with []; $name: $query, }
        )*
    };
    ( with $setup:expr; $( $name:ident: $query:expr, )* ) => {
        $(
            #[test]
            fn $name() -> Result<()> {
                let setup: &[&str] = &$setup;
                let engine = super::setup(setup.into())?;
                let mut mint = Mint::new("tests/sql/mutation");
                let mut f = mint.new_goldenfile(stringify!($name))?;

                write!(f, "Query: {}\n", $query.trim())?;
                match engine.session()?.execute($query) {
                    Ok(resultset) => {
                        write!(f, "Result: {:?}\n\n", resultset)?;
                    },
                    Err(err) => write!(f, "Error: {:?}\n\n", err)?,
                };

                write!(f, "Storage:")?;
                let txn = engine.begin(Mode::ReadWrite)?;
                for table in txn.scan_tables()? {
                    write!(f, "\n{}\n", table)?;
                    for row in txn.scan(&table.name, None)? {
                        write!(f, "{:?}\n", row?)?;
                    }

                    for column in table.columns.iter().filter(|c| c.is_indexed) {
                        write!(f, "\nIndex {}.{}\n", table.name, column.name)?;
                        let mut scan = txn.scan_index(&table.name, &column.name)?;
                        while let Some((value, pks)) = scan.next().transpose()? {
                            let mut pks = pks.into_iter().collect::<Vec<_>>();
                            pks.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                            write!(f, "{:?} => {:?}\n", value, pks)?;
                        }
                    }
                }
                txn.rollback()?;

                Ok(())
            }
        )*
    }
}

test_mutation! { with [
        "CREATE TABLE test (
            id INTEGER PRIMARY KEY DEFAULT 0,
            name STRING INDEX,
            value INTEGER
        )",
        "INSERT INTO test VALUES (1, 'a', 101), (2, 'b', 102), (3, 'c', 103)",
        "CREATE TABLE other (id INTEGER PRIMARY KEY)",
        "INSERT INTO other VALUES (1), (2), (3)",
    ];

    delete_all: "DELETE FROM test",
}