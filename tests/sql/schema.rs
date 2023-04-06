///! Schema-related tests, using an in-memory database against golden files in tests/sql/chema/
use featherdb::error::Result;
use featherdb::sql::engine::{SqlEngine as _, Mode, SqlTxn as _};
use featherdb::sql::schema::Catalog as _;

use goldenfile::Mint;
use std::io::Write;

macro_rules! test_schema {
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
                let mut mint = Mint::new("tests/sql/schema");
                let mut f = mint.new_goldenfile(stringify!($name))?;

                write!(f, "Query: {}\n", $query.trim())?;
                match engine.session()?.execute($query) {
                    Ok(result) => write!(f, "Result: {:?}\n\n", result)?,
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

test_schema! {
    create_table_bare: "CREATE TABLE",
    create_table_datatype: r#"
        CREATE TABLE name (
            id INTEGER PRIMARY KEY,
            "bool" BOOL,
            "boolean" BOOLEAN,
            "char" CHAR,
            "double" DOUBLE,
            "float" FLOAT,
            "int" INT,
            "integer" INTEGER,
            "string" STRING,
            "text" TEXT,
            "varchar" VARCHAR
        )
    "#,
    create_table_datatype_missing: "CREATE TABLE name (id)",
}