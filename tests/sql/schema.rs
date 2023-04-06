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
    create_table_datatype_null: "CREATE TABLE name (id INTEGER PRIMARY KEY, value NULL)",

    create_table_name_alphanumeric: "CREATE TABLE a_123 (id INTEGER PRIMARY KEY)",
    create_table_name_case: "CREATE TABLE mIxEd_cAsE (ÄÅÆ STRING PRIMARY KEY)",
    create_table_name_emoji: "CREATE TABLE 👋 (🆔 INTEGER PRIMARY KEY)",
    create_table_name_emoji_quoted: r#"CREATE TABLE "👋" ("🆔" INTEGER PRIMARY KEY)"#,
    create_table_name_japanese: "CREATE TABLE 表 (身元 INTEGER PRIMARY KEY, 名前 STRING)",
    create_table_name_keyword: "CREATE TABLE table (id INTEGER PRIMARY KEY)",
    create_table_name_keyword_quoted: r#"CREATE TABLE "table" (id INTEGER PRIMARY KEY)"#,
    create_table_name_missing: "CREATE TABLE (id INTEGER PRIMARY KEY)",
    create_table_name_quote_single: r#"CREATE TABLE 'name' (id INTEGER PRIMARY KEY)"#,
    create_table_name_quote_double: r#"CREATE TABLE "name" (id INTEGER PRIMARY KEY)"#,
    create_table_name_quote_double_unterminated: r#"CREATE TABLE "name (id INTEGER PRIMARY KEY)"#,
    create_table_name_quote_double_escaped: r#"CREATE TABLE "name with "" quote" (id INTEGER PRIMARY KEY)"#,
    create_table_name_quote_double_single: r#"CREATE TABLE "name with ' quote" (id INTEGER PRIMARY KEY)"#,
    create_table_name_underscore_prefix: "CREATE TABLE _name (id INTEGER PRIMARY KEY)",

    create_table_columns_empty: "CREATE TABLE name ()",
    create_table_columns_missing: "CREATE TABLE name",

    create_table_pk_missing: "CREATE TABLE name (id INTEGER)",
    create_table_pk_multiple: "CREATE TABLE name (id INTEGER PRIMARY KEY, name STRING PRIMARY KEY)",
    create_table_pk_nullable: "CREATE TABLE name (id INTEGER PRIMARY KEY NULL)",
    create_table_pk_default: "CREATE TABLE name (id INTEGER PRIMARY KEY DEFAULT 1)",
    create_table_pk_unique: "CREATE TABLE name (id INTEGER PRIMARY KEY UNIQUE)",

    create_table_null: "CREATE TABLE name (id INTEGER PRIMARY KEY, value STRING NULL)",
    create_table_null_not: "CREATE TABLE name (id INTEGER PRIMARY KEY, value STRING NOT NULL)",
    create_table_null_not_null: "CREATE TABLE name (id INTEGER PRIMARY KEY, value STRING NULL NOT NULL)",
    create_table_null_default: "CREATE TABLE name (id INTEGER PRIMARY KEY, value STRING)",

    create_table_default_boolean: "CREATE TABLE name (id INTEGER PRIMARY KEY, value BOOLEAN DEFAULT TRUE)",
    create_table_default_float: "CREATE TABLE name (id INTEGER PRIMARY KEY, value FLOAT DEFAULT 3.14)",
    create_table_default_integer: "CREATE TABLE name (id INTEGER PRIMARY KEY, value INTEGER DEFAULT 7)",
    create_table_default_string: "CREATE TABLE name (id INTEGER PRIMARY KEY, value STRING DEFAULT 'foo')",
    create_table_default_null: "CREATE TABLE name (id INTEGER PRIMARY KEY, value STRING DEFAULT NULL)",
    create_table_default_null_not: "CREATE TABLE name (id INTEGER PRIMARY KEY, value STRING NOT NULL DEFAULT NULL)",
    create_table_default_expr: "CREATE TABLE name (id INTEGER PRIMARY KEY, value INTEGER DEFAULT 1 + 2 * 3)",
    create_table_default_conflict: "CREATE TABLE name (id INTEGER PRIMARY KEY, value STRING DEFAULT 7)",
    create_table_default_conflict_float_integer: "CREATE TABLE name (id INTEGER PRIMARY KEY, value FLOAT DEFAULT 7)",
    create_table_default_conflict_integer_float: "CREATE TABLE name (id INTEGER PRIMARY KEY, value INTEGER DEFAULT 3.14)",

    create_table_index: "CREATE TABLE name (id INTEGER PRIMARY KEY, value STRING INDEX)",
    create_table_index_pk: "CREATE TABLE name (id INTEGER PRIMARY KEY INDEX, value STRING)",

    create_table_unique: "CREATE TABLE name (id INTEGER PRIMARY KEY, value STRING UNIQUE)",
    create_table_unique_null: "CREATE TABLE name (id INTEGER PRIMARY KEY, value STRING NULL UNIQUE)",
    create_table_unique_not_null: "CREATE TABLE name (id INTEGER PRIMARY KEY, value STRING NOT NULL UNIQUE)",
    create_table_unique_default: "CREATE TABLE name (id INTEGER PRIMARY KEY, value STRING DEFAULT 'foo' UNIQUE)",
}