///! Evaluates SQL expressions and compares with expectations. FIXME: not yet implemented.
use featherdb::error::{Error, Result};
use featherdb::sql::engine::SqlEngine;
use featherdb::sql::types::Value;

fn eval_expr(expr: &str) -> Result<Value> {
    let engine = super::setup(vec![])?;
    engine.session()?.execute(&format!("SELECT {}", expr))?.into_value()
}

macro_rules! test_expr {
    ( $( $name:ident: $expr:expr => $expect:expr, )* ) => {
    $(
        #[test]
        fn $name() -> Result<()> {
            let expect: Result<Value> = $expect;
            let actual = eval_expr($expr);
            match expect {
                Ok(Float(e)) if e.is_nan() => match actual {
                    Ok(Float(a)) if a.is_nan() => {},
                    _ => panic!("Expected NaN, got {:?}", actual),
                }
                _ => assert_eq!($expect, actual),
            }
            Ok(())
        }
    )*
    }
}

use Value::*;

test_expr! {
    // Constants and literals
    const_case: "TrUe" => Ok(Boolean(true)),
    lit_integer_overflow: "9223372036854775808" => Err(Error::Parse("number too large to fit in target type".into())),
}