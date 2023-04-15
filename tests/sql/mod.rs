mod expression;
mod mutation;
mod query;
mod schema;

use featherdb::concurrency::MVCC;
use featherdb::error::Result;
use featherdb::sql::engine::{SqlEngine, KvSqlEngine};
use featherdb::storage::kv::StdBPlusTree;

/// Sets up a basic in-memory SQL engine with an initial dataset.
fn setup(queries: Vec<&str>) -> Result<KvSqlEngine> {
    let engine = KvSqlEngine::new(MVCC::new(
        Box::new(StdBPlusTree::new()),
        false,
    ));
    let session = engine.session()?;
    session.execute("BEGIN")?;
    for query in queries {
        session.execute(query)?;
    }
    session.execute("COMMIT")?;
    Ok(engine)
}