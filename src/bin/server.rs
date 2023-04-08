use featherdb::concurrency::MVCC;
use featherdb::proto::database;
use featherdb::sql::engine::KvSqlEngine;
use featherdb::storage::kv::StdBPlusTree;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "127.0.0.1:50052".parse()?;

    println!("DatabaseServer listening on {}", addr);

    let engine = KvSqlEngine::new(MVCC::new(
        Box::new(StdBPlusTree::new()),
        false,
    ));
    let server = featherdb::server::Server::new(engine);
    Server::builder()
        .add_service(database::database_server::DatabaseServer::new(server))
        .serve(addr)
        .await?;

    Ok(())
}