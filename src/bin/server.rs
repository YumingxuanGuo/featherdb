use featherdb::concurrency::MVCC;
use featherdb::error::Result;
use featherdb::proto::registration::registration_server::RegistrationServer;
use featherdb::sql::engine::KvSqlEngine;
use featherdb::storage::kv::StdBPlusTree;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<()> {
    let addr = "127.0.0.1:50052".parse()?;

    println!("FeatherDB server listening on {}", addr);

    let engine = KvSqlEngine::new(MVCC::new(
        Box::new(StdBPlusTree::new()),
        false,
    ));
    let server = featherdb::server::Server::new(engine);
    match Server::builder()
        .add_service(RegistrationServer::new(server))
        .serve(addr)
        .await {
            Ok(_) => { },
            Err(err) => println!("Registration server failed: {:?}", err),
        };

    Ok(())
}