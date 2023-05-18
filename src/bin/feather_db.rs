use featherdb::error::{Error, Result};
use featherdb::FeatherDB;
use featherdb::proto::featherdb::FeatherDbServer;
use tonic::transport::Server;
use serde::Deserialize;

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::new("config/feather_db.yaml")?;
    let server = FeatherDB::new(config.kv_addrs);

    println!("FeatherDB server listening on {}...", config.serve_addr.clone());

    Server::builder()
        .add_service(FeatherDbServer::new(server))
        .serve(config.serve_addr.parse()?)
        .await
        .or_else(|e| Err(Error::Internal(format!("FeatherDB server failed: {:?}", e))))
}

#[derive(Debug, Deserialize)]
struct Config {
    // id: String,
    kv_addrs: Vec<String>,
    serve_addr: String,
}

impl Config {
    fn new(file: &str) -> Result<Self> {
        let c = config::Config::builder()
            // .set_default("id", "toydb")?
            .set_default("kv_addrs", Vec::<String>::new())?
            .set_default("serve_addr", String::new())?

            .add_source(config::File::with_name(file))
            .add_source(config::Environment::with_prefix("FEATHERDB"));

        Ok(c.build()?.try_deserialize()?)
    }
}