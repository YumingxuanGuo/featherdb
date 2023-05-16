use featherdb::error::{Error, Result};
use featherdb::FeatherDB;
use featherdb::proto::featherdb::FeatherDbServer;
use tonic::transport::Server;
use serde::Deserialize;

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::new("/config.yaml")?;
    let server = FeatherDB::new(config.peers.clone());

    Server::builder()
        .add_service(FeatherDbServer::new(server))
        .serve("".parse()?)
        .await
        .or_else(|e| Err(Error::Internal(format!("FeatherDB server failed: {:?}", e))))
}

#[derive(Debug, Deserialize)]
struct Config {
    id: String,
    peers: Vec<String>,
    listen_sql: String,
    listen_raft: String,
    log_level: String,
    data_dir: String,
    sync: bool,
    storage_log: String,
    storage_kv: String,
}

impl Config {
    fn new(file: &str) -> Result<Self> {
        let c = config::Config::builder()
            .set_default("id", "toydb")?
            .set_default("peers", Vec::<String>::new())?
            .set_default("listen_sql", "0.0.0.0:9605")?
            .set_default("listen_raft", "0.0.0.0:9705")?
            .set_default("log_level", "info")?
            .set_default("data_dir", "/var/lib/toydb")?
            .set_default("sync", true)?
            .set_default("storage_log", "hybrid")?
            .set_default("storage_kv", "memory")?

            .add_source(config::File::with_name(file))
            .add_source(config::Environment::with_prefix("TOYDB"));

        Ok(c.build()?.try_deserialize()?)
    }
}