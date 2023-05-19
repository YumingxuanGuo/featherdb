use featherdb::error::{Error, Result};
use featherdb::proto::featherkv::FeatherKvServer;
use featherdb::{concurrency, FeatherKV, sql, storage};
use tempfile::tempdir;
use tonic::transport::Server;
use serde::Deserialize;

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 2 {
        return Err(Error::Config("Usage: feather_kv <config_file_path>".to_string()));
    }
    let config = Config::new(&args[1])?;

    let log_store: Box<dyn storage::log::LogStore> = match config.storage_log.as_str() {
        "memory" => Box::new(storage::log::Memory::new()),
        name => return Err(Error::Config(format!("Unknown log storage engine {}", name))),
    };

    let kv_dir = tempdir().unwrap();
    let kv_store: Box<dyn storage::kv::KvStore> = match config.storage_kv.as_str() {
        "LSM_tempdir" => Box::new(storage::kv::LsmStorage::open(kv_dir)?),
        "LSM_local" => Box::new(storage::kv::LsmStorage::open(config.data_dir.clone())?),
        "B+tree_memory" => Box::new(storage::kv::StdBPlusTree::new()),
        name => return Err(Error::Config(format!("Unknown key-value storage engine {}", name))),
    };

    let state = Box::new(
        sql::engine::StateMachine::new(
            concurrency::MVCC::new(kv_store, true),
        )?
    );

    let server = FeatherKV::new(config.id, config.peers.clone(), state, log_store).await?;

    println!("FeatherKV server listening on {}...", config.serve_addr.clone());

    Server::builder()
        .add_service(FeatherKvServer::new(server))
        .serve(config.serve_addr.parse()?)
        .await
        .or_else(|e| Err(Error::Internal(format!("FeatherKV server failed: {:?}", e))))
}

#[derive(Debug, Deserialize)]
struct Config {
    id: u64,
    peers: Vec<String>,
    serve_addr: String,
    // log_level: String,
    data_dir: String,
    // sync: bool,
    storage_log: String,
    storage_kv: String,
}

impl Config {
    fn new(file: &str) -> Result<Self> {
        let c = config::Config::builder()
            .set_default("id", 0)?
            .set_default("peers", Vec::<String>::new())?
            .set_default("serve_addr", String::new())?
            // .set_default("log_level", "info")?
            .set_default("data_dir", "/var/lib/toydb")?
            // .set_default("sync", true)?
            .set_default("storage_log", "hybrid")?
            .set_default("storage_kv", "memory")?

            .add_source(config::File::with_name(file))
            .add_source(config::Environment::with_prefix("FEATHERDB"));

        Ok(c.build()?.try_deserialize()?)
    }
}