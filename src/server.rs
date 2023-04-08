use serde::{Serialize, Deserialize};
use tonic::{Request, Response, Status};

use crate::proto::database::{database_server::Database, ExecutionRequest, ExecutionReply};
use crate::sql::engine::{KvSqlEngine, SqlEngine};
use crate::sql::execution::ResultSet;
use crate::sql::schema::Table;
use crate::sql::types::Row;

/// A featherDB server.
pub struct Server {
    /// The server's database.
    engine: KvSqlEngine
}

#[tonic::async_trait]
impl Database for Server {
    /// Executes a request. TODO: Implement other requests & error handling.
    async fn execute(&self, request: Request<ExecutionRequest>) -> Result<Response<ExecutionReply>, Status> {
        let request = request.into_inner();
        let data = deserialize::<Args>(&request.data).unwrap();
        println!("Got a request: {:?}", data);

        match data {
            Args::Execute(query) => {
                let result_set = self.engine.session().unwrap().execute(&query).unwrap();
                let reply = ExecutionReply {
                    data: serialize(&Reply::Execute(result_set)).unwrap(),
                };
                Ok(Response::new(reply))
            },
            _ => todo!()
        }
    }
}

impl Server {
    /// Creates a new server.
    pub fn new(engine: KvSqlEngine) -> Self {
        Self { engine }
    }
}

/// A client request.
#[derive(Debug, Serialize, Deserialize)]
pub enum Args {
    Execute(String),
    GetTable(String),
    ListTables,
    Status,
}

/// A server response.
#[derive(Debug, Serialize, Deserialize)]
pub enum Reply {
    Execute(ResultSet),
    Row(Option<Row>),
    GetTable(Table),
    ListTables(Vec<String>),
    // Status(sql::engine::Status),
}

/// Serializes MVCC metadata.
pub fn serialize<V: Serialize>(value: &V) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    Ok(bincode::serialize(value)?)
}

/// Deserializes MVCC metadata.
pub fn deserialize<'a, V: Deserialize<'a>>(bytes: &'a [u8]) -> Result<V, Box<dyn std::error::Error>> {
    Ok(bincode::deserialize(bytes)?)
}