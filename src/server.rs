use parking_lot::Mutex;
use serde::{Serialize, Deserialize};
use tonic::{Request, Response, Status};

use crate::concurrency::Mode;
use crate::proto::registration::{registration_server::Registration, RegistrationRequest, RegistrationReply};
use crate::proto::session::{session_server::{Session, SessionServer}, ExecutionRequest, ExecutionReply};
use crate::sql::engine::{KvSqlEngine, SqlEngine, SqlSession};
use crate::sql::execution::ResultSet;
use crate::sql::schema::{Table, Catalog};
use crate::sql::types::Row;

/// A featherDB server.
pub struct Server {
    /// The server's database.
    engine: KvSqlEngine,
    /// The server's next client session id.
    next_session_id: Mutex<u64>,
}

impl Server {
    /// Creates a new server.
    pub fn new(engine: KvSqlEngine) -> Self {
        Self { engine, next_session_id: Mutex::new(1) }
    }
}

#[tonic::async_trait]
impl Registration for Server {
    /// Registers a new client. 
    /// TODO: used port number.
    async fn register(&self, _request: Request<RegistrationRequest>) -> Result<Response<RegistrationReply>, Status> {
        println!("Got a client connection");

        // Generates the port number for the client session using the id, 
        // then increments the session id for the next client.
        let mut guard = self.next_session_id.lock();
        let port = (50052 + *guard).to_string();
        *guard += 1;
        let addr = match format!("127.0.0.1:{}", port).parse() {
            Ok(addr) => addr,
            Err(err) => return Err(Status::internal(format!("{err}"))).into(),
        };

        // Spawns a new session to handle this client's requests.
        let client_session = ClientSession::new(self.engine.clone())?;
        tokio::spawn(async move {
            match tonic::transport::Server::builder()
                .add_service(SessionServer::new(client_session))
                .serve(addr)
                .await {
                    Ok(_) => { },
                    Err(err) => println!("Creating new client session failed: {:?}", err),
                };
        });

        Ok(Response::new(RegistrationReply { port }))
    }
}

/// A client session coupled to a SQL session.
pub struct ClientSession {
    sql: SqlSession<KvSqlEngine>,
}

impl ClientSession {
    /// Creates a new session.
    pub fn new(engine: KvSqlEngine) -> crate::error::Result<Self> {
        Ok(Self { sql: engine.session()? })
    }
}

#[tonic::async_trait]
impl Session for ClientSession {
    /// Executes a request. TODO: Implement other requests & error handling.
    async fn execute(&self, request: Request<ExecutionRequest>) -> Result<Response<ExecutionReply>, Status> {
        let request = deserialize::<Args>(&request.into_inner().data)?;
        match request {
            Args::Query(query) => {
                Ok(Response::new(ExecutionReply {
                    data: serialize(&Reply::Query(
                        self.sql.execute(&query)?
                    ))?,
                }))
            },
            Args::GetTable(table) => {
                Ok(Response::new(ExecutionReply {
                    data: serialize(&Reply::GetTable(
                        self.sql.with_txn(Mode::ReadOnly, |txn| {
                            txn.assert_read_table(&table)
                        })?
                    ))?,
                }))
            },
            Args::ListTables => {
                Ok(Response::new(ExecutionReply {
                    data: serialize(&Reply::ListTables(
                        self.sql.with_txn(Mode::ReadOnly, |txn| {
                            Ok(txn.scan_tables()?.map(|t| t.name).collect())
                        })?
                    ))?,
                }))
            },
            Args::Status => todo!(),
        }
    }
}

/// A client request.
#[derive(Debug, Serialize, Deserialize)]
pub enum Args {
    Query(String),
    GetTable(String),
    ListTables,
    Status,
}

/// A server response.
#[derive(Debug, Serialize, Deserialize)]
pub enum Reply {
    Query(ResultSet),
    Row(Option<Row>),
    GetTable(Table),
    ListTables(Vec<String>),
    // Status(sql::engine::Status),
}

/// Serializes RPC arguments.
pub fn serialize<V: Serialize>(value: &V) -> crate::error::Result<Vec<u8>> {
    Ok(bincode::serialize(value)?)
}

/// Deserializes RPC arguments.
pub fn deserialize<'a, V: Deserialize<'a>>(bytes: &'a [u8]) -> crate::error::Result<V> {
    Ok(bincode::deserialize(bytes)?)
}