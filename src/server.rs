use std::collections::HashMap;
use std::sync::{Mutex, Arc};

use serde::{Serialize, Deserialize};
use tokio::sync::{mpsc, oneshot};
use tonic::{Request, Response};

use crate::concurrency::Mode;
use crate::error::{RpcResult, Result, Error};
use crate::proto::featherdb::{RegistrationArgs, RegistrationReply, ExecutionReply, FeatherDb, ExecutionArgs};
use crate::sql::engine::{SqlEngine, SqlSession, RaftSqlEngine};
use crate::sql::execution::ResultSet;
use crate::sql::schema::{Table, Catalog};
use crate::sql::types::Row;

/// A client request.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ClientRequest {
    Query(String),
    GetTable(String),
    ListTables,
    Status,
}

/// A server response.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ClientResponse {
    Query(ResultSet),
    Row(Option<Row>),
    GetTable(Table),
    ListTables(Vec<String>),
    // Status(sql::engine::Status),
}

#[derive(Debug)]
/// A task for a key-value session.
pub struct Task {
    reply_tx: oneshot::Sender<ExecutionReply>,
    sequence_number: u64,
    client_request: Vec<u8>,
}

/// A featherDB server with Raft backend.
pub struct FeatherDB {
    /// The server's engine.
    servers: Vec<String>,
    /// The server's next client session id.
    next_session_id: Mutex<u64>,
    /// The sending channels of the ongoing sessions.
    session_txs: Arc<Mutex<HashMap<u64, mpsc::UnboundedSender<Task>>>>,
}

impl FeatherDB {
    /// Creates a new server.
    pub fn new(servers: Vec<String>) -> Self {
        Self {
            servers,
            next_session_id: Mutex::new(1),
            session_txs: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Serializes RPC arguments.
    pub fn serialize<V: Serialize>(value: &V) -> Result<Vec<u8>> {
        Ok(bincode::serialize(value)?)
    }
    
    /// Deserializes RPC arguments.
    pub fn deserialize<'a, V: Deserialize<'a>>(bytes: &'a [u8]) -> Result<V> {
        Ok(bincode::deserialize(bytes)?)
    }
}

#[tonic::async_trait]
impl FeatherDb for FeatherDB {
    async fn register(&self, _request: Request<RegistrationArgs>) -> RpcResult<RegistrationReply> {
        let session_id = {
            let mut next_session_id = self.next_session_id.lock().unwrap();
            let session_id = *next_session_id;
            *next_session_id += 1;
            session_id
        };
        println!("Registered a new client, id = {}.", session_id);

        let (task_tx, task_rx) = mpsc::unbounded_channel();
        self.session_txs.lock().unwrap().insert(session_id, task_tx);
        tokio::spawn(Session::new(self.servers.clone(), task_rx).await?.serve());

        Ok(Response::new(RegistrationReply { session_id }))
    }

    async fn execute(&self, request: Request<ExecutionArgs>) -> RpcResult<ExecutionReply> {
        let ExecutionArgs { session_id, sequence_number, client_request } = request.into_inner();
        let (reply_tx, reply_rx) = oneshot::channel();
        let task = Task {
            reply_tx,
            sequence_number,
            client_request,
        };

        {
            let mut session_txs = self.session_txs.lock().unwrap();
            match session_txs.get_mut(&session_id) {
                Some(session_tx) => {
                    session_tx.send(task).unwrap();
                },
                None => {
                    todo!()
                },
            }
        }

        let reply = reply_rx.await.unwrap();
        Ok(Response::new(reply))
    }
}

pub struct Session {
    /// The session's engine.
    sql: SqlSession<RaftSqlEngine>,
    /// The last applied sequence number.
    last_applied_sequence_number: u64,
    /// The result of the last operation.
    stored_result: Option<Result<ClientResponse>>,
    /// The channel to receive task from.
    task_rx: mpsc::UnboundedReceiver<Task>,
}

impl Session {
    /// Creates a new session.
    pub async fn new(
        servers: Vec<String>,
        task_rx: mpsc::UnboundedReceiver<Task>,
    ) -> Result<Self> {
        Ok(Self {
            sql: RaftSqlEngine::new(servers).await?.session()?,
            last_applied_sequence_number: 0,
            stored_result: None,
            task_rx,
        })
    }

    /// Starts the session.
    pub async fn serve(mut self) -> Result<()> {
        while let Some(Task { reply_tx, sequence_number, client_request }) = self.task_rx.recv().await {
            if sequence_number == self.last_applied_sequence_number {
                let reply = ExecutionReply {
                    result: FeatherDB::serialize(
                        &self.stored_result
                            .clone()
                            .ok_or_else(|| Error::Internal("No stored result.".to_string()))?
                    )?
                };
                reply_tx.send(reply).unwrap();
                continue;
            }
            
            let client_request = FeatherDB::deserialize::<ClientRequest>(&client_request)?;
            let client_response = tokio::task::block_in_place(|| self.execute(client_request));
            self.last_applied_sequence_number = sequence_number;
            self.stored_result = Some(client_response.clone());

            let reply = ExecutionReply {
                result: FeatherDB::serialize(&client_response)?,
            };
            reply_tx.send(reply).unwrap();
        }

        Ok(())
    }

    fn execute(&mut self, client_request: ClientRequest) -> Result<ClientResponse> {
        Ok(match client_request {
            ClientRequest::Query(query) => {
                let result_set = self.sql.execute(&query)?;
                ClientResponse::Query(result_set)
            },
            ClientRequest::GetTable(table) => {
                let table = self.sql.with_txn(
                    Mode::ReadOnly,
                    |txn| {
                        txn.assert_read_table(&table)
                    }
                )?;
                ClientResponse::GetTable(table)
            },
            ClientRequest::ListTables => {
                let result = self.sql.with_txn(
                    Mode::ReadOnly,
                    |txn| {
                        Ok(txn.scan_tables()?.map(|t| t.name).collect())
                    }
                )?;
                ClientResponse::ListTables(result)
            },
            ClientRequest::Status => {
                todo!()
            },
        })
    }
}

/// Serializes RPC arguments.
pub fn serialize<V: Serialize>(value: &V) -> crate::error::Result<Vec<u8>> {
    Ok(bincode::serialize(value)?)
}

/// Deserializes RPC arguments.
pub fn deserialize<'a, V: Deserialize<'a>>(bytes: &'a [u8]) -> crate::error::Result<V> {
    Ok(bincode::deserialize(bytes)?)
}