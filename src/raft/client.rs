use serde::{Deserialize, Serialize};
use tonic::transport::Channel;

use crate::error::{Result, Error};
use crate::proto::featherkv::{ExecutionReply, ExecutionRequest};
use crate::proto::featherkv::{FeatherKvClient, RegistrationRequest, RegistrationReply};
use super::RpcStatus;

/// A Raft-based key-value client.
#[derive(Clone)]
pub struct Client {
    servers: Vec<FeatherKvClient<Channel>>,
    session_id: u64,
    sequence_number: u64,
    last_leader: u64,
}

impl Client {
    /// Creates a new Raft client.
    pub async fn new(servers: Vec<String>) -> Result<Self> {
        let servers = {
            let mut clients = Vec::new();
            for addr in servers {
                let addr = format!("http://{}", addr).to_string();
                let client = FeatherKvClient::connect(addr).await.unwrap();
                clients.push(client);
            }
            clients
        };
        println!("Connected to {} FeatherKV servers.", servers.len());
        Ok(Self {
            servers,
            session_id: 0,
            sequence_number: 1,
            last_leader: 0,
        })
    }

    /// Registers a new session. This method will keep retrying until getting a valid reply.
    async fn register(&mut self) -> Result<()> {
        loop {
            match self.servers[self.last_leader as usize].register(RegistrationRequest { }).await {
                Ok(response) => {
                    let RegistrationReply { status, session_id, leader_hint } = response.into_inner();
                    self.last_leader = leader_hint;

                    match Self::deserialize::<RpcStatus>(&status)? {
                        RpcStatus::Ok => {
                            self.session_id = session_id;
                            self.sequence_number = 1;
                            return Ok(());
                        },
                        RpcStatus::NotLeader => { continue; },
                        RpcStatus::SessionExpired => {
                            return Err(Error::Internal("Should not get SessionExpired".into()));
                        },
                    }
                },

                // Timeout, retries the request.
                Err(e) => { continue; },
            }
        }
    }

    /// Mutates the Raft state machine. This method will keep retrying until getting a valid reply.
    pub async fn mutate(&mut self, mutation: Vec<u8>) -> Result<Vec<u8>> {
        loop {
            let execution_request = ExecutionRequest {
                session_id: self.session_id,
                sequence_number: self.sequence_number,
                operation: mutation.clone(),
            };

            match self.servers[self.last_leader as usize].mutate(execution_request).await {
                Ok(reply) => {
                    let ExecutionReply { status, response, leader_hint } = reply.into_inner();
                    self.last_leader = leader_hint;

                    match Self::deserialize::<RpcStatus>(&status)? {
                        RpcStatus::Ok => {
                            self.sequence_number += 1;
                            return Self::deserialize::<Result<Vec<u8>>>(&response)?;
                        },
                        RpcStatus::NotLeader => { continue; },
                        RpcStatus::SessionExpired => { self.register().await?; },
                    }
                },

                // Timeout, retries the request.
                Err(e) => { continue; },
            }
        }
    }

    /// Queries the Raft state machine. This method will keep retrying until getting a valid reply.
    pub async fn query(&mut self, query: Vec<u8>) -> Result<Vec<u8>> {
        loop {
            let execution_request = ExecutionRequest {
                session_id: self.session_id,
                sequence_number: self.sequence_number,
                operation: query.clone(),
            };

            match self.servers[self.last_leader as usize].query(execution_request).await {
                Ok(reply) => {
                    let ExecutionReply { status, response, leader_hint } = reply.into_inner();
                    self.last_leader = leader_hint;

                    match Self::deserialize::<RpcStatus>(&status)? {
                        RpcStatus::Ok => {
                            self.sequence_number += 1;
                            return Self::deserialize::<Result<Vec<u8>>>(&response)?;
                        },
                        RpcStatus::NotLeader => { continue; },
                        RpcStatus::SessionExpired => { self.register().await?; },
                    }
                },

                // Timeout, retries the request.
                Err(e) => { continue; },
            }
        }
    }

    /// Serializes a value for the Raft client.
    fn serialize<V: Serialize>(value: &V) -> Result<Vec<u8>> {
        Ok(bincode::serialize(value)?)
    }

    /// Deserializes a value from the Raft client.
    fn deserialize<'a, V: Deserialize<'a>>(bytes: &'a [u8]) -> Result<V> {
        Ok(bincode::deserialize(bytes)?)
    }
}