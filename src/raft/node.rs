#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]
#![allow(unused_mut)]

use std::sync::{Arc, Mutex};
use futures::{stream::FuturesUnordered, Future};
use tokio_stream::StreamExt;
use tonic::{Response, Status, Request};

use crate::{error::Result, proto::raft::raft_service_server::RaftService};
use crate::proto::raft::{RequestVoteReply, RequestVoteArgs};
use super::{Raft, Role, HEARTBEAT_INTERVAL};

#[derive(Clone)]
pub struct Node {
    // Your code here.
    raft: Arc<Mutex<Raft>>,
}

impl Node {
    /// Create a new raft service.
    pub fn new(raft: Raft) -> Node {
        Node { raft: Arc::new(Mutex::new(raft)) }
    }

    /// Tick the underlying Raft node to the next state.
    pub fn tick(&self) -> Result<()> {
        let mut raft = self.raft.lock()?;

        match raft.role {
            Role::Follower { ref mut leader_seen_ticks, leader_seen_timeout, .. } => {
                *leader_seen_ticks += 1;
                if *leader_seen_ticks >= leader_seen_timeout {
                    raft.become_candidate();
                    let mut request_vote_replies = raft.solicit_votes();
                    let node = self.clone();
                    let quorum = raft.quorum();
                    let current_term = raft.current_term;
                    tokio::spawn(async move {
                        node.count_votes(quorum, current_term, request_vote_replies).await;
                    });
                }
            }
            Role::Candidate {ref mut election_ticks, election_timeout, .. } => {
                *election_ticks += 1;
                if *election_ticks >= election_timeout {
                    raft.become_candidate();
                    let mut request_vote_replies = raft.solicit_votes();
                    let node = self.clone();
                    let quorum = raft.quorum();
                    let current_term = raft.current_term;
                    tokio::spawn(async move {
                        node.count_votes(quorum, current_term, request_vote_replies).await;
                    });
                }
            }
            Role::Leader { ref mut heartbeat_ticks, .. } => {
            *heartbeat_ticks += 1;
                if *heartbeat_ticks >= HEARTBEAT_INTERVAL {
                    *heartbeat_ticks = 0;
                    // TODO: Send heartbeats.
                }
            }
        }

        Ok(())
    }

    /// Counts the number of votes for a candidate.
    async fn count_votes(
        &self,
        quorum: u64,
        current_term: u64,
        mut request_vote_replies: FuturesUnordered<impl 
            Future<Output = core::result::Result<Response<RequestVoteReply>, Status>>>,
    ) {
        let mut vote_count = 1;
        while let Some(res) = request_vote_replies.next().await {
            let (term, vote_granted) = match res {
                Ok(res) => (res.get_ref().term, res.get_ref().vote_granted),
                Err(_) => continue,
            };
            if vote_granted {
                vote_count += 1;
                if vote_count >= quorum {
                    self.raft.lock().unwrap().become_leader();
                    return;
                }
            }
            if term > current_term {
                self.raft.lock().unwrap().become_follower(term, None);
                return;
            }
        }
    }
}

#[tonic::async_trait]
impl RaftService for Node {
    /// RequestVote RPC handler.
    async fn request_vote(
        &self,
        request: Request<RequestVoteArgs>,
    ) -> core::result::Result<Response<RequestVoteReply>, Status> {
        let mut raft = self.raft.lock().unwrap();
        let args = request.into_inner();

        if args.term < raft.current_term {
            let reply = RequestVoteReply {
                term: raft.current_term,
                vote_granted: false
            };
            return Ok(Response::new(reply));
        }

        if args.term > raft.current_term {
            raft.become_follower(args.term, None);
        }

        if raft.voted_for.is_none() || raft.voted_for == Some(args.candidate_id) {
            raft.voted_for = Some(args.candidate_id);
            raft.persist();
        } else {
            let reply = RequestVoteReply {
                term: raft.current_term,
                vote_granted: false
            };
            return Ok(Response::new(reply));
        }

        let reply = RequestVoteReply {
            term: raft.current_term,
            vote_granted: true
        };
        Ok(Response::new(reply))
    }

    /// AppendEntries RPC handler.
    async fn append_entries(
        &self,
        request: tonic::Request<crate::proto::raft::AppendEntriesArgs>,
    ) -> core::result::Result<tonic::Response<crate::proto::raft::AppendEntriesReply>, tonic::Status> {
        todo!()
    }
}