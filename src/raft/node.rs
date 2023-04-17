#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]
#![allow(unused_mut)]

use std::sync::{Arc, Mutex};
use futures::{stream::FuturesUnordered, Future};
use tokio_stream::StreamExt;
use tonic::{Response, Status, Request};

use crate::error::{Result, Error};
use crate::proto::raft::raft_service_server::RaftService;
use crate::proto::raft::{RequestVoteReply, RequestVoteArgs, AppendEntriesArgs, AppendEntriesReply};
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

    /// The service using Raft (e.g. a k/v server) wants to start
    /// agreement on the next command to be appended to Raft's log. If this
    /// server isn't the leader, returns [`Error::NotLeader`]. Otherwise start
    /// the agreement and return immediately. There is no guarantee that this
    /// command will ever be committed to the Raft log, since the leader
    /// may fail or lose an election. Even if the Raft instance has been killed,
    /// this function should return gracefully.
    ///
    /// The first value of the tuple is the index that the command will appear
    /// at if it's ever committed; the second is the current term.
    ///
    /// This method must return without blocking on the raft.
    pub fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    // where
    //     M: labcodec::Message,
    {
        let mut raft = self.raft.lock()?;
        if !raft.is_leader() {
            return Err(Error::NotLeader);
        }
        raft.start(command)
    }

    /// The current term of this peer.
    pub fn term(&self) -> Result<u64> {
        Ok(self.raft.lock()?.current_term)
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> Result<bool> {
        Ok(self.raft.lock()?.is_leader())
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
                    raft.send_heartbeats();
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
        request: Request<AppendEntriesArgs>,
    ) -> core::result::Result<Response<AppendEntriesReply>, Status> {
        let mut raft = self.raft.lock().unwrap();
        let args = request.into_inner();

        if args.term < raft.current_term {
            let reply = AppendEntriesReply {
                term: raft.current_term,
                success: false
            };
            return Ok(Response::new(reply));
        }

        raft.become_follower(args.term, None);

        if let Role::Follower { ref mut leader, ref mut leader_seen_ticks, .. } = raft.role {
            *leader_seen_ticks = 0;
            *leader = Some(args.leader_id);
        }

        let reply = AppendEntriesReply {
            term: raft.current_term,
            success: true
        };
        Ok(Response::new(reply))
    }
}