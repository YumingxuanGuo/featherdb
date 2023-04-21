#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]
#![allow(unused_mut)]

use std::sync::{Arc, Mutex};
use std::time::Duration;
use futures::{stream::FuturesUnordered, Future};
use rand::Rng;
use tokio_stream::StreamExt;
use tonic::transport::{Server, Channel};
use tonic::{Response, Status, Request};

use crate::error::{Result, Error};
use crate::proto::raft::raft_service_client::RaftServiceClient;
use crate::proto::raft::raft_service_server::{RaftService, RaftServiceServer};
use crate::proto::raft::{RequestVoteReply, RequestVoteArgs, AppendEntriesArgs, AppendEntriesReply};
use super::{Raft, Role, HEARTBEAT_INTERVAL};

// An interceptor function. TODO: use layer instead.
fn intercept(req: Request<()>) -> core::result::Result<Request<()>, Status> {
    let mut rng = rand::thread_rng();
    if rng.gen_bool(0.0) {
        Err(Status::unavailable("RPC got intercepted."))
    } else {
        Ok(req)
    }
}

#[derive(Clone)]
pub struct Node {
    // Your code here.
    raft: Arc<Mutex<Raft>>,
}

impl Node {
    /// Create a new raft service. TODO: Set up the raft server according to the config.
    pub async fn new(me: u64, peers: Vec<String>) -> Result<Node> {
        let node = Node { raft: Arc::new(Mutex::new(Raft::new()?)) };
        let node_clone = node.clone();

        let layer = tower::ServiceBuilder::new()
            .layer(tonic::service::interceptor(intercept))
            .into_inner();

        let my_addr = peers[me as usize].parse()?;
        tokio::spawn(async move {
            match Server::builder()
                .layer(layer)
                .add_service(RaftServiceServer::new(node_clone))
                .serve(my_addr)
                .await {
                    Ok(_) => { println!("Raft server built on addr {:?}", my_addr) },
                    Err(err) => println!("Raft server failed on addr {:?}: {:?}", my_addr, err),
                };
        });

        let mut conns = vec![false; peers.len()];
        'outer: loop {
            let mut conn_count = 0;
            for i in 0..peers.len() {
                if conns[i] {
                    conn_count += 1;
                    if conn_count == peers.len() {
                        break 'outer;
                    }
                    continue;
                }
                let mut addr = "http://".to_string();
                addr.push_str(&peers[i]);
                let channel = match Channel::from_shared(addr).unwrap().connect().await {
                    Ok(channel) => channel,
                    Err(err) => {
                        continue;
                    },
                };
                let mut client = RaftServiceClient::new(channel);
                let mut raft = node.raft.lock()?;
                raft.me = me;
                raft.peers.push(client);
                conns[i] = true;
            }
            tokio::time::sleep(Duration::from_millis(1000)).await;
        }

        Ok(node)
    }

    /// Start the Raft server. This method should not return until shutdown.
    pub async fn serve(&self) -> Result<()> {
        loop {
            tokio::time::sleep(Duration::from_millis(100)).await;
            self.tick().unwrap();
        }
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
    pub fn start(&self, command: Option<Vec<u8>>) -> Result<(u64, u64)> {
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

    /// The id of this peer.
    pub fn id(&self) -> Result<u64> {
        Ok(self.raft.lock()?.me)
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