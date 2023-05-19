use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::{stream::FuturesUnordered, Future};
use rand::Rng;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tonic::transport::Server;
use tonic::{Response, Status, Request};

use crate::error::{Result, Error, RpcResult};
use crate::proto::raft::raft_service_client::RaftServiceClient;
use crate::proto::raft::raft_service_server::{RaftService, RaftServiceServer};
use crate::proto::raft::{RequestVoteReply, RequestVoteArgs, AppendEntriesArgs, AppendEntriesReply};
use crate::server::{deserialize, serialize};
use crate::storage::log::LogStore;
use super::{HEARTBEAT_INTERVAL, Raft, Role, ApplyMsg, Command, Entry};

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
    pub async fn new(
        me: u64,
        peers: Vec<String>,
        apply_tx: mpsc::UnboundedSender<ApplyMsg>,
        log_store: Box<dyn LogStore>,
    ) -> Result<Node> {
        let node = Node { raft: Arc::new(Mutex::new(Raft::new(
            me,
            apply_tx,
            log_store,
        )?)) };
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
                let client = match RaftServiceClient::connect(addr).await {
                    Ok(channel) => channel,
                    Err(_) => { continue; },
                };
                let mut raft = node.raft.lock()?;
                raft.peers.push(client);
                conns[i] = true;
            }
            tokio::time::sleep(Duration::from_millis(1000)).await;
        }

        Ok(node)
    }

    /// Start the Raft server. This method should not return until shutdown.
    pub async fn serve(self) -> Result<()> {
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
    pub fn start(&self, command: Command) -> Result<(u64, u64)> {
        let mut raft = self.raft.lock()?;
        if !raft.is_leader() {
            return Err(Error::NotLeader);
        }
        let (index, term) = raft.start(command)?;
        Ok((index, term))
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

    /// The id of the peer that this peer believes is the current leader.
    pub fn leader_id(&self) -> Result<u64> {
        Ok(self.raft.lock()?.leader_id())
    }

    /// Tick the underlying Raft node to the next state.
    pub fn tick(&self) -> Result<()> {
        let mut raft = self.raft.lock()?;

        match raft.role {
            Role::Follower { ref mut leader_seen_ticks, leader_seen_timeout, .. } => {
                *leader_seen_ticks += 1;
                if *leader_seen_ticks >= leader_seen_timeout {
                    raft.become_candidate();
                    let request_vote_replies = raft.solicit_votes();

                    let quorum = raft.quorum();
                    let current_term = raft.current_term;
                    let raft = self.raft.clone();
                    tokio::spawn(async move {
                        Self::count_votes(raft, quorum, current_term, request_vote_replies).await.unwrap();
                    });
                }
            }
            Role::Candidate {ref mut election_ticks, election_timeout, .. } => {
                *election_ticks += 1;
                if *election_ticks >= election_timeout {
                    raft.become_candidate();
                    let request_vote_replies = raft.solicit_votes();

                    let quorum = raft.quorum();
                    let current_term = raft.current_term;
                    let raft = self.raft.clone();
                    tokio::spawn(async move {
                        Self::count_votes(raft, quorum, current_term, request_vote_replies).await.unwrap();
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
        arc_raft: Arc<Mutex<Raft>>, 
        quorum: u64,
        current_term: u64,
        mut request_vote_replies: FuturesUnordered<impl 
            Future<Output = RpcResult<RequestVoteReply>>>,
    ) -> Result<()> {
        let mut vote_count = 1;
        
        // If there is only myself in the cluster, we can become leader immediately.
        if vote_count >= quorum {
            let mut raft = arc_raft.lock()?;
            let work_txs = HashMap::new();
            raft.become_leader(work_txs);
            return Ok(());
        }

        while let Some(res) = request_vote_replies.next().await {
            let (term, vote_granted) = match res {
                Ok(res) => (res.get_ref().term, res.get_ref().vote_granted),
                Err(_) => continue,
            };
            if vote_granted {
                vote_count += 1;
                if vote_count >= quorum {
                    let mut raft = arc_raft.lock()?;
                    let mut work_txs = HashMap::new();
                    for id in 0..raft.peers.len() as u64 {
                        if id == raft.me {
                            continue;
                        }
                        let (work_tx, work_rx) = mpsc::unbounded_channel();
                        work_txs.insert(id, work_tx);
                        tokio::spawn(Self::replicator(arc_raft.clone(), work_rx, id));
                    }
                    raft.become_leader(work_txs);
                    return Ok(());
                }
            }
            if term > current_term {
                arc_raft.lock()?.become_follower(term, None);
                return Ok(());
            }
        }
        Ok(())
    }

    async fn replicator(
        arc_raft: Arc<Mutex<Raft>>,
        mut work_rx: mpsc::UnboundedReceiver<u64>,
        id: u64
    ) -> Result<()> {
        while let Some(log_index) = work_rx.recv().await {
            let raft = arc_raft.lock()?;

            if log_index < raft.log.last_index {
                continue;
            }

            if let Role::Leader { ref next_index, ref work_txs, .. } = raft.role {
                let prev_log_index = next_index.get(&id).unwrap() - 1;
                let prev_log_term = raft.log.get(prev_log_index)?.map_or(0, |e| e.term);
                let entries = raft.log
                    .scan((prev_log_index+1)..=log_index)
                    .collect::<Result<Vec<_>>>()?
                    .into_iter()
                    .map(|e| serialize(&e))
                    .collect::<Result<Vec<_>>>()?;

                let args = AppendEntriesArgs {
                    term: raft.current_term,
                    leader_id: raft.me,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit: raft.commit_index,
                };

                let current_term = raft.current_term;
                let work_tx = work_txs.get(&id).unwrap().clone();
                let mut client = raft.peers[id as usize].clone();
                let raft = arc_raft.clone();
                tokio::spawn(async move {
                    let (term, success) = match client.append_entries(args).await {
                        Ok(res) => (res.get_ref().term, res.get_ref().success),
                        Err(_) => {
                            work_tx.send(log_index).unwrap();
                            return;
                        },
                    };
                    if term > current_term {
                        raft.lock().unwrap().become_follower(term, None);
                        return;
                    }
                    match success {
                        true => {
                            let mut raft = raft.lock().unwrap();
                            let original_commit_index = raft.commit_index;
                            if let Role::Leader { ref mut next_index, ref mut match_index, .. } = raft.role {
                                if log_index > match_index[&id] {
                                    next_index.entry(id).and_modify(|index| *index = log_index + 1);
                                    match_index.entry(id).and_modify(|index| *index = log_index);
                                }

                                // Checks if there are entries ready to be committed.
                                let mut match_indexes = match_index.values().cloned().collect::<Vec<_>>();
                                match_indexes.push(raft.log.last_index);
                                match_indexes.sort_unstable();
                                let mut new_commit_index = match_indexes[raft.quorum() as usize - 1];
                                while let Some(entry) = raft.log.get(new_commit_index).unwrap() {
                                    if entry.term == raft.current_term {
                                        break;
                                    }
                                    new_commit_index -= 1;
                                }
                                if new_commit_index > original_commit_index {
                                    let entries = raft.log
                                        .scan((original_commit_index+1)..=new_commit_index)
                                        .collect::<Result<Vec<_>>>()
                                        .unwrap();
                                    for Entry { index, term, command } in entries {
                                        let apply_msg = ApplyMsg {
                                            log_index: index,
                                            command,
                                        };
                                        raft.apply_tx.send(apply_msg).unwrap();
                                    }
                                    raft.commit_index = new_commit_index;
                                }
                                
                            }
                        },

                        false => {
                            let mut raft = raft.lock().unwrap();
                            if let Role::Leader { ref mut next_index, .. } = raft.role {
                                next_index.entry(id).and_modify(|index| *index -= 1);
                            }
                            work_tx.send(log_index).unwrap();
                        },
                    }
                });
            }
        }

        Ok(())
    }
}

#[tonic::async_trait]
impl RaftService for Node {
    /// RequestVote RPC handler.
    async fn request_vote(
        &self,
        request: Request<RequestVoteArgs>,
    ) -> RpcResult<RequestVoteReply> {
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
    ) -> RpcResult<AppendEntriesReply> {
        let mut raft = self.raft.lock().unwrap();
        let args = request.into_inner();

        if args.term < raft.current_term {
            let reply = AppendEntriesReply {
                term: raft.current_term,
                success: false
            };
            return Ok(Response::new(reply));
        }

        if let Role::Candidate { .. } | Role::Leader { .. } = raft.role {
            raft.become_follower(args.term, None);
        }

        if let Role::Follower { ref mut leader, ref mut leader_seen_ticks, .. } = raft.role {
            *leader_seen_ticks = 0;
            *leader = Some(args.leader_id);
        }

        if args.prev_log_index != 0 && 
            raft.log.get(args.prev_log_index)?.map_or(true, |e| e.term != args.prev_log_term) {
            let reply = AppendEntriesReply {
                term: raft.current_term,
                success: false
            };
            return Ok(Response::new(reply));
        }

        raft.log.splice(args.entries.iter().map(|e| deserialize(e)).collect::<Result<_>>()?)?;

        // Commits entries if necessary.
        if args.leader_commit > raft.commit_index {
            let commit_index = std::cmp::min(args.leader_commit, raft.log.last_index);
            for index in (raft.commit_index + 1)..=commit_index {
                let Entry {index, term, command} = raft.log.get(index)?
                    .ok_or(Error::Internal(format!("Expected entry at index {}", index)))?;
                let apply_msg = ApplyMsg {
                    log_index: index,
                    command,
                };
                match raft.apply_tx.send(apply_msg) {
                    Ok(_) => {},
                    Err(e) => {
                        return Err(Status::internal(format!("Failed to send apply msg: {}", e)));
                    },
                }
            }
            raft.commit_index = commit_index;
            raft.log.commit(commit_index)?;
        }

        let reply = AppendEntriesReply {
            term: raft.current_term,
            success: true
        };
        Ok(Response::new(reply))
    }
}