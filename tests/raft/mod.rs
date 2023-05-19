mod leader_election;
mod log_replication;

use std::collections::HashMap;
use std::time::Duration;

use featherdb::error::{Error, Result};
use featherdb::raft::{Node, ApplyMsg};
use featherdb::storage;
use tokio::sync::mpsc;

/// Set up a cluster of `cluster_size` nodes.
async fn setup(cluster_size: u64) -> Result<Cluster> {
    let (node_tx, node_rx) = std::sync::mpsc::channel();
    let (apply_ch_tx, apply_ch_rx) = std::sync::mpsc::channel();
    for i in 0..cluster_size {
        let node_tx = node_tx.clone();
        let apply_ch_tx = apply_ch_tx.clone();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async move {
                let (apply_tx, apply_rx) = mpsc::unbounded_channel();
                let node = Node::new(
                    i,
                    // TODO: make this configurable
                    vec![
                        "127.0.0.1:50057".to_string(),
                        "127.0.0.1:50058".to_string(),
                        "127.0.0.1:50059".to_string(),
                    ],
                    apply_tx,
                    Box::new(storage::log::Memory::new()),
                ).await.unwrap();
                node_tx.send(node.clone()).unwrap();
                drop(node_tx);
                apply_ch_tx.send(apply_rx).unwrap();
                drop(apply_ch_tx);
                node.serve().await.unwrap();
            })
        });
    }

    let mut nodes = vec![];
    for _ in 0..cluster_size {
        let node = node_rx.recv().unwrap();
        let apply_rx = apply_ch_rx.recv().unwrap();
        nodes.push(RaftNode { node, apply_rx });
    }
    nodes.sort_by(|a, b| a.node.id().unwrap().cmp(&b.node.id().unwrap()));
    
    Ok(Cluster { nodes })
}

/// A cluster of raft nodes.
pub struct Cluster {
    pub nodes: Vec<RaftNode>,
}

/// A raft node with a simulated state machine channel.
pub struct RaftNode {
    pub node: Node,
    pub apply_rx: mpsc::UnboundedReceiver<ApplyMsg>,
}

impl Cluster {
    /// Check that there is exactly one leader in the cluster.
    /// Returns the ID of the leader.
    async fn check_one_leader(&self) -> Result<u64> {
        for _ in 0..10 {
            tokio::time::sleep(Duration::from_millis(500)).await;

            let mut termed_leaders = HashMap::<u64, Vec<u64>>::new();
            for RaftNode {node, apply_rx: _} in &self.nodes {
                if node.is_leader()? {
                    println!("node {} is leader", node.id()?);
                    termed_leaders.entry(node.term()?).or_default().push(node.id()?);
                }
            }

            if termed_leaders.is_empty() {
                continue;
            }

            let mut last_leader_term = 0;
            let mut last_leader = 0;
            for (term, leaders) in termed_leaders {
                if leaders.len() > 1 {
                    return Err(Error::Internal(format!(
                        "Term {} has multiple ({}) leaders: {:?}",
                        term, leaders.len(), leaders
                    )));
                }
                if term > last_leader_term {
                    last_leader_term = term;
                    last_leader = leaders[0];
                }
            }

            return Ok(last_leader);
        }
        
        Err(Error::Internal("Expected one leader, got none.".to_string()))
    }

    // async fn check_consensus(&self) -> Result<()> {


    //     Ok(())
    // }
}