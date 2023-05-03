mod leader_election;

use std::collections::HashMap;
use std::sync::mpsc;
use std::time::Duration;

use featherdb::error::{Error, Result};
use featherdb::raft::Node;

/// Set up a cluster of `cluster_size` nodes.
async fn setup(cluster_size: u64) -> Result<Cluster> {
    let (tx, rx) = mpsc::channel();
    for i in 0..cluster_size {
        let tx = tx.clone();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async move {
                let node = Node::new(
                    i,
                    // TODO: make this configurable
                    vec![
                        "127.0.0.1:50057".to_string(),
                        "127.0.0.1:50058".to_string(),
                        "127.0.0.1:50059".to_string(),
                    ],
                    todo!(),
                ).await.unwrap();
                tx.send(node.clone()).unwrap();
                drop(tx);
                node.serve().await.unwrap();
            })
        });
    }

    let mut nodes = vec![];
    for _ in 0..cluster_size {
        nodes.push(rx.recv().unwrap());
    }
    nodes.sort_by(|a, b| a.id().unwrap().cmp(&b.id().unwrap()));
    
    Ok(Cluster { nodes })
}

/// A cluster of raft nodes.
pub struct Cluster {
    pub nodes: Vec<Node>,
}

impl Cluster {
    /// Check that there is exactly one leader in the cluster.
    /// Returns the ID of the leader.
    async fn check_one_leader(&self) -> Result<u64> {
        for _ in 0..10 {
            tokio::time::sleep(Duration::from_millis(500)).await;

            let mut termed_leaders = HashMap::<u64, Vec<u64>>::new();
            for node in &self.nodes {
                if node.is_leader()? {
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
}