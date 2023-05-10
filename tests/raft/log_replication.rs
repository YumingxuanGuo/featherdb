use featherdb::{error::Result, raft::Command};
use super::setup;

#[tokio::test]
async fn test_basic_replication() -> Result<()> {
    let mut cluster = setup(3).await?;
    let leader = cluster.check_one_leader().await?;
    println!("leader: {}", leader);
    cluster.nodes[leader as usize].node.start(Command::Operation { session_id: 0, sequence_number: 0, operation: b"123".to_vec() })?;
    cluster.nodes[leader as usize].node.start(Command::Operation { session_id: 0, sequence_number: 1, operation: b"456".to_vec() })?;
    cluster.nodes[leader as usize].node.start(Command::Operation { session_id: 0, sequence_number: 2, operation: b"789".to_vec() })?;
    for _ in 0..3 {
        for i in 0..3 {
            let raft_node = &mut cluster.nodes[i];
            let apply_msg = raft_node.apply_rx.recv().await.unwrap();
            println!("apply_msg: {:?}", apply_msg);
        }
    }
    Ok(())
}