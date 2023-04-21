use featherdb::error::Result;
use super::setup;

#[tokio::test]
async fn test_initial_election() -> Result<()> {
    let cluster = setup(3).await?;
    cluster.check_one_leader().await?;
    // TODO: check that the leader remains unchanged without failures
    Ok(())
}