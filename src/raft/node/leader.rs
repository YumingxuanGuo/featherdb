use crate::error::Result;

use super::RoleNode;



pub struct Leader {

}

impl Leader {
    /// Creates a new leader role.
    pub fn new(peers: Vec<String>, last_index: u64) -> Self {
        todo!()
    }
}

impl RoleNode<Leader> {
    /// Appends an entry to the log and replicates it to peers.
    pub fn append(&mut self, command: Option<Vec<u8>>) -> Result<u64> {
        todo!()
    }
}