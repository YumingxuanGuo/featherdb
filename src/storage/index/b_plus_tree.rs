use crate::{common::PageID, buffer::buffer_pool_manager::BufferPoolManager, storage::disk::disk_manager::DiskManager};

use super::Store;



/**
 * Main class providing the API for the Interactive B+ Tree.
 *
 * Implementation of simple b+ tree data structure where internal pages direct
 * the search and leaf pages contain actual data.
 * (1) We only support unique key
 * (2) support insert & remove
 * (3) The structure should shrink and grow dynamically
 * (4) Implement index iterator for range scan
 */
pub struct BPlusTree {
    index_name: String,
    root_page_id: PageID,
    buffer_pool_manager: BufferPoolManager,
    // comparator
    leaf_max_size: usize,
    internal_max_size: usize,
}

impl BPlusTree {
    pub fn new() -> Self {
        Self {
            index_name: "index".to_string(),
            root_page_id: 0,
            buffer_pool_manager: BufferPoolManager::new(4, DiskManager::new(), 4),
            leaf_max_size: 0,
            internal_max_size: 0,
        }
    }
}

impl Store for BPlusTree {
    fn insert(&self, key: &String, value: &Vec<u8>) -> crate::error::Result<()> {
        
        Ok(())
    }

    fn remove() -> crate::error::Result<()> {
        Ok(())
    }

    fn get_value() -> crate::error::Result<Option<Vec<u8>>> {
        Ok(None)
    }
}