use std::mem;

use crate::{common::{PageID, PAGE_SIZE}, buffer::buffer_pool_manager::BufferPoolManager, storage::{disk::disk_manager::DiskManager, page::{b_plus_tree_page::BPlusTreePage, b_plus_tree_leaf_page::BPlusTreeLeafPage}}};

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
pub struct BPlusTree<K, V, const N: usize> {
    index_name: String,
    root_page_id: PageID,
    buffer_pool_manager: BufferPoolManager,
    // comparator
    leaf_max_size: usize,
    internal_max_size: usize,
    k: std::marker::PhantomData<K>,
    v: std::marker::PhantomData<V>,
}

impl<K, V, const N: usize> BPlusTree<K, V, N> {
    pub fn new() -> Self {
        Self {
            index_name: "index".to_string(),
            root_page_id: 0,
            buffer_pool_manager: BufferPoolManager::new(4, DiskManager::new(), 4),
            leaf_max_size: 0,
            internal_max_size: 0,
            k: std::marker::PhantomData,
            v: std::marker::PhantomData
        }
    }

    pub fn get_root_page_id(&self) -> PageID {
        return self.root_page_id;
    }
}

impl<K, V, const N: usize> Store for BPlusTree<K, V, N> {
    fn insert(&mut self, key: &String, value: &Vec<u8>) -> crate::error::Result<()> {
        let page_id = self.get_root_page_id();
        let page = self.buffer_pool_manager.fetch_page(page_id);
        if page.is_none() {
            return Ok(())
        }
        let page = page.unwrap();
        let data_ptr: *mut [u8; PAGE_SIZE] = &mut page.data;

        const LEAF_HEADER_SIZE: usize = mem::size_of::<BPlusTreePage>() + mem::size_of::<PageID>();
        const LEAF_DATA_SIZE: usize = PAGE_SIZE - LEAF_HEADER_SIZE;
        // const MAX_SIZE: usize = LEAF_DATA_SIZE / N;

        unsafe {
            let leaf_page = data_ptr.cast::<BPlusTreeLeafPage>();
        };
        Ok(())
    }

    fn remove() -> crate::error::Result<()> {
        Ok(())
    }

    fn get_value() -> crate::error::Result<Option<Vec<u8>>> {
        Ok(None)
    }
}