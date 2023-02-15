use std::{mem};

use crate::{common::{PageID, PAGE_SIZE, INVALID_PAGE_ID, rid::RID}, buffer::buffer_pool_manager::BufferPoolManager, storage::{disk::disk_manager::DiskManager, page::{b_plus_tree_page::{BPlusTreePage, BPlusTreePageTraits, LeafPage}, b_plus_tree_leaf_page::BPlusTreeLeafPage, page::Page, b_plus_tree_internal_page::BPlusTreeInternalPage}}};

use super::Store;

type KeyType = i32;
type ValueType = RID;

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
    pub buffer_pool_manager: BufferPoolManager,
    // comparator
    leaf_max_size: usize,
    internal_max_size: usize,
}

impl BPlusTree {
    pub fn new(index_name: String, buffer_pool_manager: BufferPoolManager, leaf_max_size: usize, internal_max_size: usize) -> Self {
        Self {
            index_name,
            root_page_id: 0,
            buffer_pool_manager,
            leaf_max_size,
            internal_max_size,
        }
    }

    pub fn get_root_page_id(&self) -> PageID {
        return self.root_page_id;
    }
}

impl Store for BPlusTree {
    fn insert(&mut self, key: &String, value: &Vec<u8>) -> crate::error::Result<()> {
        let page_id = self.get_root_page_id();
        let page = self.buffer_pool_manager.fetch_page(page_id);
        if page.is_none() {
            return Ok(())
        }
        let page = page.unwrap();
        let data_ptr: *mut [u8; PAGE_SIZE] = &mut page.data;

        unsafe {
            let leaf_page = data_ptr.cast::<BPlusTreeLeafPage>();
        };
        Ok(())
    }

    fn remove() -> crate::error::Result<()> {
        Ok(())
    }

    fn get_value(&mut self, key: KeyType) -> Option<ValueType> {
        unsafe {
            let page = self.buffer_pool_manager.fetch_page(self.root_page_id).expect("fetch failed");
            let data_ptr: *const [u8; PAGE_SIZE] = &page.data;
            let tree_page_ptr: *const BPlusTreeInternalPage = data_ptr.cast::<BPlusTreeInternalPage>();
            let mut tree_page = tree_page_ptr.as_ref().unwrap();
            while !tree_page.is_leaf_page() {
                // brute force search; use binary search later
                if key < tree_page.array[1].0 {
                    let page = self.buffer_pool_manager.fetch_page(tree_page.array[0].1).expect("fetch failed");
                    let data_ptr: *const [u8; PAGE_SIZE] = &page.data;
                    let tree_page_ptr: *const BPlusTreeInternalPage = data_ptr.cast::<BPlusTreeInternalPage>();
                    tree_page = tree_page_ptr.as_ref().unwrap();
                } else {
                    for i in 1..tree_page.get_size() {
                        let left_key = tree_page.array[i as usize].0;
                        let right_key = tree_page.array[(i+1) as usize].0;
                        if left_key <= key && key < right_key {
                            let page = self.buffer_pool_manager.fetch_page(tree_page.array[i as usize].1).expect("fetch failed");
                            let data_ptr: *const [u8; PAGE_SIZE] = &page.data;
                            let tree_page_ptr: *const BPlusTreeInternalPage = data_ptr.cast::<BPlusTreeInternalPage>();
                            tree_page = tree_page_ptr.as_ref().unwrap();
                            break;
                        }
                    }
                    let last_key_index: usize = tree_page.get_size() as usize;
                    if key >= tree_page.array[last_key_index].0 {
                        let page = self.buffer_pool_manager.fetch_page(tree_page.array[last_key_index].1).expect("fetch failed");
                        let data_ptr: *const [u8; PAGE_SIZE] = &page.data;
                        let tree_page_ptr: *const BPlusTreeInternalPage = data_ptr.cast::<BPlusTreeInternalPage>();
                        tree_page = tree_page_ptr.as_ref().unwrap();
                    }
                }
            }
            assert!(tree_page.is_leaf_page());
            let tree_page_ptr: *const BPlusTreeInternalPage = tree_page;
            let leaf_page_ptr: *const BPlusTreeLeafPage = tree_page_ptr.cast::<BPlusTreeLeafPage>();
            let tree_page = leaf_page_ptr.as_ref().unwrap();
            for (k, v) in tree_page.array {
                if key == k {
                    return Some(v);
                }
            }
        }
        return None;
    }
}