use crate::common::{PageID, PAGE_SIZE, rid::RID};
use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::storage::page::b_plus_tree_page::BPlusTreePageTraits;
use crate::storage::page::b_plus_tree_leaf_page::BPlusTreeLeafPage;
use crate::storage::page::b_plus_tree_internal_page::BPlusTreeInternalPage;

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
    pub fn new(index_name: String, buffer_pool_manager: BufferPoolManager, 
                leaf_max_size: usize, internal_max_size: usize) -> Self {
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
    fn insert(&mut self, key: KeyType, value: ValueType) -> crate::error::Result<()> {
        Ok(())
    }

    fn remove() -> crate::error::Result<()> {
        Ok(())
    }

    fn get_value(&mut self, key: &KeyType) -> Option<ValueType> {
        unsafe {
            let leaf_page_id: PageID = self.find_leaf_page_id(key);
            let leaf_page_ptr =
                self.fetch_page_as_const_ptr::<BPlusTreeLeafPage>(leaf_page_id);
            let leaf_page = leaf_page_ptr.as_ref()
                .expect("raw-pointer to reference failed");
            for (k, v) in leaf_page.array {
                if *key == k {
                    return Some(v);
                }
            }
        }
        return None;
    }
}

impl BPlusTree {
    unsafe fn find_leaf_page_id(&mut self, key: &KeyType) -> PageID {
        let tree_page_ptr =
            self.fetch_page_as_const_ptr::<BPlusTreeInternalPage>(self.root_page_id);
        let mut tree_page = tree_page_ptr.as_ref()
            .expect("raw-pointer to reference failed");
        while !tree_page.is_leaf_page() {
            // brute force search; use binary search later
            if *key < tree_page.array[1].0 {
                let tree_page_ptr =
                    self.fetch_page_as_const_ptr::<BPlusTreeInternalPage>(tree_page.array[0].1);
                tree_page = tree_page_ptr.as_ref().expect("raw-pointer to reference failed");
            } else {
                for i in 1..tree_page.get_size() {
                    let left_key = tree_page.array[i as usize].0;
                    let right_key = tree_page.array[(i+1) as usize].0;
                    if left_key <= *key && *key < right_key {
                        let tree_page_ptr =
                            self.fetch_page_as_const_ptr::<BPlusTreeInternalPage>(tree_page.array[i as usize].1);
                        tree_page = tree_page_ptr.as_ref().expect("raw-pointer to reference failed");
                        break;
                    }
                }
                let last_key_index: usize = tree_page.get_size() as usize;
                if *key >= tree_page.array[last_key_index].0 {
                    let tree_page_ptr =
                        self.fetch_page_as_const_ptr::<BPlusTreeInternalPage>(tree_page.array[last_key_index].1);
                    tree_page = tree_page_ptr.as_ref().expect("raw-pointer to reference failed");
                }
            }
        }
        assert!(tree_page.is_leaf_page());
        return tree_page.get_page_id();
    }
    
    unsafe fn fetch_page_as_const_ptr<Dst>(&mut self, page_id: PageID) -> *const Dst {
        let page = self.buffer_pool_manager.fetch_page(page_id)
            .expect("fetch failed");
        let data_ptr: *const [u8; PAGE_SIZE] = &page.data;
        let tree_page_ptr: *const Dst = data_ptr.cast::<Dst>();
        return tree_page_ptr;
    }

    unsafe fn fetch_page_as_mut_ptr<Dst>(&mut self, page_id: PageID) -> *mut Dst {
        let page = self.buffer_pool_manager.fetch_page(page_id)
            .expect("fetch failed");
        let data_ptr: *mut [u8; PAGE_SIZE] = &mut page.data;
        let tree_page_ptr: *mut Dst = data_ptr.cast::<Dst>();
        return tree_page_ptr;
    }
}