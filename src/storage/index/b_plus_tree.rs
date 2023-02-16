use crate::common::{PageID, PAGE_SIZE, INVALID_PAGE_ID, rid::RID};
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
        unsafe {
            let leaf_page_id: PageID = self.find_leaf_page_id(&key);
            let leaf_page_ptr =
                self.fetch_page_as_mut_ptr::<BPlusTreeLeafPage>(leaf_page_id);
            let leaf_page = leaf_page_ptr.as_mut()
                .expect("raw-pointer to reference failed");

            if leaf_page.get_size() < leaf_page.get_max_size() {
                return self.insert_in_leaf(key, value, leaf_page);
            }

            // Create node L′
            let mut new_page_id: PageID = INVALID_PAGE_ID;
            let new_page = self.buffer_pool_manager.new_page(&mut new_page_id)
                .expect("fetch failed");
            let data_ptr: *mut [u8; PAGE_SIZE] = &mut new_page.data;
            let tree_page_ptr: *mut BPlusTreeLeafPage = data_ptr.cast::<BPlusTreeLeafPage>();
            let new_leaf_page = tree_page_ptr.as_mut().expect("cast failed");

            // Copy L.P1 ... L.Kn−1 to a block of memory T that can hold n (pointer, key-value) pairs, insert in it
            let mut temp_memory = vec![(KeyType::default(), ValueType{page_id: -1, slot_num: 0}); (leaf_page.get_size()+1) as usize];
            let pos = self.find_new_key_position::<BPlusTreeLeafPage>(key, leaf_page);
            for i in 0..pos {
                temp_memory[i as usize] = leaf_page.array[i as usize];
            }
            temp_memory[pos as usize] = (key, value);
            for i in (pos+1)..(leaf_page.get_size()+1) {
                temp_memory[i as usize] = leaf_page.array[i as usize];
            }

            // Set L′.Pn = L.Pn; Set L.Pn = L′
            new_leaf_page.set_next_page_id(leaf_page.get_next_page_id());
            leaf_page.set_next_page_id(new_page_id);

            // Erase L.P1 through L.Kn−1 from L
            // Copy T.P1 through T.K⌈n∕2⌉ from T into L starting at L.P1 
            let mid = (leaf_page.get_size() + 1) / 2;
            for i in 0..mid {
                leaf_page.array[i as usize] = temp_memory[i as usize];
            }

            // Copy T.P⌈n∕2⌉+1 through T.Kn from T into L′ starting at L′.P1 
            for i in mid..leaf_page.get_size() {
                new_leaf_page.array[(i-mid) as usize] = temp_memory[i as usize];
            }

            // Let K′ be the smallest key-value in L′
            // insert in parent(L, K′, L′)
            self.insert_in_parent(new_leaf_page.key_at(0), leaf_page_id, new_page_id, leaf_page.get_parent_page_id())
        }
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
            for i in 0..leaf_page.get_size() {
                if leaf_page.key_at(i) == *key {
                    let value = Some(leaf_page.value_at(i));
                    self.buffer_pool_manager.unpin_page(leaf_page_id, false);
                    return value;
                }
            }
            self.buffer_pool_manager.unpin_page(leaf_page_id, false);
            return None;
        }
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
            if *key < tree_page.key_at(1) {
                let tree_page_ptr =
                    self.fetch_page_as_const_ptr::<BPlusTreeInternalPage>(tree_page.value_at(0));
                tree_page = tree_page_ptr.as_ref().expect("raw-pointer to reference failed");
            } else if *key >= tree_page.key_at(tree_page.get_size()) {
                let tree_page_ptr =
                    self.fetch_page_as_const_ptr::<BPlusTreeInternalPage>(tree_page.value_at(tree_page.get_size()));
                tree_page = tree_page_ptr.as_ref().expect("raw-pointer to reference failed");
            } else {
                for i in 1..tree_page.get_size() {
                    let left_key: KeyType = tree_page.key_at(i);
                    let right_key: KeyType = tree_page.key_at(i+1);
                    if left_key <= *key && *key < right_key {
                        let tree_page_ptr =
                            self.fetch_page_as_const_ptr::<BPlusTreeInternalPage>(tree_page.value_at(i));
                        tree_page = tree_page_ptr.as_ref().expect("raw-pointer to reference failed");
                        break;
                    }
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

    fn find_new_key_position<PageType: BPlusTreePageTraits>(&self, key: KeyType, page: &PageType) -> i32 {
        let mut pos: i32 = -1;
        if key < page.key_at(0) {
            pos = 0;
        } else if key >= page.key_at(page.get_size()) {
            pos = page.get_size() + 1;
        } else {
            for i in 1..page.get_size() {
                let left_key: KeyType = page.key_at(i);
                let right_key: KeyType = page.key_at(i+1);
                if left_key <= key && key < right_key {
                    pos = i;
                    break;
                }
            }
        }
        return pos;
    }

    fn insert_in_leaf(&mut self, key: KeyType, value: ValueType, leaf_page: &mut BPlusTreeLeafPage) -> crate::error::Result<()> {
        let pos = self.find_new_key_position::<BPlusTreeLeafPage>(key, leaf_page);
        assert_ne!(pos, -1);
        for i in (pos..leaf_page.get_size()).rev() {
            leaf_page.array[(i+1) as usize] = leaf_page.array[i as usize];
        }
        leaf_page.array[pos as usize] = (key, value);
        Ok(())
    }

    fn insert_in_internal(&mut self, key: KeyType, value: PageID, internal_page: &mut BPlusTreeInternalPage) -> crate::error::Result<()> {
        let pos = self.find_new_key_position::<BPlusTreeInternalPage>(key, internal_page);
        assert_ne!(pos, -1);
        for i in (pos..internal_page.get_size()).rev() {
            internal_page.array[(i+1) as usize] = internal_page.array[i as usize];
        }
        internal_page.array[pos as usize] = (key, value);
        Ok(())
    }

    unsafe fn insert_in_parent(&mut self, key: KeyType, left_page_id: PageID, right_page_id: PageID, parent_page_id: PageID) -> crate::error::Result<()> {
        // current node is root
        if parent_page_id == INVALID_PAGE_ID {
            let mut new_root_page_id: PageID = INVALID_PAGE_ID;
            let new_page = self.buffer_pool_manager.new_page(&mut new_root_page_id)
                .expect("fetch failed");
            let data_ptr: *mut [u8; PAGE_SIZE] = &mut new_page.data;
            let tree_page_ptr: *mut BPlusTreeInternalPage = data_ptr.cast::<BPlusTreeInternalPage>();
            let new_root_page = tree_page_ptr.as_mut().expect("cast failed");

            let left_page_ptr =
                self.fetch_page_as_mut_ptr::<BPlusTreeInternalPage>(left_page_id);
            let left_page = left_page_ptr.as_mut()
                .expect("raw-pointer to reference failed");
            left_page.set_parent_page_id(new_root_page_id);

            let right_page_ptr =
                self.fetch_page_as_mut_ptr::<BPlusTreeInternalPage>(right_page_id);
            let right_page = right_page_ptr.as_mut()
                .expect("raw-pointer to reference failed");
            right_page.set_parent_page_id(new_root_page_id);

            new_root_page.set_key_at(1, &right_page.key_at(1));

            return Ok(());
        }

        let parent_page_ptr =
                self.fetch_page_as_mut_ptr::<BPlusTreeInternalPage>(parent_page_id);
        let parent_page = parent_page_ptr.as_mut()
            .expect("raw-pointer to reference failed");

        if parent_page.get_size() < parent_page.get_max_size()-1 {
            return self.insert_in_internal(key, right_page_id, parent_page);
        }

        // Create node L′
        let mut new_page_id: PageID = INVALID_PAGE_ID;
        let new_page = self.buffer_pool_manager.new_page(&mut new_page_id)
            .expect("fetch failed");
        let data_ptr: *mut [u8; PAGE_SIZE] = &mut new_page.data;
        let tree_page_ptr: *mut BPlusTreeInternalPage = data_ptr.cast::<BPlusTreeInternalPage>();
        let new_parent_page = tree_page_ptr.as_mut().expect("cast failed");

        // Copy L.P1 ... L.Kn−1 to a block of memory T that can hold n (pointer, key-value) pairs, insert in it
        let mut temp_memory = vec![(KeyType::default(), PageID::default()); (parent_page.get_size()+1) as usize];
        let pos = self.find_new_key_position::<BPlusTreeInternalPage>(key, parent_page);
        for i in 0..pos {
            temp_memory[i as usize] = parent_page.array[i as usize];
        }
        temp_memory[pos as usize] = (key, new_page_id);
        for i in (pos+1)..(parent_page.get_size()+1) {
            temp_memory[i as usize] = parent_page.array[i as usize];
        }

        // Erase L.P1 through L.Kn−1 from L
        // Copy T.P1 through T.K⌈n∕2⌉ from T into L starting at L.P1 
        let mid = (parent_page.get_size() + 1) / 2;
        for i in 1..mid {
            parent_page.array[i as usize] = temp_memory[i as usize];
        }

        let (mid_key, _) = temp_memory[mid as usize];

        // Copy T.P⌈n∕2⌉+1 through T.Kn from T into L′ starting at L′.P1 
        for i in (mid+1)..parent_page.get_size() {
            new_parent_page.array[(i-mid) as usize] = temp_memory[i as usize];
        }

        self.insert_in_parent(mid_key, parent_page_id, new_page_id, parent_page.get_parent_page_id())
    }
}