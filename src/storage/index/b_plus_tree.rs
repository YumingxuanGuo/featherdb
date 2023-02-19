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
    leaf_max_size: i32,
    internal_max_size: i32,
    
    leftmost_leaf_page_id: PageID,
    it_ptr: ItPtr,
}

impl BPlusTree {
    pub fn new(index_name: String, buffer_pool_manager: BufferPoolManager, 
                leaf_max_size: i32, internal_max_size: i32) -> Self {
        Self {
            index_name,
            root_page_id: INVALID_PAGE_ID,
            buffer_pool_manager,
            leaf_max_size,
            internal_max_size,
            leftmost_leaf_page_id: INVALID_PAGE_ID,
            it_ptr: ItPtr { cur_page_id: INVALID_PAGE_ID, cur_index: -1 }
        }
    }

    pub fn get_root_page_id(&self) -> PageID {
        return self.root_page_id;
    }
}

impl Store for BPlusTree {
    fn insert(&mut self, key: KeyType, value: ValueType) -> crate::error::Result<()> {
        unsafe {
            println!("inserting ({key}, {:?})...", value);

            // if inserting into an empty tree
            if self.get_root_page_id() == INVALID_PAGE_ID {
                println!("inserting into empty tree...");

                // create a leaf node as root
                let mut new_leaf_page_id: PageID = INVALID_PAGE_ID;
                let new_page = self.buffer_pool_manager.new_page(&mut new_leaf_page_id)
                    .expect("fetch failed");
                let leaf_data_ptr: *mut [u8; PAGE_SIZE] = &mut new_page.data;
                let leaf_page_ptr: *mut BPlusTreeLeafPage = leaf_data_ptr.cast::<BPlusTreeLeafPage>();
                let new_leaf_page = leaf_page_ptr.as_mut().expect("cast failed");

                // set root's corresponding fields
                new_leaf_page.init(new_leaf_page_id, INVALID_PAGE_ID, self.leaf_max_size);

                // update the tree's root's page_id
                self.root_page_id = new_leaf_page_id;

                // set the tree's leftmost leaf's page_id
                self.leftmost_leaf_page_id = new_leaf_page_id;

                // insert in the root/leaf
                new_leaf_page.array[0] = (key, value);
                new_leaf_page.increase_size(1);

                // unpin the page
                self.buffer_pool_manager.unpin_page(new_leaf_page_id, true);

                println!("leaf insertion succeeded!\n");
                return Ok(());
            }

            // find the leaf page for insertion
            let leaf_page_id: PageID = self.find_leaf_page_id(&key);
            let leaf_page_ptr =
                self.fetch_page_as_mut_ptr::<BPlusTreeLeafPage>(leaf_page_id);
            let leaf_page = leaf_page_ptr.as_mut()
                .expect("raw-pointer to reference failed");

            // if the leaf is not full, simply insert in it
            if leaf_page.get_size() < leaf_page.get_max_size() {
                return self.insert_in_leaf(key, value, leaf_page);
            }

            // the leaf is full, need spliting

            // create a new leaf page
            let mut new_leaf_page_id: PageID = INVALID_PAGE_ID;
            let new_page = self.buffer_pool_manager.new_page(&mut new_leaf_page_id)
                .expect("fetch failed");
            let data_ptr: *mut [u8; PAGE_SIZE] = &mut new_page.data;
            let new_leaf_page_ptr: *mut BPlusTreeLeafPage = data_ptr.cast::<BPlusTreeLeafPage>();
            let new_leaf_page = new_leaf_page_ptr.as_mut().expect("cast failed");

            // debug output
            println!("leaf {} is full; created new leaf {}", leaf_page.get_page_id(), new_leaf_page_id);

            // set the leaf's corresponding fields
            new_leaf_page.init(new_leaf_page_id, leaf_page.get_parent_page_id(), self.leaf_max_size);


            // copy all k-v pairs into a temp vec, including the one to insert
            let total_size = leaf_page.get_size() + 1;
            let mut temp_memory =
                vec![(KeyType::default(), ValueType{page_id: INVALID_PAGE_ID, slot_num: 0}); total_size as usize];
            let pos = self.find_new_key_position::<BPlusTreeLeafPage>(key, leaf_page);
            for i in 0..pos {
                temp_memory[i as usize] = leaf_page.array[i as usize];
            }
            temp_memory[pos as usize] = (key, value);
            for i in (pos+1)..(leaf_page.get_size() + 1) {
                temp_memory[i as usize] = leaf_page.array[(i-1) as usize];
            }

            // copy the first half (floor) to the original leaf
            let mid = total_size / 2;
            for i in 0..mid {
                leaf_page.array[i as usize] = temp_memory[i as usize];
            }

            // copy the second half (ceiling) to the new leaf
            for i in mid..total_size {
                new_leaf_page.array[(i-mid) as usize] = temp_memory[i as usize];
            }

            // set size bound for the k-v pairs that are still in use
            // instead of erasing them
            leaf_page.set_size(mid);
            new_leaf_page.set_size(total_size - mid);

            // debug outputs
            print!("leaf {}: {{ ", leaf_page.get_page_id());
            for i in 0..leaf_page.get_size() {
                print!("{} ", leaf_page.key_at(i));
            }
            println!("}}, parent = {}", new_leaf_page.get_parent_page_id());

            print!("leaf {}: {{ ", new_leaf_page.get_page_id());
            for i in 0..new_leaf_page.get_size() {
                print!("{} ", new_leaf_page.key_at(i));
            }
            println!("}}, parent = {}", new_leaf_page.get_parent_page_id());

            // set the neighbor pointers
            new_leaf_page.set_next_page_id(leaf_page.get_next_page_id());
            leaf_page.set_next_page_id(new_leaf_page_id);

            // insert the smallest key in the new leaf (second half) to the parent
            let up_key = new_leaf_page.key_at(0);
            let parent_page_id = leaf_page.get_parent_page_id();

            // unpin the pages
            self.buffer_pool_manager.unpin_page(leaf_page_id, true);
            self.buffer_pool_manager.unpin_page(new_leaf_page_id, true);

            self.insert_in_parent(up_key, leaf_page_id, new_leaf_page_id, parent_page_id)
        }
    }

    fn remove() -> crate::error::Result<()> {
        Ok(())
    }

    fn get_value(&mut self, key: &KeyType) -> Option<ValueType> {
        unsafe {
            println!("searching ({key})...");

            // if searching in an empty tree
            if self.get_root_page_id() == INVALID_PAGE_ID {
                return None;
            }

            // fetch the leaf where the search key is located in
            let leaf_page_id: PageID = self.find_leaf_page_id(key);
            let leaf_page_ptr =
                self.fetch_page_as_const_ptr::<BPlusTreeLeafPage>(leaf_page_id);
            let leaf_page = leaf_page_ptr.as_ref()
                .expect("raw-pointer to reference failed");

            // debug output
            println!("{leaf_page_id}, parent = {}", leaf_page.get_parent_page_id());

            // use brute force to find the k-v pair
            for i in 0..leaf_page.get_size() {
                if leaf_page.key_at(i) == *key {
                    let value = Some(leaf_page.value_at(i));

                    // unpin the page
                    self.buffer_pool_manager.unpin_page(leaf_page_id, false);

                    println!("key {key} found at leaf {leaf_page_id}, pos {i}\n");

                    return value;
                }
            }

            // unpin the page
            self.buffer_pool_manager.unpin_page(leaf_page_id, false);
            println!("key {key} does not exist!\n");

            // key not found
            return None;
        }
    }
}



/**
 * Helper functions for BPlusTree.
 * 
 * 
 *    find_leaf_page_id(&mut self, key: &KeyType) -> PageID
 * 
 *    fetch_page_as_const_ptr<Dst>(&mut self, page_id: PageID) -> *const Dst
 * 
 *    fetch_page_as_mut_ptr<Dst>(&mut self, page_id: PageID) -> *mut Dst
 * 
 *    find_new_key_position<PageType: BPlusTreePageTraits>(&self, key: KeyType, page: &PageType) -> i32
 * 
 *    insert_in_leaf(&mut self, key: KeyType, value: ValueType, 
 *                  leaf_page: &mut BPlusTreeLeafPage) -> crate::error::Result<()>
 *
 *    insert_in_internal(&mut self, key: KeyType, value: PageID, 
 *                      internal_page: &mut BPlusTreeInternalPage) -> crate::error::Result<()>
 *                      
 *    insert_in_parent(&mut self, key: KeyType, left_page_id: PageID, 
 *                      right_page_id: PageID, parent_page_id: PageID) -> crate::error::Result<()>
 */
impl BPlusTree {

    /**
     * Find the leaf's page_id that the key is supposed to locate in.
     */
    unsafe fn find_leaf_page_id(&mut self, key: &KeyType) -> PageID {

        // fetch and start from the root page
        let mut tree_page_id = self.get_root_page_id();
        let tree_page_ptr =
            self.fetch_page_as_const_ptr::<BPlusTreeInternalPage>(tree_page_id);
        let mut tree_page = tree_page_ptr.as_ref()
            .expect("raw-pointer to reference failed");

        println!("reached {tree_page_id}");

        // keep searching until the current page is a leaf
        // due to the field set-ups, is_leaf_page() works on both internal and leaf pages
        while !tree_page.is_leaf_page() {
            // brute force search; use binary search later
            if *key < tree_page.key_at(1) {
                // check the head position
                // unpin the current page
                self.buffer_pool_manager.unpin_page(tree_page_id, false);
                tree_page_id = tree_page.value_at(0);
                let tree_page_ptr =
                    self.fetch_page_as_const_ptr::<BPlusTreeInternalPage>(tree_page_id);
                tree_page = tree_page_ptr.as_ref().expect("raw-pointer to reference failed");
                println!("reached {tree_page_id}");
            } else if *key >= tree_page.key_at(tree_page.get_size() - 1) {
                // check the tail position
                // unpin the current page
                self.buffer_pool_manager.unpin_page(tree_page_id, false);
                tree_page_id = tree_page.value_at(tree_page.get_size() - 1);
                let tree_page_ptr =
                    self.fetch_page_as_const_ptr::<BPlusTreeInternalPage>(tree_page_id);
                tree_page = tree_page_ptr.as_ref().expect("raw-pointer to reference failed");
                println!("reached {tree_page_id}");
            } else {
                // check the middle positions
                // brute force; use binary search later
                for i in 1..(tree_page.get_size() - 1) {
                    let left_key: KeyType = tree_page.key_at(i);
                    let right_key: KeyType = tree_page.key_at(i+1);
                    if left_key <= *key && *key < right_key {
                        // unpin the current page
                        self.buffer_pool_manager.unpin_page(tree_page_id, false);
                        tree_page_id = tree_page.value_at(i);
                        let tree_page_ptr =
                            self.fetch_page_as_const_ptr::<BPlusTreeInternalPage>(tree_page_id);
                        tree_page = tree_page_ptr.as_ref().expect("raw-pointer to reference failed");
                        println!("reached {tree_page_id}");
                        break;
                    }
                }
            }
        }

        // unpin the page
        self.buffer_pool_manager.unpin_page(tree_page_id, false);
        return tree_page_id;
    }
    
    /**
     * Fetch the target page using BufferPoolManager, then
     * return the page a const raw-pointer of the @Dst type.
     */
    unsafe fn fetch_page_as_const_ptr<Dst>(&mut self, page_id: PageID) -> *const Dst {
        let page = self.buffer_pool_manager.fetch_page(page_id)
            .expect("fetch failed");
        let data_ptr: *const [u8; PAGE_SIZE] = &page.data;
        let tree_page_ptr: *const Dst = data_ptr.cast::<Dst>();
        return tree_page_ptr;
    }

    /**
     * Fetch the target page using BufferPoolManager, then
     * return the page a mutable raw-pointer of the @Dst type.
     */
    unsafe fn fetch_page_as_mut_ptr<Dst>(&mut self, page_id: PageID) -> *mut Dst {
        let page = self.buffer_pool_manager.fetch_page(page_id)
            .expect("fetch failed");
        let data_ptr: *mut [u8; PAGE_SIZE] = &mut page.data;
        let tree_page_ptr: *mut Dst = data_ptr.cast::<Dst>();
        return tree_page_ptr;
    }

    /**
     * Locate the correct offset to insert the key in the given page.
     * 
     * Example: 
     *      `pos = i` means the key shoule be inserted between [i-1] and [i];
     *      that is, it should be located at [i] after the insertion.
     */
    fn find_new_key_position<PageType: BPlusTreePageTraits>(&self, key: KeyType, page: &PageType) -> i32 {
        assert!(page.get_size() > 0);
        let mut pos: i32 = -1;
        let start_pos: i32;

        // internal pages has their first meaningful k-v pair at [1]
        // while leaf pages at [0]
        if page.is_leaf_page() {
            start_pos = 0;
        } else {
            start_pos = 1;
        }

        if key < page.key_at(start_pos) {
            pos = start_pos;
        } else if key >= page.key_at(page.get_size() - 1) {
            pos = page.get_size();
        } else {
            for i in start_pos..page.get_size() {
                let left_key: KeyType = page.key_at(i);
                let right_key: KeyType = page.key_at(i+1);
                if left_key <= key && key < right_key {
                    pos = i + 1;
                    break;
                }
            }
        }
        return pos;
    }

    /**
     * Insert the k-v pair in the correct position of the given leaf page.
     * Assumes the leaf page is not full.
     */
    fn insert_in_leaf(&mut self, key: KeyType, value: ValueType, 
                        leaf_page: &mut BPlusTreeLeafPage) -> crate::error::Result<()> {

        let pos = self.find_new_key_position::<BPlusTreeLeafPage>(key, leaf_page);
        assert_ne!(pos, -1);
        assert!(leaf_page.get_size() < leaf_page.get_max_size());
        for i in (pos..leaf_page.get_size()).rev() {
            leaf_page.array[(i+1) as usize] = leaf_page.array[i as usize];
        }
        leaf_page.array[pos as usize] = (key, value);
        leaf_page.increase_size(1);
        println!("leaf insertion at leaf {} position {} succeeded! current leaf size = {}\n", 
            leaf_page.get_page_id(), pos, leaf_page.get_size());
        Ok(())
    }

    /**
     * Insert the k-v pair in the correct position of the given internal page.
     * Assumes the internal page is not full.
     */
    fn insert_in_internal(&mut self, key: KeyType, value: PageID, 
                            internal_page: &mut BPlusTreeInternalPage) -> crate::error::Result<()> {

        let pos = self.find_new_key_position::<BPlusTreeInternalPage>(key, internal_page);
        assert_ne!(pos, -1);
        assert!(internal_page.get_size() < internal_page.get_max_size());
        for i in (pos..internal_page.get_size()).rev() {
            internal_page.array[(i+1) as usize] = internal_page.array[i as usize];
        }
        internal_page.array[pos as usize] = (key, value);
        internal_page.increase_size(1);
        println!("internal insertion at internal {} position {} succeeded! current internal size = {}\n",
            internal_page.get_page_id(), pos, internal_page.get_size());
        Ok(())
    }

    /**
     * Insert the key into the corresponding page to the given parent page_id.
     */
    unsafe fn insert_in_parent(&mut self, key: KeyType, left_page_id: PageID, 
                                right_page_id: PageID, parent_page_id: PageID) -> crate::error::Result<()> {

        // there is no parent for the left page, meaning the left page is the root
        if parent_page_id == INVALID_PAGE_ID {
            // create a new root page
            let mut new_root_page_id: PageID = INVALID_PAGE_ID;
            let new_page = self.buffer_pool_manager.new_page(&mut new_root_page_id)
                .expect("fetch failed");
            let data_ptr: *mut [u8; PAGE_SIZE] = &mut new_page.data;
            let tree_page_ptr: *mut BPlusTreeInternalPage = data_ptr.cast::<BPlusTreeInternalPage>();
            let new_root_page = tree_page_ptr.as_mut().expect("cast failed");

            // initialize the corresponding fields
            new_root_page.init(new_root_page_id, INVALID_PAGE_ID, self.internal_max_size);

            // update the tree's root's page_id
            self.root_page_id = new_root_page_id;

            // fetch and set the parent pointer for the left page (old root)
            let left_page_ptr =
                self.fetch_page_as_mut_ptr::<BPlusTreeInternalPage>(left_page_id);
            let left_page = left_page_ptr.as_mut()
                .expect("raw-pointer to reference failed");
            left_page.set_parent_page_id(new_root_page_id);

            // unpin the left page
            self.buffer_pool_manager.unpin_page(left_page_id, true);

            // fetch set the parent pointer for the right page (newly splitted node)
            let right_page_ptr =
                self.fetch_page_as_mut_ptr::<BPlusTreeInternalPage>(right_page_id);
            let right_page = right_page_ptr.as_mut()
                .expect("raw-pointer to reference failed");
            right_page.set_parent_page_id(new_root_page_id);

            // unpin the right page
            self.buffer_pool_manager.unpin_page(right_page_id, true);

            // debug output
            println!("page {}: parent = {}", left_page_id, left_page.get_parent_page_id());
            println!("page {}: parent = {}", right_page_id, right_page.get_parent_page_id());

            // set the new root's k-v pairs
            new_root_page.array[0] = (0, left_page_id);
            new_root_page.array[1] = (key, right_page_id);

            // increase the new root's size
            new_root_page.increase_size(1);

            // unpin the new root page
            self.buffer_pool_manager.unpin_page(new_root_page.get_page_id(), true);

            // debug output
            println!("parent insertion succeeded (created new root {})!", self.root_page_id);
            print!("root {}: {{ {:?} ", new_root_page.get_page_id(), new_root_page.array[0]);
            for i in 1..new_root_page.get_size() {
                print!("{:?} ", new_root_page.array[i as usize]);
            }
            println!("}}\n");

            return Ok(());
        }

        // fetch the parent page
        let parent_page_ptr =
                self.fetch_page_as_mut_ptr::<BPlusTreeInternalPage>(parent_page_id);
        let parent_page = parent_page_ptr.as_mut()
            .expect("raw-pointer to reference failed");

        // if the parent page is not full, simply insert into it
        if parent_page.get_size() < parent_page.get_max_size() {
            return self.insert_in_internal(key, right_page_id, parent_page);
        }

        // the parent page is full; need to split
        // create a new parent page
        let mut new_parent_page_id: PageID = INVALID_PAGE_ID;
        let new_page = self.buffer_pool_manager.new_page(&mut new_parent_page_id)
            .expect("fetch failed");
        let data_ptr: *mut [u8; PAGE_SIZE] = &mut new_page.data;
        let tree_page_ptr: *mut BPlusTreeInternalPage = data_ptr.cast::<BPlusTreeInternalPage>();
        let new_parent_page = tree_page_ptr.as_mut().expect("cast failed");

        // initialize the corresponding fields
        new_parent_page.init(new_parent_page_id, parent_page.get_parent_page_id(), self.internal_max_size);

        // copy all the k-v pairs into a temp vec, including the one to insert and the [no-key] one in the head
        let total_size = parent_page.get_size()+1;
        let mut temp_memory = vec![(KeyType::default(), PageID::default()); total_size as usize];
        let pos = self.find_new_key_position::<BPlusTreeInternalPage>(key, parent_page);
        for i in 0..pos {
            temp_memory[i as usize] = parent_page.array[i as usize];
        }
        temp_memory[pos as usize] = (key, right_page_id);
        for i in (pos+1)..total_size {
            temp_memory[i as usize] = parent_page.array[(i-1) as usize];
        }

        // debug output
        println!("{:?}", temp_memory);

        // copy the first half (floor) to the original parent, including [no-key]
        let mid = total_size / 2;
        for i in 0..mid {
            parent_page.array[i as usize] = temp_memory[i as usize];
        }

        // the key to push upwards
        let (up_key, _) = temp_memory[mid as usize];

        // copy the last half (ceil) to the new parent, including the key to push upwards,
        // which fits in the [0] position
        for i in mid..total_size {
            new_parent_page.array[(i-mid) as usize] = temp_memory[i as usize];
        }

        // set the corresponding page sizes
        parent_page.set_size(mid);
        new_parent_page.set_size(total_size - mid);

        // debug outputs
        print!("internal {}: {{ {:?} ", parent_page.get_page_id(), parent_page.array[0]);
        for i in 1..parent_page.get_size() {
            print!("{:?} ", parent_page.array[i as usize]);
        }
        println!("}}, parent = {}", new_parent_page.get_parent_page_id());

        print!("internal {}: {{ {:?} ", new_parent_page.get_page_id(), new_parent_page.array[0]);
        for i in 1..new_parent_page.get_size() {
            print!("{:?} ", new_parent_page.array[i as usize]);
        }
        println!("}}, parent = {}", new_parent_page.get_parent_page_id());

        // update the new parent's children's parent_page_id
        for i in 0..new_parent_page.get_size() {
            // fetch the child page
            let child_page_id = new_parent_page.value_at(i);
            let child_page_ptr: *mut BPlusTreeInternalPage = self.fetch_page_as_mut_ptr(child_page_id);
            let child_page = child_page_ptr.as_mut()
                .expect("child raw-pointer to reference failed");
            child_page.set_parent_page_id(new_parent_page_id);

            // unpin the child page
            self.buffer_pool_manager.unpin_page(child_page_id, true);
        }

        // the parent page_id of the old parent
        let para_parent_page_id = parent_page.get_parent_page_id();

        // unpin the old and the new parent page
        self.buffer_pool_manager.unpin_page(parent_page_id, true);
        self.buffer_pool_manager.unpin_page(new_parent_page_id, true);

        self.insert_in_parent(up_key, parent_page_id, new_parent_page_id, para_parent_page_id)
    }
}



/**
 * Struct that holds information for the iterator.
 */
struct ItPtr {
    cur_page_id: PageID,
    cur_index: i32,
}

/**
 * Iterator for BPlusTree.
 */
impl Iterator for BPlusTree {
    type Item = (KeyType, ValueType);

    fn next(&mut self) -> Option<Self::Item> {
        unsafe {
            // increment cur_index
            self.it_ptr.cur_index += 1;

            // fetch the corresponding leaf page and check if it has been passed
            let mut leaf_page_ptr =
                self.fetch_page_as_const_ptr::<BPlusTreeLeafPage>(self.it_ptr.cur_page_id);
            let mut leaf_page = leaf_page_ptr.as_ref()
                .expect("raw-pointer to reference failed");
            if self.it_ptr.cur_index >= leaf_page.get_size() {
                self.it_ptr.cur_page_id = leaf_page.get_next_page_id();
                self.it_ptr.cur_index = 0;

                // assumption: if there is a next sibling, then it is not empty
                if self.it_ptr.cur_page_id == INVALID_PAGE_ID {
                    return None;
                }

                leaf_page_ptr = self.fetch_page_as_const_ptr::<BPlusTreeLeafPage>(self.it_ptr.cur_page_id);
                leaf_page = leaf_page_ptr.as_ref().expect("raw-pointer to reference failed");
            }

            let (key, value) = leaf_page.array[self.it_ptr.cur_index as usize];
            return Some((key, value));
        }
    }
}