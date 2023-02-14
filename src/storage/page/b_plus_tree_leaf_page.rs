use crate::common::{PageID, LSN, PAGE_SIZE, INVALID_PAGE_ID, rid::RID};
use std::{mem};
use super::b_plus_tree_page::{BPlusTreePage, BPlusTreePageTraits, IndexPageType};

const LEAF_HEADER_SIZE: usize = mem::size_of::<BPlusTreePage>() + mem::size_of::<PageID>();
const LEAF_DATA_SIZE: usize = PAGE_SIZE - LEAF_HEADER_SIZE;
const KVPAIR_SIZE: usize = LEAF_DATA_SIZE / mem::size_of::<(i32, RID)>();

pub type KVPair = (i32, RID);
pub type KeyType = i32;
pub type ValueType = RID;

pub struct BPlusTreeLeafPage {
    b_plus_tree_page: BPlusTreePage,
    next_page_id: PageID,
    // array: KeyValuePairs
    array: [KVPair; KVPAIR_SIZE]
}

impl BPlusTreePageTraits for BPlusTreeLeafPage {
    fn is_leaf_page(&self) -> bool { return false; }
    fn is_root_page(&self) -> bool { return false; }
    fn set_page_type(&mut self, page_type: IndexPageType) {  }

    fn get_size(&self) -> i32 { return 0; }
    fn set_size(&mut self, size: i32) {  }
    fn increase_size(&mut self, amount: i32) {  }

    fn get_max_size(&self) -> i32 { return 0; }
    fn set_max_size(&mut self, max_size: i32) { self.b_plus_tree_page.max_size = max_size; }
    fn get_min_size(&self) -> i32 { return 0; }

    fn get_parent_page_id(&self) -> PageID { return INVALID_PAGE_ID; }
    fn set_parent_page_id(&mut self, parent_page_id: PageID) {  }

    fn get_page_id(&self) -> PageID { return INVALID_PAGE_ID; }
    fn set_page_id(&mut self, page_id: PageID) {  }

    fn set_lsn(&mut self, lsn: LSN) {  }
}

impl BPlusTreeLeafPage {
    pub fn init(&mut self, page_id: PageID, parent_page_id: PageID, max_size: i32) {
        self.b_plus_tree_page = BPlusTreePage::new(page_id, parent_page_id, max_size);
    }

    pub fn get_next_page_id(&self) -> PageID { return self.next_page_id; }

    pub fn set_next_page_id(&mut self, next_page_id: PageID) { self.next_page_id = next_page_id; }

    pub fn key_at(&self, index: i32) -> &KeyType {
        let kvpair = &self.array[index as usize];
        return &kvpair.0;
    }
}