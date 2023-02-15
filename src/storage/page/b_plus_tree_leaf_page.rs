use crate::common::{PageID, LSN, PAGE_SIZE, rid::RID, INVALID_PAGE_ID};
use std::mem;
use super::b_plus_tree_page::{BPlusTreePage, BPlusTreePageTraits, LEAF_PAGE};

const LEAF_HEADER_SIZE: usize = mem::size_of::<BPlusTreePage>() + mem::size_of::<PageID>();
const LEAF_DATA_SIZE: usize = PAGE_SIZE - LEAF_HEADER_SIZE;
const KVPAIR_SIZE: usize = LEAF_DATA_SIZE / mem::size_of::<(i32, ValueType)>();

type KeyType = i32;
type ValueType = RID;

pub struct BPlusTreeLeafPage {
    pub b_plus_tree_page: BPlusTreePage,
    pub next_page_id: PageID,
    pub array: [(KeyType, ValueType); KVPAIR_SIZE]
}

impl BPlusTreePageTraits for BPlusTreeLeafPage {
    fn is_leaf_page(&self) -> bool { return self.b_plus_tree_page.page_type as u32 == LEAF_PAGE as u32; }
    fn is_root_page(&self) -> bool { return false; }
    fn set_page_type(&mut self, page_type: u32) { self.b_plus_tree_page.page_type = page_type; }

    fn get_size(&self) -> i32 { return self.b_plus_tree_page.size; }
    fn set_size(&mut self, size: i32) { self.b_plus_tree_page.size = size; }
    fn increase_size(&mut self, amount: i32) { self.b_plus_tree_page.size += amount; }

    fn get_max_size(&self) -> i32 { return self.b_plus_tree_page.max_size; }
    fn set_max_size(&mut self, max_size: i32) { self.b_plus_tree_page.max_size = max_size; }
    fn get_min_size(&self) -> i32 { return self.b_plus_tree_page.max_size / 2; }

    fn get_parent_page_id(&self) -> PageID { return self.b_plus_tree_page.parent_page_id; }
    fn set_parent_page_id(&mut self, parent_page_id: PageID) { self.b_plus_tree_page.parent_page_id = parent_page_id; }

    fn get_page_id(&self) -> PageID { return self.b_plus_tree_page.page_id; }
    fn set_page_id(&mut self, page_id: PageID) { self.b_plus_tree_page.page_id = page_id; }

    fn set_lsn(&mut self, lsn: LSN) { self.b_plus_tree_page.lsn = lsn; }
}

impl BPlusTreeLeafPage {
    pub fn init(&mut self, page_id: PageID, parent_page_id: PageID, _max_size: i32) {
        self.b_plus_tree_page = BPlusTreePage::new(page_id, parent_page_id, KVPAIR_SIZE.try_into().unwrap());
        self.next_page_id = INVALID_PAGE_ID;
    }

    pub fn get_next_page_id(&self) -> PageID { return self.next_page_id; }

    pub fn set_next_page_id(&mut self, next_page_id: PageID) { self.next_page_id = next_page_id; }

    pub fn key_at(&self, index: i32) -> &KeyType {
        let (key, _value) = &self.array[index as usize];
        return &key;
    }
}