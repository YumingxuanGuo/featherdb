// use std::sync::{Arc, RwLock};
use crate::common::{PageID, LSN};

// const DEFAULT_ORDER: usize = 8;

pub const INVALID_INDEX_PAGE: u32 = 0;
pub const LEAF_PAGE: u32 = 1;
pub const INTERNAL_PAGE: u32 = 2;

pub struct BPlusTreePage {
    pub page_type: u32,
    pub lsn: LSN,
    pub size: i32,
    pub max_size: i32,
    pub page_id: PageID,
    pub parent_page_id: PageID,
}

pub trait BPlusTreePageTraits {
    fn is_leaf_page(&self) -> bool;
    fn is_root_page(&self) -> bool;
    fn set_page_type(&mut self, page_type: u32);

    fn get_size(&self) -> i32;
    fn set_size(&mut self, size: i32);
    fn increase_size(&mut self, amount: i32);

    fn get_max_size(&self) -> i32;
    fn set_max_size(&mut self, max_size: i32);
    fn get_min_size(&self) -> i32;

    fn get_parent_page_id(&self) -> PageID;
    fn set_parent_page_id(&mut self, parent_page_id: PageID);

    fn get_page_id(&self) -> PageID;
    fn set_page_id(&mut self, page_id: PageID);

    fn set_lsn(&mut self, lsn: LSN);
}

impl BPlusTreePage {
    pub fn new(page_id: PageID, parent_page_id: PageID, max_size: i32) -> Self {
        Self {
            page_type: INVALID_INDEX_PAGE, 
            lsn: -1, 
            size: -1, 
            max_size,
            page_id, 
            parent_page_id,
        }
    }
}