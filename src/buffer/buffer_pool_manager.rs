use std::collections::{HashMap, LinkedList};
use std::rc::Rc;
use std::vec::{Vec};
use crate::common::{FrameID, PageID};
use crate::storage::kv::page::{Page};
use crate::storage::disk::disk_manager::{DiskManager};

use super::lru_k_replacer::{LRUKReplacer};

struct BufferPoolManager {
    pool_size: usize,
    next_page_id: PageID,

    // log_manager,
    disk_mamager: DiskManager,
    page_table: HashMap<PageID, FrameID>,
    replacer: LRUKReplacer,
    free_list: LinkedList<FrameID>,
    pages: Vec<Rc<Page>>
}

impl BufferPoolManager {
    pub fn new(pool_size: usize, replacer_k: usize) -> Self {
        let mut this = Self {
            pool_size,
            next_page_id: 0,
            disk_mamager: DiskManager::new(),
            page_table: HashMap::new(),
            replacer: LRUKReplacer::new(pool_size, replacer_k),
            free_list: LinkedList::new(),
            pages: vec![Rc::new(Page::new()); pool_size]
        };
        
        for i in 0..pool_size {
            this.free_list.push_back(i as FrameID);
        }

        return this;
    }

    /// TODO: unimplemented
    pub fn new_page(&self, page_id: PageID) -> Rc<Page> {
        return Rc::clone(&self.pages[page_id as usize]);
    }

    /// TODO: unimplemented
    pub fn fetch_page(&self, page_id: PageID) -> Rc<Page> {
        return Rc::clone(&self.pages[page_id as usize]);
    }

    /// TODO: unimplemented
    pub fn unpin_page(&self, page_id: PageID, is_dirty: bool) -> bool {
        return false;
    }

    /// TODO: unimplemented
    pub fn flush_page(&self, page_id: PageID) -> bool {
        return false;
    }

    /// TODO: unimplemented
    pub fn flush_all_pages() {

    }

    /// TODO: unimplemented
    pub fn delete_page(page_id: PageID) -> bool {
        return false;
    }
}