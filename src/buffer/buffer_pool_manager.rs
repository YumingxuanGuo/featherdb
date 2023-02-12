use std::collections::{HashMap, LinkedList};
use std::sync::Arc;
use std::vec::{Vec};
use crate::common::{FrameID, PageID};
use crate::storage::page::page::{Page};
use crate::storage::disk::disk_manager::{DiskManager};

use super::lru_k_replacer::{LRUKReplacer};

pub struct BufferPoolManager {
    pool_size: usize,
    next_page_id: PageID,

    // log_manager,
    disk_mamager: DiskManager,
    page_table: HashMap<PageID, FrameID>,
    replacer: LRUKReplacer,
    free_list: LinkedList<FrameID>,
    pages: Vec<Page>
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
            pages: vec![Page::new(); pool_size]
        };
        
        for i in 0..pool_size {
            this.free_list.push_back(i as FrameID);
        }

        return this;
    }

    /**
     * @brief Create a new page in the buffer pool. Set page_id to the new page's id, or nullptr if all frames
     * are currently in use and not evictable (in another word, pinned).
     *
     * You should pick the replacement frame from either the free list or the replacer (always find from the free list
     * first), and then call the AllocatePage() method to get a new page id. If the replacement frame has a dirty page,
     * you should write it back to the disk first. You also need to reset the memory and metadata for the new page.
     *
     * Remember to "Pin" the frame by calling replacer.SetEvictable(frame_id, false)
     * so that the replacer wouldn't evict the frame before the buffer pool manager "Unpin"s it.
     * Also, remember to record the access history of the frame in the replacer for the lru-k algorithm to work.
     *
     * @param[out] page_id id of created page
     * @return nullptr if no new pages could be created, otherwise pointer to new page
     */
    pub fn new_page(&mut self, page_id: &mut PageID) -> Option<&mut Page> {
        let mut frame_id: FrameID = -1;

        if !self.free_list.is_empty() {
            // if free frames exist
            frame_id = *self.free_list.front().unwrap();
            self.free_list.pop_front();
        } else {
            // all frames are occupied, need eviction
            if !self.replacer.evict(&mut frame_id) {
                return None;
            }
            let evicted_page = &self.pages[frame_id as usize];
            if evicted_page.is_dirty {
                self.disk_mamager.write_page(evicted_page.page_id, &evicted_page.data);
            }
            self.page_table.remove(&evicted_page.page_id);
        }

        *page_id = self.allocate_page();
        self.page_table.insert(*page_id, frame_id);

        self.replacer.record_access(frame_id);
        self.replacer.set_evictable(frame_id, false);

        self.pages[frame_id as usize].pin_count = 1;
        self.pages[frame_id as usize].page_id = *page_id;

        return Some(&mut self.pages[frame_id as usize]);
    }

    /// TODO: unimplemented
    /**
     * @brief Fetch the requested page from the buffer pool. Return nullptr if page_id needs to be fetched from the disk
     * but all frames are currently in use and not evictable (in another word, pinned).
     *
     * First search for page_id in the buffer pool. If not found, pick a replacement frame from either the free list or
     * the replacer (always find from the free list first), read the page from disk by calling disk_manager_->ReadPage(),
     * and replace the old page in the frame. Similar to NewPage(), if the old page is dirty, you need to write it back
     * to disk and update the metadata of the new page
     *
     * In addition, remember to disable eviction and record the access history of the frame like you did for NewPage().
     *
     * @param page_id id of page to be fetched
     * @param access_type type of access to the page, only needed for leaderboard tests.
     * @return nullptr if page_id cannot be fetched, otherwise pointer to the requested page
     */
    pub fn fetch_page(&mut self, page_id: PageID) -> Option<&mut Page> {
        let mut frame_id: FrameID = -1;

        // if page is already in the buffer pool
        if self.page_table.contains_key(&page_id) {
            frame_id = self.page_table[&page_id];
            self.replacer.record_access(frame_id);
            self.replacer.set_evictable(frame_id, false);
            self.pages[frame_id as usize].pin_count += 1;
            return Some(&mut self.pages[frame_id as usize])
        }

        // page not buffered, need to read page
        if !self.free_list.is_empty() {
            // if free frames exist
            frame_id = *self.free_list.front().unwrap();
            self.free_list.pop_front();
        } else {
            // all frames are occupied, need eviction
            if !self.replacer.evict(&mut frame_id) {
                return None;
            }
            let evicted_page = &self.pages[frame_id as usize];
            if evicted_page.is_dirty {
                self.disk_mamager.write_page(evicted_page.page_id, &evicted_page.data);
            }
            self.page_table.remove(&evicted_page.page_id);
        }

        self.disk_mamager.read_page(page_id, &mut self.pages[frame_id as usize].data);
        self.page_table.insert(page_id, frame_id);

        self.replacer.record_access(frame_id);
        self.replacer.set_evictable(frame_id, false);

        self.pages[frame_id as usize].pin_count = 1;
        self.pages[frame_id as usize].page_id = page_id;

        return Some(&mut self.pages[frame_id as usize]);
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

    fn allocate_page(&mut self) -> PageID {
        self.next_page_id += 1;
        return self.next_page_id - 1;
    }
}