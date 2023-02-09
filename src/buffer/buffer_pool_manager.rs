use std::collections::HashMap;
use crate::common::{FrameID, PageID};
use crate::storage::kv::page::{Page};

use super::lru_k_replacer::{self, LRUKReplacer};

struct BufferPoolManager {
    pool_size: usize,
    next_page_id: PageID,

    // disk_mamager,
    // log_manager,
    page_table: HashMap<PageID, FrameID>,
    replacer: LRUKReplacer,
    // free_list,
    pages: [Page],
}

impl BufferPoolManager {
    // pub fn new(
    //     pool_size: usize,
    //     // disk_manager: ,
    //     // log_manager: ,
    //     replacer_k: usize,
    //     ) -> Self {
    //     Self {
    //         pool_size,
    //         next_page_id: 0,
    //         page_table: (),
    //         replacer: (),
    //         pages: [Page::new(); pool_size],
    //     }
    // }
}