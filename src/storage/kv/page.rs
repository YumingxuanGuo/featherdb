use std::marker::{Sized};
use crate::common::{FrameID, PageID, PAGE_SIZE, INVALID_PAGE_ID};

pub struct Page {
    data: [char; PAGE_SIZE],
    page_id: PageID,
    pin_count: i32,
    is_dirty: bool,
    // mutex
}

impl Page {
    pub fn new() -> Self {
        Self {
            data: ['\0'; PAGE_SIZE],
            page_id: INVALID_PAGE_ID,
            pin_count: 0,
            is_dirty: false,
        }
    }
}
