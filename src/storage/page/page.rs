use crate::common::{FrameID, PageID, PAGE_SIZE, INVALID_PAGE_ID};

#[derive(Clone)]
pub struct Page {
    pub data: [u8; PAGE_SIZE],
    pub page_id: PageID,
    pub pin_count: i32,
    pub is_dirty: bool,
    // mutex
}

impl Page {
    pub fn new() -> Self {
        Self {
            data: [u8::default(); PAGE_SIZE],
            page_id: INVALID_PAGE_ID,
            pin_count: 0,
            is_dirty: false,
        }
    }

    pub fn reset_memory(&mut self) {
        self.data = [u8::default(); PAGE_SIZE];
    }
}
