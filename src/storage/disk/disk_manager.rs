use crate::common::{FrameID, PageID, PAGE_SIZE};


struct PageFile {
    filename: String,
    offset: usize,
    // mutex
}

pub struct DiskManager {
    // mutex
    data: Vec<PageFile>,
}

impl DiskManager {
    pub fn new() -> Self {
        Self { data: Vec::new() }
    }

    pub fn write_page(&self, page_id: PageID, page_data: &[char]) {

    }

    pub fn read_page(&self, page_id: PageID, page_data: &mut [char]) {

    }
}