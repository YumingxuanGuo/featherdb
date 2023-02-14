pub mod rid;

pub type FrameID = i32;  // frame id type
pub type PageID = i32;   // page id type
pub type LSN = i32;      // log sequence number type

pub const PAGE_SIZE: usize = 4096;
pub const INVALID_PAGE_ID: PageID = -1;
pub const INVALID_LSN: LSN = -1;