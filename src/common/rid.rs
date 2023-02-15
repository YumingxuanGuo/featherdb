use crate::common::PageID;

#[derive(Copy, Clone)]
pub struct RID {
    pub page_id: PageID,
    pub slot_num: u32
}