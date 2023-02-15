// use std::sync::{Arc, RwLock};
use crate::common::{PageID, LSN};

// const DEFAULT_ORDER: usize = 8;

// #[derive(Clone)]
// pub enum IndexPageType {
//     InvalidIndexPage = 0,
//     LeafPage,
//     InternalPage
// }

pub const InvalidIndexPage: u32 = 0;
pub const LeafPage: u32 = 1;
pub const InternalPage: u32 = 2;

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
            page_type: InvalidIndexPage, 
            lsn: -1, 
            size: -1, 
            max_size,
            page_id, 
            parent_page_id,
        }
    }
}

// pub struct BPlusTreePage {
//     /// The tree root, guarded by an RwLock to support multiple iterators across it.
//     root: Arc<RwLock<Node>>,
// }

// impl BPlusTreePage {
//     pub fn new() -> Self {
//         Self::new_with_order(DEFAULT_ORDER).unwrap()
//     }

//     pub fn new_with_order(order: usize) -> Result<Self> {
//         if order < 2 {
//             return Err(Error::Internal("Order must be at least 2".into()));
//         }
//         Ok(Self { root: Arc::new(RwLock::new(Node::Root(Children::new(order)))) })
//     }
// }

// enum Node {
//     Root(Children),
//     Inner(Children),
//     Leaf(Values),
// }

// struct Children {
//     keys: Vec<Vec<u8>>,
//     nodes: Vec<Node>,
// }

// impl Children {
//     fn new(order: usize) -> Self {
//         Self { keys: Vec::with_capacity(order - 1), nodes: Vec::with_capacity(order) }
//     }
// }

// struct Values{
//     data: Vec<(Vec<u8>, Vec<u8>)>,
// }