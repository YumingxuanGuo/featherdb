use std::sync::{Arc, RwLock};
use crate::error::{Error, Result};

const DEFAULT_ORDER: usize = 8;

pub struct BPlusTreePage {
    /// The tree root, guarded by an RwLock to support multiple iterators across it.
    root: Arc<RwLock<Node>>,
}

impl BPlusTreePage {
    pub fn new() -> Self {
        Self::new_with_order(DEFAULT_ORDER).unwrap()
    }

    pub fn new_with_order(order: usize) -> Result<Self> {
        if order < 2 {
            return Err(Error::Internal("Order must be at least 2".into()));
        }
        Ok(Self { root: Arc::new(RwLock::new(Node::Root(Children::new(order)))) })
    }
}

enum Node {
    Root(Children),
    Inner(Children),
    Leaf(Values),
}

struct Children {
    keys: Vec<Vec<u8>>,
    nodes: Vec<Node>,
}

impl Children {
    fn new(order: usize) -> Self {
        Self { keys: Vec::with_capacity(order - 1), nodes: Vec::with_capacity(order) }
    }
}

struct Values{
    data: Vec<(Vec<u8>, Vec<u8>)>,
}