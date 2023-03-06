use std::sync::Arc;

use super::block::Block;

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;
