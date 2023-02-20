use super::{Store};
use crate::error::{Error, Result};

use std::{sync::{Arc, RwLock}, fmt::Display, ops::{DerefMut, Deref}, cmp::Ordering};

/// The default B+tree order, i.e. maximum number of children per node.
const DEFAULT_ORDER: usize = 8;

type KeyType = Vec<u8>;
type ValueType = Vec<u8>;

/// In-memory key-value store using a B+tree.
pub struct BPlusTree {
    /// The tree root, guarded by an RwLock to support multiple iterators across it.
    root: Arc<RwLock<Node>>,
}

impl BPlusTree {
    /// Creates a new in-memory store using the default order.
    pub fn new() -> Self {
        Self::new_with_order(DEFAULT_ORDER).unwrap()
    }

    /// Creates a new in-memory store using the given order.
    pub fn new_with_order(order: usize) -> Result<Self> {
        if order < 2 {
            return Err(Error::Internal("Order must be at least 2".into()));
        }
        Ok(Self { root: Arc::new(RwLock::new(Node::Root(Children::new(order)))) })
    }
}

impl Display for BPlusTree {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "memory")
    }
}

impl Store for BPlusTree {
    fn set_or_insert(&mut self, key: &[u8], value: ValueType) -> Result<()> {
        // self.root.write()?.set(key, value);
        Ok(())
    }

    fn get(&self, key: &[u8]) -> Result<Option<ValueType>> {
        Ok(self.root.read()?.node_get(key))
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        // self.root.write()?.delete(key);
        Ok(())
    }

}

/// B-tree node variants. Most internal logic is delegated to the contained Children/Values structs,
/// while this outer structure manages the overall tree, particularly root special-casing.
///
/// All nodes in a tree have the same order (i.e. the same maximum number of children/values). The
/// root node can contain anywhere between 0 and the maximum number of items, while inner and leaf
/// nodes try to stay between order/2 and order items.
#[derive(Debug, PartialEq)]
enum Node {
    Root(Children),
    Inner(Children),
    Leaf(Values),
}

impl Node {
    /// Fetches a value for a key, if it exists.
    fn node_get(&self, key: &[u8]) -> Option<ValueType> {
        match self {
            Self::Root(children) | Self::Inner(children) => children.children_get(key),
            Self::Leaf(values) => values.values_get(key),
        }
    }
}

#[derive(Debug, PartialEq)]
struct Children {
    keys: Vec<KeyType>,
    nodes: Vec<Node>
}

impl Deref for Children {
    type Target = Vec<Node>;
    fn deref(&self) -> &Self::Target {
        &self.nodes
    }
}

impl DerefMut for Children {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.nodes
    }
}

impl Children {
    /// Creates a new child set of the given order (maximum capacity).
    fn new(order: usize) -> Self {
        Self {
            keys: Vec::with_capacity(order - 1),
            nodes: Vec::with_capacity(order)
        }
    }

    /// Fetches a value for a key, if it exists.
    fn children_get(&self, key: &[u8]) -> Option<ValueType> {
        if !self.is_empty() {
            self.lookup(key).1.node_get(key)
        } else {
            None
        }
    }

    /// Looks up the child responsible for a given key. 
    /// Assumes non-empty child sets, which should be all child sets except for the initial root node.
    fn lookup(&self, key: &[u8]) -> (usize, &Node) {
        let i = self.keys.iter()
            .position(|k| k.deref() > key)  // first index greater than `key`
            .unwrap_or_else(|| self.keys.len());                    // the last node
        (i, &self[i])
    }
}


#[derive(Debug, PartialEq)]
struct Values(Vec<(KeyType, ValueType)>);

impl Deref for Values {
    type Target = Vec<(Vec<u8>, Vec<u8>)>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Values {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Values {
    /// Creates a new value set with the given order (maximum capacity).
    fn new(order: usize) -> Self {
        Self(Vec::with_capacity(order))
    }

    /// Fetches a value from the set, if the key exists.
    fn values_get(&self, key: &[u8]) -> Option<ValueType> {
        self.iter()
            .find_map(|(k, v)| match (&**k).cmp(key) {
                Ordering::Greater => Some(None),
                Ordering::Equal => Some(Some(v.to_vec())),
                Ordering::Less => None,
        })
        .flatten()
    }
}