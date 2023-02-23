use crate::storage::{Store, Scan, Range};
use crate::error::{Error, Result};
use crate::common::{KeyType, ValueType};

use std::{sync::{Arc, RwLock}, fmt::Display, ops::{DerefMut, Deref}, cmp::Ordering};

/// The default B+tree order, i.e. maximum number of children per node.
const DEFAULT_ORDER: usize = 8;

/// In-memory B+tree index.
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
        self.root.write()?.set_or_insert(key, value);
        Ok(())
    }

    fn get(&self, key: &[u8]) -> Result<Option<ValueType>> {
        Ok(self.root.read()?.get(key))
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        // self.root.write()?.delete(key);
        Ok(())
    }

    fn scan(&self, range: Range) -> Scan {
        Box::new(Iter::new(self.root.clone(), range))
    }

    fn flush(&mut self) -> Result<()> {
        // interact with disk manager
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
    /// Sets a key to a value in the node, inserting or updating the key as appropriate. If the
    /// node splits, return the split key and new (right) node.
    fn set_or_insert(&mut self, key: &[u8], value: ValueType) -> Option<(Vec<u8>, Node)> {

        None
    }

    /// Fetches a value for a key, if it exists.
    fn get(&self, key: &[u8]) -> Option<ValueType> {
        match self {
            Self::Root(children) | Self::Inner(children) => children.get(key),
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
    fn get(&self, key: &[u8]) -> Option<ValueType> {
        if !self.is_empty() {
            self.lookup(key).1.get(key)
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
/// A key range scan; currently O(log n).
struct Iter {
    /// The root node of the tree we're iterating across.
    root: Arc<RwLock<Node>>,
    /// The range we're iterating over.
    range: Range,
    /// The front cursor keeps track of the last returned value from the front.
    front_cursor: Option<Vec<u8>>,
    /// The back cursor keeps track of the last returned value from the back.
    back_cursor: Option<Vec<u8>>,
}

impl Iter {
    /// Creates a new iterator.
    fn new(root: Arc<RwLock<Node>>, range: Range) -> Self {
        Self { root, range, front_cursor: None, back_cursor: None }
    }

    // next() with error handling.
    fn try_next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        Ok(None)
    }

    /// next_back() with error handling.
    fn try_next_back(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        Ok(None)
    }
}

impl Iterator for Iter {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
    }
}

impl DoubleEndedIterator for Iter {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.try_next_back().transpose()
    }
}