use crate::storage::kv::concurrency::mvcc::MVCC;

/// A SQL engine based on an underlying MVCC key/value store
pub struct MVCCEngine {
    /// The underlying MVCC key/value store
    pub(super) mvcc: MVCC,
}

// impl Clone for MVCCEngine {
//     fn clone(&self) -> Self {
        
//     }
// }

impl MVCCEngine {
    ///// Creates a new MVCC key/value-based SQL engine
    // pub fn new() -> Self {
    //     Self { kv }
    // }
}