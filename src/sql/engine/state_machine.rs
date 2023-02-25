use super::kv_engine::KvEngine;

/// The Raft state machine for the Raft-based SQL engine, using a KV SQL engine
pub struct StateMachine {
    /// The underlying KV SQL engine
    engine: KvEngine,
    /// The last applied index
    applied_index: u64,
}