use std::collections::HashMap;
use crate::common::{FrameID};

struct FrameMetaData {

}

pub struct LRUKReplacer {
    current_timestamp: usize,
    num_frames: usize,
    cur_size: usize,
    k: usize,
    // mutex

    fully_accessed_frames: HashMap<FrameID, FrameMetaData>,
    partial_accessed_frames: HashMap<FrameID, FrameMetaData>,
}

impl LRUKReplacer {
    pub fn new(num_frames: usize, k: usize) -> Self {
        Self {
            current_timestamp: 0,
            num_frames, cur_size: 0,
            k,
            fully_accessed_frames: HashMap::new(),
            partial_accessed_frames: HashMap::new(),
        }
    }

    pub fn evict(frame_id: FrameID) -> bool {
        return false;
    }

    pub fn record_access(frame_id: FrameID) {

    }

    pub fn set_evictable(frame_id: FrameID, mode: bool) {

    }

    pub fn remove(frame_id: FrameID) {

    }
}