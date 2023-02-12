use std::collections::{HashMap, LinkedList};
use crate::common::{FrameID};

struct FrameMetaData {
    evictable: bool,
    access_time: usize,
    timestamps: LinkedList<usize>,
}

pub struct LRUKReplacer {
    current_timestamp: usize,
    num_frames: usize,
    cur_size: usize,
    k: usize,
    // latch

    fully_accessed_frames: HashMap<FrameID, FrameMetaData>,
    partial_accessed_frames: HashMap<FrameID, FrameMetaData>,
}

impl LRUKReplacer {
    pub fn new(num_frames: usize, k: usize) -> Self {
        Self {
            current_timestamp: 0,
            num_frames,
            cur_size: 0,
            k,
            fully_accessed_frames: HashMap::new(),
            partial_accessed_frames: HashMap::new(),
        }
    }

  /**
   * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
   * that are marked as 'evictable' are candidates for eviction.
   *
   * A frame with less than k historical references is given +inf as its backward k-distance.
   * If multiple frames have inf backward k-distance, then evict frame with earliest timestamp
   * based on LRU.
   *
   * Successful eviction of a frame should decrement the size of replacer and remove the frame's
   * access history.
   *
   * @param[out] frame_id id of frame that is evicted.
   * @return true if a frame is evicted successfully, false if no frames can be evicted.
   */
    pub fn evict(&mut self, frame_id: &mut FrameID) -> bool {
        // lock

        if !self.partial_accessed_frames.is_empty() {
            let mut frame_id_to_evict: FrameID = -1;
            let mut earliest_timestamp: usize = 0;
            for (id, frame_meta_data) in &self.partial_accessed_frames {
                if frame_meta_data.evictable && 
                        earliest_timestamp > *frame_meta_data.timestamps.back().expect("linked list back() failed") {
                    frame_id_to_evict = *id;
                    earliest_timestamp = *frame_meta_data.timestamps.back().expect("linked list back() failed");
                }
            }
            if frame_id_to_evict != -1 {
                *frame_id = frame_id_to_evict;
                self.partial_accessed_frames.remove(&frame_id_to_evict);
                self.cur_size -= 1;
                // unlock
                return true;
            }
        }

        if !self.fully_accessed_frames.is_empty() {
            let mut frame_id_to_evict: FrameID = -1;
            let mut earliest_timestamp: usize = 0;
            for (id, frame_meta_data) in &self.fully_accessed_frames {
                if frame_meta_data.evictable && 
                        earliest_timestamp > *frame_meta_data.timestamps.back().expect("linked list back() failed") {
                    frame_id_to_evict = *id;
                    earliest_timestamp = *frame_meta_data.timestamps.back().expect("linked list back() failed");
                }
            }
            if frame_id_to_evict != -1 {
                *frame_id = frame_id_to_evict;
                self.fully_accessed_frames.remove(&frame_id_to_evict);
                self.cur_size -= 1;
                // unlock
                return true;
            }
        }

        // unlock
        return false;
    }

    pub fn record_access(&mut self, frame_id: FrameID) {
        // lock

        if frame_id as usize > self.num_frames {
            panic!("frame id out of bound");
        }

        if self.fully_accessed_frames.contains_key(&frame_id) {
            let frame = self.fully_accessed_frames.get_mut(&frame_id)
                    .expect("get_mut failed");
            frame.timestamps.pop_back();
            frame.timestamps.push_front(self.current_timestamp);
            self.current_timestamp += 1;
        }

        if self.partial_accessed_frames.contains_key(&frame_id) {
            let frame = self.partial_accessed_frames.get_mut(&frame_id)
                    .expect("get_mut failed");
            self.current_timestamp += 1;
            frame.timestamps.push_front(self.current_timestamp);
            frame.access_time += 1;
            if frame.access_time >= self.k {
                self.fully_accessed_frames.insert(frame_id,
                    self.partial_accessed_frames.remove(&frame_id).expect("remove failed"));
            }
        }
        // unlock
    }

    pub fn set_evictable(frame_id: FrameID, mode: bool) {

    }

    pub fn remove(frame_id: FrameID) {

    }
}