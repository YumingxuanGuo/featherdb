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

  /**
   * @brief Record the event that the given frame id is accessed at current timestamp.
   * Create a new entry for access history if frame id has not been seen before.
   *
   * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
   * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
   *
   * @param frame_id id of frame that received a new access.
   */
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

  /**
   * @brief Toggle whether a frame is evictable or non-evictable. This function also
   * controls replacer's size. Note that size is equal to number of evictable entries.
   *
   * If a frame was previously evictable and is to be set to non-evictable, then size should
   * decrement. If a frame was previously non-evictable and is to be set to evictable,
   * then size should increment.
   *
   * If frame id is invalid, throw an exception or abort the process.
   *
   * For other scenarios, this function should terminate without modifying anything.
   *
   * @param frame_id id of frame whose 'evictable' status will be modified
   * @param set_evictable whether the given frame is evictable or not
   */
    pub fn set_evictable(&mut self, frame_id: FrameID, mode: bool) {
        // lock

        if frame_id as usize > self.num_frames {
            panic!("frame id out of bound");
        }

        if self.fully_accessed_frames.contains_key(&frame_id) {
            let frame = self.fully_accessed_frames.get_mut(&frame_id)
                    .expect("get_mut failed");
            if frame.evictable && !mode {
                self.cur_size -= 1;
            } else if !frame.evictable && mode {
                self.cur_size += 1;
            }
            frame.evictable = mode;
            // unlock
            return;
        }

        if self.partial_accessed_frames.contains_key(&frame_id) {
            let frame = self.partial_accessed_frames.get_mut(&frame_id)
                    .expect("get_mut failed");
            if frame.evictable && !mode {
                self.cur_size -= 1;
            } else if !frame.evictable && mode {
                self.cur_size += 1;
            }
            frame.evictable = mode;
            // unlock
            return;
        }

        panic!("should not reach here: set_evictable()");
    }

  /**
   * @brief Remove an evictable frame from replacer, along with its access history.
   * This function should also decrement replacer's size if removal is successful.
   *
   * Note that this is different from evicting a frame, which always remove the frame
   * with largest backward k-distance. This function removes specified frame id,
   * no matter what its backward k-distance is.
   *
   * If Remove is called on a non-evictable frame, throw an exception or abort the
   * process.
   *
   * If specified frame is not found, directly return from this function.
   *
   * @param frame_id id of frame to be removed
   */
    pub fn remove(&mut self, frame_id: FrameID) {
        // lock

        if frame_id as usize > self.num_frames {
            panic!("frame id out of bound");
        }

        if self.fully_accessed_frames.contains_key(&frame_id) {
            let frame = self.fully_accessed_frames.get_mut(&frame_id)
                    .expect("get_mut failed");
            if !frame.evictable {
                panic!("remove(): cannot remove unevictable frame");
            }
            self.fully_accessed_frames.remove(&frame_id);
            self.cur_size -= 1;
            // unlock
            return;
        }

        if self.partial_accessed_frames.contains_key(&frame_id) {
            let frame = self.partial_accessed_frames.get_mut(&frame_id)
                    .expect("get_mut failed");
            if !frame.evictable {
                panic!("remove(): cannot remove unevictable frame");
            }
            self.partial_accessed_frames.remove(&frame_id);
            self.cur_size -= 1;
            // unlock
            return;
        }
    }

    pub fn size(&self) -> usize {
        // lock
        let size = self.cur_size;
        // unlock
        return size;
    }
}