use gman_db::{buffer::lru_k_replacer::{LRUKReplacer}, common::FrameID};



#[test]
fn test_general_lru() {
    let mut lru_replacer = LRUKReplacer::new(7, 2);

    // Scenario: add six elements to the replacer. We have [1,2,3,4,5]. Frame 6 is non-evictable.
    lru_replacer.record_access(1);
    lru_replacer.record_access(2);
    lru_replacer.record_access(3);
    lru_replacer.record_access(4);
    lru_replacer.record_access(5);
    lru_replacer.record_access(6);
    lru_replacer.set_evictable(1, true);
    lru_replacer.set_evictable(2, true);
    lru_replacer.set_evictable(3, true);
    lru_replacer.set_evictable(4, true);
    lru_replacer.set_evictable(5, true);
    lru_replacer.set_evictable(6, false);
    assert_eq!(lru_replacer.size(), 5);

    // Scenario: Insert access history for frame 1. Now frame 1 has two access histories.
    // All other frames have max backward k-dist. The order of eviction is [2,3,4,5,1].
    lru_replacer.record_access(1);

    // Scenario: evict three pages from the replacer. Elements with max k-distance should be popped
    // first based on LRU.
    let mut value: FrameID = -1;
    lru_replacer.evict(&mut value);
    assert_eq!(2, value);
    lru_replacer.evict(&mut value);
    assert_eq!(3, value);
    lru_replacer.evict(&mut value);
    assert_eq!(4, value);
    assert_eq!(2, lru_replacer.size());

    // Scenario: Now replacer has frames [5,1].
    // Insert new frames 3, 4, and update access history for 5. We should end with [3,1,5,4]
    lru_replacer.record_access(3);
    lru_replacer.record_access(4);
    lru_replacer.record_access(5);
    lru_replacer.record_access(4);
    lru_replacer.set_evictable(3, true);
    lru_replacer.set_evictable(4, true);
    assert_eq!(4, lru_replacer.size());

    // Scenario: continue looking for victims. We expect 3 to be evicted next.
    lru_replacer.evict(&mut value);
    assert_eq!(3, value);
    assert_eq!(3, lru_replacer.size());

    // Set 6 to be evictable. 6 Should be evicted next since it has max backward k-dist.
    lru_replacer.set_evictable(6, true);
    assert_eq!(4, lru_replacer.size());
    lru_replacer.evict(&mut value);
    assert_eq!(6, value);
    assert_eq!(3, lru_replacer.size());

    // Now we have [1,5,4]. Continue looking for victims.
    lru_replacer.set_evictable(1, false);
    assert_eq!(2, lru_replacer.size());
    assert_eq!(true, lru_replacer.evict(&mut value));
    assert_eq!(5, value);
    assert_eq!(1, lru_replacer.size());

    // Update access history for 1. Now we have [4,1]. Next victim is 4.
    lru_replacer.record_access(1);
    lru_replacer.record_access(1);
    lru_replacer.set_evictable(1, true);
    assert_eq!(2, lru_replacer.size());
    assert_eq!(true, lru_replacer.evict(&mut value));
    assert_eq!(value, 4);

    assert_eq!(1, lru_replacer.size());
    lru_replacer.evict(&mut value);
    assert_eq!(value, 1);
    assert_eq!(0, lru_replacer.size());

    // This operation should not modify size
    assert_eq!(false, lru_replacer.evict(&mut value));
    assert_eq!(0, lru_replacer.size());
}