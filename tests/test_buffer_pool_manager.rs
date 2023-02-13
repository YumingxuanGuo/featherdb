use rand::RngCore;
use gman_db::common::{PageID, PAGE_SIZE};
use gman_db::storage::disk::disk_manager::DiskManager;
use gman_db::buffer::buffer_pool_manager::BufferPoolManager;

#[test]
fn test_binary_data() {
    let buffer_pool_size = 10;
    let k = 5;
  
    let disk_manager = DiskManager::new();
    let mut bpm = BufferPoolManager::new(buffer_pool_size, disk_manager, k);
  
    let mut page_id_temp: PageID = -1;
    let mut page0 = bpm.new_page(&mut page_id_temp);
  
    // Scenario: The buffer pool is empty. We should be able to create a new page.
    assert!(page0.is_some());
    assert_eq!(page_id_temp, 0);
  
    let mut random_binary_data = [0u8; PAGE_SIZE];
    rand::thread_rng().fill_bytes(&mut random_binary_data);
  
    // Insert terminal characters both in the middle and at end
    random_binary_data[PAGE_SIZE / 2] = 0;
    random_binary_data[PAGE_SIZE - 1] = 0;
  
    // Scenario: Once we have a page, we should be able to read and write content.
    page0.as_mut().unwrap().data.copy_from_slice(&random_binary_data);
    assert_eq!(random_binary_data, page0.as_mut().unwrap().data);
  
    // Scenario: We should be able to create new pages until we fill up the buffer pool.
    for _ in 1..buffer_pool_size {
        assert!(bpm.new_page(&mut page_id_temp).is_some());
    }
  
    // Scenario: Once the buffer pool is full, we should not be able to create any new pages.
    for _ in 0..buffer_pool_size {
        assert!(bpm.new_page(&mut page_id_temp).is_none());
    }
  
    // Scenario: After unpinning pages {0, 1, 2, 3, 4} we should be able to create 5 new pages
    for i in 0..5 {
        assert_eq!(bpm.unpin_page(i, true), true);
        bpm.flush_page(i);
    }
    for _ in 0..5 {
        assert!(bpm.new_page(&mut page_id_temp).is_some());
        bpm.unpin_page(page_id_temp, false);
    }

    // Scenario: We should be able to fetch the data we wrote a while ago.
    page0 = bpm.fetch_page(0);
    assert_eq!(random_binary_data, page0.as_mut().unwrap().data);
    assert_eq!(bpm.unpin_page(0, true), true);
}