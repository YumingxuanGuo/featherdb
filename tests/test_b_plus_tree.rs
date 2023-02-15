use gman_db::common::rid::RID;
use gman_db::storage::index::Store;
use gman_db::storage::index::b_plus_tree::BPlusTree;
use gman_db::storage::page::b_plus_tree_internal_page::BPlusTreeInternalPage;
use gman_db::storage::page::b_plus_tree_leaf_page::BPlusTreeLeafPage;
use gman_db::storage::page::b_plus_tree_page::{BPlusTreePage, BPlusTreePageTraits, LeafPage};
use gman_db::common::{PAGE_SIZE, INVALID_PAGE_ID};
use gman_db::storage::disk::disk_manager::DiskManager;
use gman_db::buffer::buffer_pool_manager::BufferPoolManager;

#[test]
fn test_b_plus_tree_get_value() {
    let buffer_pool_size = 10;
    let k = 5;
  
    let disk_manager = DiskManager::new();
    let mut bpm = BufferPoolManager::new(buffer_pool_size, disk_manager, k);

    // Page 0
    let mut kv_data = [(0i32, 0i32); 509];
    kv_data[0] = (-1, 1);
    kv_data[1] = (3, 2);
    kv_data[2] = (7, 3);
    let mut root_page = BPlusTreeInternalPage{
        b_plus_tree_page: BPlusTreePage::new(0, INVALID_PAGE_ID, 509),
        array: kv_data
    };
    root_page.b_plus_tree_page.size = 2;
    let bytes: &[u8; PAGE_SIZE] = unsafe {
        let data_ptr: *const BPlusTreeInternalPage = &root_page;
        data_ptr.cast::<[u8; PAGE_SIZE]>().as_ref().unwrap()
    };
    let mut page0 = bpm.fetch_page(0);
    page0.as_mut().unwrap().data.copy_from_slice(bytes);

    // Page 1
    let mut kv_data1 = [(0i32, RID{page_id: 0, slot_num: 0}); 339];
    kv_data1[0] = (0, RID{page_id: 000, slot_num: 000});
    kv_data1[1] = (1, RID{page_id: 111, slot_num: 111});
    kv_data1[2] = (2, RID{page_id: 222, slot_num: 222});
    let mut root_page1 = BPlusTreeLeafPage{
        b_plus_tree_page: BPlusTreePage::new(2, 0, 339),
        next_page_id: 3,
        array: kv_data1
    };
    root_page1.b_plus_tree_page.size = 3;
    root_page1.set_page_type(LeafPage);
    let bytes1: &[u8; PAGE_SIZE] = unsafe {
        let data_ptr: *const BPlusTreeLeafPage = &root_page1;
        data_ptr.cast::<[u8; PAGE_SIZE]>().as_ref().unwrap()
    };
    let mut page1 = bpm.fetch_page(1);
    page1.as_mut().unwrap().data.copy_from_slice(bytes1);

    // Page 2
    let mut kv_data2 = [(0i32, RID{page_id: 0, slot_num: 0}); 339];
    kv_data2[0] = (3, RID{page_id: 333, slot_num: 333});
    kv_data2[1] = (5, RID{page_id: 555, slot_num: 555});
    kv_data2[2] = (6, RID{page_id: 666, slot_num: 666});
    let mut root_page2 = BPlusTreeLeafPage{
        b_plus_tree_page: BPlusTreePage::new(2, 0, 339),
        next_page_id: 3,
        array: kv_data2
    };
    root_page2.b_plus_tree_page.size = 3;
    root_page2.set_page_type(LeafPage);
    let bytes2: &[u8; PAGE_SIZE] = unsafe {
        let data_ptr: *const BPlusTreeLeafPage = &root_page2;
        data_ptr.cast::<[u8; PAGE_SIZE]>().as_ref().unwrap()
    };
    let mut page2 = bpm.fetch_page(2);
    page2.as_mut().unwrap().data.copy_from_slice(bytes2);

    // Page 3
    let mut kv_data3 = [(0i32, RID{page_id: 0, slot_num: 0}); 339];
    kv_data3[0] = (7, RID{page_id: 777, slot_num: 777});
    kv_data3[1] = (8, RID{page_id: 888, slot_num: 888});
    kv_data3[2] = (9, RID{page_id: 999, slot_num: 999});
    let mut root_page3 = BPlusTreeLeafPage{
        b_plus_tree_page: BPlusTreePage::new(3, 0, 339),
        next_page_id: INVALID_PAGE_ID,
        array: kv_data3
    };
    root_page3.b_plus_tree_page.size = 3;
    root_page3.set_page_type(LeafPage);
    let bytes3: &[u8; PAGE_SIZE] = unsafe {
        let data_ptr: *const BPlusTreeLeafPage = &root_page3;
        data_ptr.cast::<[u8; PAGE_SIZE]>().as_ref().unwrap()
    };
    let mut page3 = bpm.fetch_page(3);
    page3.as_mut().unwrap().data.copy_from_slice(bytes3);

    // B+ tree
    let mut tree = BPlusTree::new(String::from("my_tree"), bpm, 339, 509);

    let val0 = tree.get_value(0);
    assert!(val0.is_some());
    assert_eq!(val0.unwrap().page_id, 000);
    assert_eq!(val0.unwrap().slot_num, 000);

    let val2 = tree.get_value(2);
    assert!(val2.is_some());
    assert_eq!(val2.unwrap().page_id, 222);
    assert_eq!(val2.unwrap().slot_num, 222);

    let val3 = tree.get_value(3);
    assert!(val3.is_some());
    assert_eq!(val3.unwrap().page_id, 333);
    assert_eq!(val3.unwrap().slot_num, 333);

    let val6 = tree.get_value(6);
    assert!(val6.is_some());
    assert_eq!(val6.unwrap().page_id, 666);
    assert_eq!(val6.unwrap().slot_num, 666);

    let val7 = tree.get_value(7);
    assert!(val7.is_some());
    assert_eq!(val7.unwrap().page_id, 777);
    assert_eq!(val7.unwrap().slot_num, 777);
    
    let val9 = tree.get_value(9);
    assert!(val9.is_some());
    assert_eq!(val9.unwrap().page_id, 999);
    assert_eq!(val9.unwrap().slot_num, 999);

    tree.buffer_pool_manager.flush_all_pages();
}