use gman_db::{storage::disk::disk_manager::{DiskManager}, common::{PageID, PAGE_SIZE}};

#[test]
fn test_constructor() {
    let disk_manager = DiskManager::new();

    println!("currently has {} pages", disk_manager.next_page_id);
    for page_file in disk_manager.pages {
        println!("{}", page_file.filename);
    }
}

#[test]
fn test_write_page() {
    let page_id: PageID = 0;
    let page_data = "hello, world!".as_bytes();

    let disk_manager = DiskManager::new();
    disk_manager.write_page(page_id, page_data);
}

#[test]
fn test_read_page() {
    let page_id: PageID = 0;
    let mut page_data: [u8; PAGE_SIZE] = [0; PAGE_SIZE];

    let disk_manager = DiskManager::new();
    disk_manager.read_page(page_id, &mut page_data);
    let content = std::str::from_utf8_mut(&mut page_data).expect("convert failed");
    assert_eq!(&content[..13], "hello, world!");
}