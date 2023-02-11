use gman_db::storage::disk::disk_manager::{DiskManager};

fn main() {
    let disk_manager = DiskManager::new();

    println!("currently has {} pages", disk_manager.next_page_id);
    for page_file in disk_manager.pages {
        println!("{}", page_file.filename);
    }
}
