// use std::fs::{File};
// use std::io::{prelude::*, BufReader};
// use std::sync::{Arc, RwLock};

// use crate::common::{PageID};
// use crate::storage::page::page::RawPage;

// pub struct PageFile {  // TODO: temporarily set public for debugging
//     pub filename: String,
//     // size
//     // mutex
// }

// #[derive(Default)]
// pub struct DiskManager {
//     // mutex
//     pub pages: Vec<PageFile>,  // TODO: temporarily set public for debugging
//     pub next_page_id: PageID  // TODO: temporarily set public for debugging
// }

// impl DiskManager {
//     pub fn new() -> Self {
//         let mut this = Self {
//             pages: Vec::new(),
//             next_page_id: 0,
//         };
        
//         // read the persisted page file names
//         let pagenames_file = File::open("data/pagenames").expect("opening `data/pagenames` failed");
//         let reader = BufReader::new(pagenames_file);
//         for line in reader.lines() {
//             this.pages.push(PageFile{filename:line.unwrap()});
//         }
//         this.next_page_id = this.pages.len() as PageID;

//         return this;
//     }

//     pub fn write_page_arc(&self, page_id: PageID, page_arc: Arc<RwLock<RawPage>>) {
//         let page = &self.pages[page_id as usize];
//         let filename = &page.filename;
//         let mut file = File::create(filename).expect("create failed");
//         let guard = page_arc.read().unwrap();
//         let data = guard.as_ref();
//         file.write_all(data).expect("write failed");
//     }

//     pub fn read_page_arc(&self, page_id: PageID, page_arc: Arc<RwLock<RawPage>>) {
//         let page = &self.pages[page_id as usize];
//         let filename = &page.filename;
//         let mut file = File::open(filename).expect("open failed");
//         let mut guard = page_arc.write().unwrap();
//         let data = guard.as_mut();
//         file.read(data).expect("read failed");
//     }

//     pub fn write_page(&self, page_id: PageID, page_data: &[u8]) {
//         let page = &self.pages[page_id as usize];
//         let filename = &page.filename;
//         let mut file = File::create(filename).expect("create failed");
//         file.write_all(page_data).expect("write failed");
//     }

//     pub fn read_page(&self, page_id: PageID, page_data: &mut [u8]) {
//         let page = &self.pages[page_id as usize];
//         let filename = &page.filename;
//         let mut file = File::open(filename).expect("open failed");
//         file.read(page_data).expect("read failed");
//     }
// }