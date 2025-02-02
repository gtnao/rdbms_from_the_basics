use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
};

const PAGE_SIZE: usize = 16;

struct Page {
    bytes: [u8; PAGE_SIZE],
}

impl Page {
    const HEADER_SIZE: usize = 2;
    const MAX_TUPLE_LENGTH: u8 = PAGE_SIZE as u8 - Self::HEADER_SIZE as u8;
    fn init(page_id: u8) -> Self {
        let mut bytes = [0; PAGE_SIZE];
        bytes[0] = page_id;
        Self { bytes }
    }
    fn load(bytes: [u8; PAGE_SIZE]) -> Self {
        Self { bytes }
    }
    fn page_id(&self) -> u8 {
        self.bytes[0]
    }
    fn tuple_length(&self) -> u8 {
        self.bytes[1]
    }
    fn read_tuples(&self) -> &[u8] {
        &self.bytes[Self::HEADER_SIZE..(self.tuple_length() as usize + Self::HEADER_SIZE)]
    }
    fn read_tuple(&self, index: u8) -> u8 {
        self.bytes[index as usize + Self::HEADER_SIZE]
    }
    fn has_space(&self) -> bool {
        self.tuple_length() < Self::MAX_TUPLE_LENGTH
    }
    fn insert_tuple(&mut self, tuple: u8) {
        self.bytes[self.tuple_length() as usize + Self::HEADER_SIZE] = tuple;
        self.bytes[1] += 1;
    }
}

struct PageManager {
    file: File,
}

impl PageManager {
    fn init(file_name: &str) -> Self {
        Self {
            file: OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(file_name)
                .unwrap(),
        }
    }
    fn load(file_name: &str) -> Self {
        Self {
            file: OpenOptions::new()
                .read(true)
                .write(true)
                .open(file_name)
                .unwrap(),
        }
    }
    fn write_page(&mut self, page: &Page) {
        let offset = page.page_id() as u64 * PAGE_SIZE as u64;
        self.file.seek(SeekFrom::Start(offset)).unwrap();
        self.file.write_all(&page.bytes).unwrap();
        self.file.sync_all().unwrap();
    }
    fn read_page(&mut self, page_id: u8) -> Page {
        let offset = page_id as u64 * PAGE_SIZE as u64;
        self.file.seek(SeekFrom::Start(offset)).unwrap();
        let mut bytes = [0; PAGE_SIZE];
        self.file.read_exact(&mut bytes).unwrap();
        Page::load(bytes)
    }
    fn allocate_page(&mut self) -> u8 {
        let page_id = self.next_page_id();
        let page = Page::init(page_id);
        self.write_page(&page);
        page_id
    }
    fn next_page_id(&self) -> u8 {
        let metadata = self.file.metadata().unwrap();
        (metadata.len() / PAGE_SIZE as u64) as u8
    }
}

struct Database {
    page_manager: PageManager,
    last_page_id: u8,
}

impl Database {
    fn init(file_name: &str) -> Self {
        let mut page_manager = PageManager::init(file_name);
        page_manager.allocate_page();
        Self {
            page_manager,
            last_page_id: 0,
        }
    }
    fn load(file_name: &str) -> Self {
        let page_manager = PageManager::load(file_name);
        let last_page_id = page_manager.next_page_id() - 1;
        Self {
            page_manager,
            last_page_id,
        }
    }
    fn insert(&mut self, tuple: u8) {
        let page_id = self.last_page_id;
        let mut page = self.page_manager.read_page(page_id);
        if !page.has_space() {
            let next_page_id = self.page_manager.allocate_page();
            self.last_page_id = next_page_id;
            page = self.page_manager.read_page(next_page_id);
        }
        page.insert_tuple(tuple);
        self.page_manager.write_page(&page);
    }
    fn read_all(&mut self) -> Vec<u8> {
        let mut values = Vec::new();
        for page_id in 0..=self.last_page_id {
            let page = self.page_manager.read_page(page_id);
            values.extend_from_slice(page.read_tuples());
        }
        values
    }
    fn read(&mut self, index: usize) -> u8 {
        let page_id = index / Page::MAX_TUPLE_LENGTH as usize;
        let page = self.page_manager.read_page(page_id as u8);
        page.read_tuple(index as u8 % PAGE_SIZE as u8)
    }
}

fn main() {
    let mut database = Database::init("db");
    database.insert(0);
    println!("Insert 0");
    database.insert(1);
    println!("Insert 1");
    let values = database.read_all();
    println!("Read all");
    println!("  values: {:?}", values);
    for i in 2..100 {
        database.insert(i);
    }
    println!("Insert 2..100");
    let values = database.read_all();
    println!("Read all");
    println!("  values: {:?}", values);
    println!("last_page_id: {}", database.last_page_id);
    let value = database.read(50);
    println!("Read index 50");
    println!("  value: {:?}", value);

    println!("__________________________");
    println!("Open existing database.");
    let mut database = Database::load("db");
    let values = database.read_all();
    println!("Read all");
    println!("  values: {:?}", values);
}
