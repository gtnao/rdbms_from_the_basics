use std::{
    collections::{HashMap, VecDeque},
    fmt::Debug,
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    sync::{Arc, RwLock},
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

struct BufferPoolManager {
    page_manager: PageManager,
    max_frame_length: usize,
    frames: Vec<Frame>,
    page_frame_table: HashMap<u8, usize>,
    replacer: Replacer,
}

struct Frame {
    page: Arc<RwLock<Page>>,
    page_id: u8,
    pin_count: usize,
}

impl BufferPoolManager {
    fn new(page_manager: PageManager, max_frame_length: usize) -> Self {
        Self {
            page_manager,
            max_frame_length,
            frames: Vec::with_capacity(max_frame_length),
            page_frame_table: HashMap::new(),
            replacer: Replacer::new(),
        }
    }
    fn read_page(&mut self, page_id: u8) -> Arc<RwLock<Page>> {
        if let Some(frame_id) = self.page_frame_table.get(&page_id) {
            let frame = &mut self.frames[*frame_id];
            frame.pin_count += 1;
            self.replacer.pin(*frame_id);
            frame.page.clone()
        } else if self.frames.len() < self.max_frame_length {
            self.frames.push(Frame {
                page: Arc::new(RwLock::new(self.page_manager.read_page(page_id))),
                page_id,
                pin_count: 1,
            });
            let frame_id = self.frames.len() - 1;
            self.page_frame_table.insert(page_id, frame_id);
            self.replacer.pin(frame_id);
            self.frames[frame_id].page.clone()
        } else {
            let victim_frame_id = self.replacer.victim();
            self.page_frame_table
                .remove(&self.frames[victim_frame_id].page_id);
            self.frames[victim_frame_id] = Frame {
                page: Arc::new(RwLock::new(self.page_manager.read_page(page_id))),
                page_id,
                pin_count: 1,
            };
            self.page_frame_table.insert(page_id, victim_frame_id);
            self.replacer.pin(victim_frame_id);
            self.frames[victim_frame_id].page.clone()
        }
    }
    fn allocate_page(&mut self) -> Arc<RwLock<Page>> {
        let page_id = self.page_manager.allocate_page();
        self.read_page(page_id)
    }
    fn unpin_page(&mut self, page_id: u8, is_dirty: bool) {
        let frame_id = *self.page_frame_table.get(&page_id).unwrap();
        let frame = &mut self.frames[frame_id];
        frame.pin_count -= 1;
        if frame.pin_count == 0 {
            self.replacer.unpin(frame_id);
        }
        if is_dirty {
            self.page_manager.write_page(&frame.page.read().unwrap());
        }
    }
}

impl Debug for BufferPoolManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "BufferPoolManager")?;
        writeln!(f, "  max_frame_length: {:?}", self.max_frame_length)?;
        writeln!(f, "  frames:")?;
        for (i, frame) in self.frames.iter().enumerate() {
            writeln!(f, "    {} => page: {:?}", i, frame.page_id)?;
        }
        Ok(())
    }
}

struct Replacer {
    queue: VecDeque<usize>,
}

impl Replacer {
    fn new() -> Self {
        Self {
            queue: VecDeque::new(),
        }
    }
    fn victim(&mut self) -> usize {
        self.queue.pop_front().unwrap()
    }
    fn unpin(&mut self, frame_index: usize) {
        if let Some(index) = self.queue.iter().position(|&x| x == frame_index) {
            self.queue.remove(index);
        }
        self.queue.push_back(frame_index);
    }
    fn pin(&mut self, frame_index: usize) {
        if let Some(index) = self.queue.iter().position(|&x| x == frame_index) {
            self.queue.remove(index);
        }
    }
}

struct Database {
    buffer_pool_manager: BufferPoolManager,
    last_page_id: u8,
}

impl Database {
    fn init(file_name: &str, buffer_pool_max_frame_length: usize) -> Self {
        let mut page_manager = PageManager::init(file_name);
        page_manager.allocate_page();
        Self {
            buffer_pool_manager: BufferPoolManager::new(page_manager, buffer_pool_max_frame_length),
            last_page_id: 0,
        }
    }
    fn load(file_name: &str, buffer_pool_max_frame_length: usize) -> Self {
        let page_manager = PageManager::load(file_name);
        let last_page_id = page_manager.next_page_id() - 1;
        Self {
            buffer_pool_manager: BufferPoolManager::new(page_manager, buffer_pool_max_frame_length),
            last_page_id,
        }
    }
    fn insert(&mut self, tuple: u8) {
        let page_id = self.last_page_id;
        let page = self.buffer_pool_manager.read_page(page_id);
        {
            let mut page = page.write().unwrap();
            if page.has_space() {
                page.insert_tuple(tuple);
            } else {
                let new_page = self.buffer_pool_manager.allocate_page();
                let new_page_id = {
                    let mut new_page = new_page.write().unwrap();
                    let new_page_id = new_page.page_id();
                    new_page.insert_tuple(tuple);
                    new_page_id
                };
                self.buffer_pool_manager.unpin_page(new_page_id, true);
                self.last_page_id = new_page_id;
            }
        }
        self.buffer_pool_manager.unpin_page(page_id, true);
    }
    fn read_all(&mut self) -> Vec<u8> {
        let mut values = Vec::new();
        let mut page_id = 0;
        loop {
            let page = self.buffer_pool_manager.read_page(page_id);
            {
                let page = page.read().unwrap();
                values.extend_from_slice(page.read_tuples());
            }
            self.buffer_pool_manager.unpin_page(page_id, false);
            if self.last_page_id > page_id {
                page_id += 1;
            } else {
                break;
            }
        }
        values
    }
    fn read(&mut self, index: usize) -> u8 {
        let page_id = (index / Page::MAX_TUPLE_LENGTH as usize) as u8;
        let page = self.buffer_pool_manager.read_page(page_id);
        let value = {
            let page = page.read().unwrap();
            page.read_tuple(index as u8 % Page::MAX_TUPLE_LENGTH)
        };
        self.buffer_pool_manager.unpin_page(page_id, false);
        value
    }
}

impl Debug for Database {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "{:?}", self.buffer_pool_manager)?;
        Ok(())
    }
}

fn main() {
    example(2);
    example(3);
    // A error happens when a tuple is inserted across multiple pages.
    // example(1);

    println!("______________________");
    println!("Open existing database.");
    let mut database = Database::load("db", 2);
    let values = database.read_all();
    println!("Read all");
    println!("  values: {:?}", values);
}

fn example(max_frame_length: usize) {
    println!("______________________");
    let mut database = Database::init("db", max_frame_length);
    println!("{:?}", database);
    database.insert(0);
    println!("Insert 0");
    println!("{:?}", database);
    for i in 1..16 {
        database.insert(i);
    }
    println!("Insert 1..16");
    println!("{:?}", database);
    for i in 16..29 {
        database.insert(i);
    }
    println!("Insert 16..29");
    println!("{:?}", database);
    for i in 29..100 {
        database.insert(i);
    }
    println!("Insert 29..100");
    println!("{:?}", database);
    let v = database.read(0);
    println!("Read index 0(0-based)");
    println!("  value: {:?}", v);
    println!("{:?}", database);
}
