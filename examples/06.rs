use std::{
    collections::{HashMap, VecDeque},
    fmt::Debug,
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    sync::{Arc, RwLock},
};

#[derive(Clone, Debug)]
struct Log {
    lsn: u8,
    log_type: LogType,
}

#[derive(Clone, Debug)]
enum LogType {
    Begin(BeginLog),
    Commit(CommitLog),
    Abort(AbortLog),
    Insert(InsertLog),
    CompensateInsert(CompensateInsertLog),
}

#[derive(Clone, Debug)]
struct BeginLog {
    transaction_id: u8,
}

#[derive(Clone, Debug)]
struct CommitLog {
    transaction_id: u8,
}

#[derive(Clone, Debug)]
struct AbortLog {
    transaction_id: u8,
}

#[derive(Clone, Debug)]
struct InsertLog {
    prev_lsn: u8,
    transaction_id: u8,
    page_id: u8,
    slot_id: u8,
    tuple: u8,
}

#[derive(Clone, Debug)]
struct CompensateInsertLog {
    next_compenstate_lsn: u8,
    transaction_id: u8,
    page_id: u8,
    slot_id: u8,
}

const BEGIN_LOG_TYPE: u8 = 0;
const COMMIT_LOG_TYPE: u8 = 1;
const ABORT_LOG_TYPE: u8 = 2;
const INSERT_LOG_TYPE: u8 = 3;
const COMPENSATE_INSERT_LOG_TYPE: u8 = 4;

impl Log {
    fn serialize(&self) -> Vec<u8> {
        let mut bytes = vec![self.lsn];
        match self.log_type {
            LogType::Begin(ref commit_log) => {
                bytes.push(BEGIN_LOG_TYPE);
                bytes.push(commit_log.transaction_id);
            }
            LogType::Commit(ref commit_log) => {
                bytes.push(COMMIT_LOG_TYPE);
                bytes.push(commit_log.transaction_id);
            }
            LogType::Abort(ref abort_log) => {
                bytes.push(ABORT_LOG_TYPE);
                bytes.push(abort_log.transaction_id);
            }
            LogType::Insert(ref insert_log) => {
                bytes.push(INSERT_LOG_TYPE);
                bytes.push(insert_log.prev_lsn);
                bytes.push(insert_log.transaction_id);
                bytes.push(insert_log.page_id);
                bytes.push(insert_log.slot_id);
                bytes.push(insert_log.tuple);
            }
            LogType::CompensateInsert(ref compensate_insert_log) => {
                bytes.push(COMPENSATE_INSERT_LOG_TYPE);
                bytes.push(compensate_insert_log.next_compenstate_lsn);
                bytes.push(compensate_insert_log.transaction_id);
                bytes.push(compensate_insert_log.page_id);
                bytes.push(compensate_insert_log.slot_id);
            }
        }
        bytes
    }
    fn deserialize(bytes: &[u8]) -> (Self, usize) {
        let lsn = bytes[0];
        let (log_type, log_size) = match bytes[1] {
            BEGIN_LOG_TYPE => {
                let transaction_id = bytes[2];
                (LogType::Begin(BeginLog { transaction_id }), 3)
            }
            COMMIT_LOG_TYPE => {
                let transaction_id = bytes[2];
                (LogType::Commit(CommitLog { transaction_id }), 3)
            }
            ABORT_LOG_TYPE => {
                let transaction_id = bytes[2];
                (LogType::Abort(AbortLog { transaction_id }), 3)
            }
            INSERT_LOG_TYPE => {
                let prev_lsn = bytes[2];
                let transaction_id = bytes[3];
                let page_id = bytes[4];
                let slot_id = bytes[5];
                let tuple = bytes[6];
                (
                    LogType::Insert(InsertLog {
                        prev_lsn,
                        transaction_id,
                        page_id,
                        slot_id,
                        tuple,
                    }),
                    7,
                )
            }
            COMPENSATE_INSERT_LOG_TYPE => {
                let next_compenstate_lsn = bytes[2];
                let transaction_id = bytes[3];
                let page_id = bytes[4];
                let slot_id = bytes[5];
                (
                    LogType::CompensateInsert(CompensateInsertLog {
                        next_compenstate_lsn,
                        transaction_id,
                        page_id,
                        slot_id,
                    }),
                    6,
                )
            }
            _ => panic!("Unknown log type"),
        };
        (Self { lsn, log_type }, log_size)
    }
}

struct LogManager {
    file: File,
    current_lsn: u8,
    buffer: Vec<Log>,
}

impl LogManager {
    fn init(file_name: &str) -> Self {
        Self {
            file: OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(file_name)
                .unwrap(),
            current_lsn: 0,
            buffer: Vec::new(),
        }
    }
    fn load(file_name: &str) -> Self {
        let mut manager = Self {
            file: OpenOptions::new()
                .read(true)
                .write(true)
                .open(file_name)
                .unwrap(),
            current_lsn: 0,
            buffer: Vec::new(),
        };
        let logs = manager.read();
        let current_lsn = logs.last().map_or(0, |log| log.lsn);
        manager.current_lsn = current_lsn;
        manager
    }
    fn read(&mut self) -> Vec<Log> {
        self.file.seek(SeekFrom::Start(0)).unwrap();
        let mut bytes = Vec::new();
        self.file.read_to_end(&mut bytes).unwrap();
        let mut logs = Vec::new();
        loop {
            if bytes.is_empty() {
                break;
            }
            let (log, log_size) = Log::deserialize(&bytes);
            logs.push(log);
            bytes = bytes.split_off(log_size);
        }
        logs
    }
    fn append(&mut self, log_type: LogType) -> Log {
        let log = Log {
            lsn: self.current_lsn,
            log_type,
        };
        self.current_lsn += 1;
        self.buffer.push(log.clone());
        log.clone()
    }
    fn flush(&mut self) {
        self.file.seek(SeekFrom::End(0)).unwrap();
        let bytes: Vec<u8> = self.buffer.iter().flat_map(|log| log.serialize()).collect();
        self.file.write_all(&bytes).unwrap();
        self.file.sync_all().unwrap();
        self.buffer.clear();
    }
}

struct RecoveryManager {
    log_manager: Arc<RwLock<LogManager>>,
    buffer_pool_manager: Arc<RwLock<BufferPoolManager>>,
    // tx_id -> last_lsn
    transaction_table: HashMap<u8, u8>,
    max_transaction_id: u8,
}

impl RecoveryManager {
    fn new(
        log_manager: Arc<RwLock<LogManager>>,
        buffer_pool_manager: Arc<RwLock<BufferPoolManager>>,
    ) -> Self {
        Self {
            log_manager,
            buffer_pool_manager,
            transaction_table: HashMap::new(),
            max_transaction_id: 0,
        }
    }

    fn run(&mut self) -> u8 {
        let logs = self.log_manager.write().unwrap().read();
        self.analyze(&logs);
        self.redo(&logs);
        self.undo(&logs);
        self.max_transaction_id
    }

    fn analyze(&mut self, logs: &[Log]) {
        for log in logs {
            match log.log_type {
                LogType::Insert(ref log_type) => {
                    self.transaction_table
                        .entry(log_type.transaction_id)
                        .or_insert(log.lsn);
                    self.transaction_table
                        .entry(log_type.transaction_id)
                        .and_modify(|e| {
                            if *e < log.lsn {
                                *e = log.lsn;
                            }
                        });
                    self.max_transaction_id = self.max_transaction_id.max(log_type.transaction_id);
                }
                LogType::CompensateInsert(ref log_type) => {
                    self.transaction_table
                        .entry(log_type.transaction_id)
                        .or_insert(log.lsn);
                    self.transaction_table
                        .entry(log_type.transaction_id)
                        .and_modify(|e| {
                            if *e < log.lsn {
                                *e = log.lsn;
                            }
                        });
                    self.max_transaction_id = self.max_transaction_id.max(log_type.transaction_id);
                }
                LogType::Begin(ref log_type) => {
                    self.transaction_table
                        .entry(log_type.transaction_id)
                        .or_insert(log.lsn);
                    self.transaction_table
                        .entry(log_type.transaction_id)
                        .and_modify(|e| {
                            if *e < log.lsn {
                                *e = log.lsn;
                            }
                        });
                    self.max_transaction_id = self.max_transaction_id.max(log_type.transaction_id);
                }
                LogType::Commit(ref commit_log) => {
                    self.transaction_table.remove(&commit_log.transaction_id);
                    self.max_transaction_id =
                        self.max_transaction_id.max(commit_log.transaction_id);
                }
                LogType::Abort(ref abort_log) => {
                    self.transaction_table.remove(&abort_log.transaction_id);
                    self.max_transaction_id = self.max_transaction_id.max(abort_log.transaction_id);
                }
            }
        }
    }

    fn redo(&self, logs: &[Log]) {
        for log in logs {
            match log.log_type {
                LogType::Insert(ref insert_log) => {
                    let page_arc = self
                        .buffer_pool_manager
                        .write()
                        .unwrap()
                        .read_page(insert_log.page_id);
                    let mut is_dirty = false;
                    {
                        let mut page = page_arc.write().unwrap();
                        if page.page_lsn() < log.lsn {
                            page.insert_tuple(insert_log.tuple, None);
                            is_dirty = true;
                        }
                    }
                    self.buffer_pool_manager
                        .write()
                        .unwrap()
                        .unpin_page(insert_log.page_id, is_dirty);
                }
                LogType::CompensateInsert(ref compensate_insert_log) => {
                    let page_arc = self
                        .buffer_pool_manager
                        .write()
                        .unwrap()
                        .read_page(compensate_insert_log.page_id);
                    let mut is_dirty = false;
                    {
                        let mut page = page_arc.write().unwrap();
                        if page.page_lsn() < log.lsn {
                            page.rollback_insert(compensate_insert_log.slot_id, None);
                            is_dirty = true;
                        }
                    }
                    self.buffer_pool_manager
                        .write()
                        .unwrap()
                        .unpin_page(compensate_insert_log.page_id, is_dirty);
                }
                _ => {}
            }
        }
    }

    fn undo(&self, logs: &[Log]) {
        let mut lsn_table = HashMap::new();
        for (i, log) in logs.iter().enumerate() {
            lsn_table.insert(log.lsn, i);
        }
        for (_, last_lsn) in self.transaction_table.iter() {
            let mut lsn = *last_lsn;
            let log_index = lsn_table[&lsn];
            match logs[log_index].log_type {
                LogType::CompensateInsert(ref compensate_insert_log) => {
                    lsn = compensate_insert_log.next_compenstate_lsn;
                }
                LogType::Insert(_) => {}
                LogType::Begin(_) => {}
                LogType::Commit(_) => {}
                LogType::Abort(_) => {}
            }
            loop {
                let log_index = lsn_table[&lsn];
                let log = &logs[log_index];
                match &log.log_type {
                    LogType::Insert(insert_log) => {
                        let page_arc = self
                            .buffer_pool_manager
                            .write()
                            .unwrap()
                            .read_page(insert_log.page_id);
                        {
                            let mut page = page_arc.write().unwrap();
                            page.rollback_insert(insert_log.slot_id, None);
                        }
                        self.buffer_pool_manager
                            .write()
                            .unwrap()
                            .read_page(insert_log.page_id);
                        lsn = insert_log.prev_lsn;
                    }
                    LogType::Begin(_) => {
                        break;
                    }
                    _ => {}
                }
            }
        }
    }
}

struct Transaction {
    transaction_id: u8,
    log_manager: Arc<RwLock<LogManager>>,
    logs: Vec<Log>,
}

impl Transaction {
    fn new(transaction_id: u8, log_manager: Arc<RwLock<LogManager>>) -> Self {
        Self {
            transaction_id,
            log_manager,
            logs: Vec::new(),
        }
    }
    fn log_insert(&mut self, page_id: u8, slot_id: u8, tuple: u8) -> u8 {
        let log = self
            .log_manager
            .write()
            .unwrap()
            .append(LogType::Insert(InsertLog {
                prev_lsn: self.prev_lsn(),
                transaction_id: self.transaction_id,
                page_id,
                slot_id,
                tuple,
            }));
        let lsn = log.lsn;
        self.logs.push(log);
        lsn
    }
    fn log_compensate_insert(&mut self, page_id: u8, slot_id: u8, next_lsn: u8) -> u8 {
        let log = self
            .log_manager
            .write()
            .unwrap()
            .append(LogType::CompensateInsert(CompensateInsertLog {
                next_compenstate_lsn: next_lsn,
                transaction_id: self.transaction_id,
                page_id,
                slot_id,
            }));
        let lsn = log.lsn;
        self.logs.push(log);
        lsn
    }
    fn log_begin(&mut self) {
        let log = self
            .log_manager
            .write()
            .unwrap()
            .append(LogType::Begin(BeginLog {
                transaction_id: self.transaction_id,
            }));
        self.logs.push(log);
    }
    fn log_commit(&mut self) {
        let log = self
            .log_manager
            .write()
            .unwrap()
            .append(LogType::Commit(CommitLog {
                transaction_id: self.transaction_id,
            }));
        self.logs.push(log);
    }
    fn log_abort(&mut self) {
        let log = self
            .log_manager
            .write()
            .unwrap()
            .append(LogType::Abort(AbortLog {
                transaction_id: self.transaction_id,
            }));
        self.logs.push(log);
    }
    fn prev_lsn(&self) -> u8 {
        self.logs.last().unwrap().lsn
    }
}

const PAGE_SIZE: usize = 16;

struct Page {
    bytes: [u8; PAGE_SIZE],
}

impl Page {
    const HEADER_SIZE: usize = 3;
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
    fn page_lsn(&self) -> u8 {
        self.bytes[1]
    }
    fn tuple_length(&self) -> u8 {
        self.bytes[2]
    }
    fn read_tuples(&self) -> &[u8] {
        &self.bytes[Self::HEADER_SIZE..(self.tuple_length() as usize + Self::HEADER_SIZE)]
    }
    fn has_space(&self) -> bool {
        self.tuple_length() < Self::MAX_TUPLE_LENGTH
    }
    fn insert_tuple(&mut self, tuple: u8, transaction: Option<&mut Transaction>) {
        let slot_id = self.tuple_length();
        if let Some(transaction) = transaction {
            let lsn = transaction.log_insert(self.page_id(), slot_id, tuple);
            self.bytes[1] = lsn;
        }
        self.bytes[slot_id as usize + Self::HEADER_SIZE] = tuple;
        self.bytes[2] += 1;
    }
    fn rollback_insert(
        &mut self,
        slot_id: u8,
        transaction_with_next_lsn: Option<(&mut Transaction, u8)>,
    ) {
        if let Some((transaction, next_lsn)) = transaction_with_next_lsn {
            let lsn = transaction.log_compensate_insert(self.page_id(), slot_id, next_lsn);
            self.bytes[1] = lsn;
        }
        self.bytes[2] -= 1;
        for i in slot_id..self.tuple_length() {
            self.bytes[i as usize + Self::HEADER_SIZE] =
                self.bytes[i as usize + Self::HEADER_SIZE + 1];
        }
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
    is_dirty: bool,
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
                is_dirty: false,
            });
            let frame_id = self.frames.len() - 1;
            self.page_frame_table.insert(page_id, frame_id);
            self.replacer.pin(frame_id);
            self.frames[frame_id].page.clone()
        } else {
            let victim_frame_id = self.replacer.victim();
            if self.frames[victim_frame_id].is_dirty {
                let page = self.frames[victim_frame_id].page.read().unwrap();
                self.page_manager.write_page(&page);
            }
            self.page_frame_table
                .remove(&self.frames[victim_frame_id].page_id);
            self.frames[victim_frame_id] = Frame {
                page: Arc::new(RwLock::new(self.page_manager.read_page(page_id))),
                page_id,
                pin_count: 1,
                is_dirty: false,
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
            self.frames[frame_id].is_dirty = true;
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
    log_manager: Arc<RwLock<LogManager>>,
    buffer_pool_manager: Arc<RwLock<BufferPoolManager>>,
    current_transaction_id: u8,
    last_page_id: u8,
}

impl Database {
    fn init(file_name: &str, log_file_name: &str, buffer_pool_max_frame_length: usize) -> Self {
        let mut page_manager = PageManager::init(file_name);
        page_manager.allocate_page();
        Self {
            log_manager: Arc::new(RwLock::new(LogManager::init(log_file_name))),
            buffer_pool_manager: Arc::new(RwLock::new(BufferPoolManager::new(
                page_manager,
                buffer_pool_max_frame_length,
            ))),
            current_transaction_id: 0,
            last_page_id: 0,
        }
    }
    fn load(file_name: &str, log_file_name: &str, buffer_pool_max_frame_length: usize) -> Self {
        let log_manager = Arc::new(RwLock::new(LogManager::load(log_file_name)));
        let page_manager = PageManager::load(file_name);
        let last_page_id = page_manager.next_page_id() - 1;
        let buffer_pool_manager = Arc::new(RwLock::new(BufferPoolManager::new(
            page_manager,
            buffer_pool_max_frame_length,
        )));
        let mut recovery_manager =
            RecoveryManager::new(log_manager.clone(), buffer_pool_manager.clone());
        let max_transaction_id = recovery_manager.run();
        Self {
            log_manager,
            buffer_pool_manager,
            current_transaction_id: max_transaction_id + 1,
            last_page_id,
        }
    }
    fn begin(&mut self) -> Transaction {
        let mut transaction =
            Transaction::new(self.current_transaction_id, self.log_manager.clone());
        transaction.log_begin();
        self.current_transaction_id += 1;
        transaction
    }
    fn commit(&mut self, transaction: &mut Transaction) {
        transaction.log_commit();
        self.log_manager.write().unwrap().flush();
    }
    fn abort(&mut self, transaction: &mut Transaction) {
        let logs = transaction.logs.clone();
        for (i, log) in logs.iter().rev().enumerate() {
            match &log.log_type {
                LogType::Insert(ref insert_log) => {
                    let page = self
                        .buffer_pool_manager
                        .write()
                        .unwrap()
                        .read_page(insert_log.page_id);
                    {
                        let mut page = page.write().unwrap();
                        page.rollback_insert(
                            insert_log.slot_id,
                            Some((transaction, logs[i + 1].lsn)),
                        );
                    }
                    {
                        let mut page = page.write().unwrap();
                        page.rollback_insert(
                            insert_log.slot_id,
                            Some((transaction, logs[i + 1].lsn)),
                        );
                    }
                    self.buffer_pool_manager
                        .write()
                        .unwrap()
                        .unpin_page(insert_log.page_id, true);
                }
                LogType::CompensateInsert(_) => {}
                LogType::Begin(_) => {}
                LogType::Commit(_) => {}
                LogType::Abort(_) => {}
            }
        }
        transaction.log_abort();
        self.log_manager.write().unwrap().flush();
    }
    fn insert(&mut self, transaction: &mut Transaction, tuple: u8) {
        let page_id = self.last_page_id;
        let page = self.buffer_pool_manager.write().unwrap().read_page(page_id);
        {
            let mut page = page.write().unwrap();
            if page.has_space() {
                page.insert_tuple(tuple, Some(transaction));
            } else {
                let new_page = self.buffer_pool_manager.write().unwrap().allocate_page();
                let new_page_id = {
                    let mut new_page = new_page.write().unwrap();
                    let new_page_id = new_page.page_id();
                    new_page.insert_tuple(tuple, Some(transaction));
                    new_page_id
                };
                self.buffer_pool_manager
                    .write()
                    .unwrap()
                    .unpin_page(new_page_id, true);
                self.last_page_id = new_page_id;
            }
        }
        self.buffer_pool_manager
            .write()
            .unwrap()
            .unpin_page(page_id, true);
    }
    fn read_all(&mut self) -> Vec<u8> {
        let mut values = Vec::new();
        let mut page_id = 0;
        loop {
            let page = self.buffer_pool_manager.write().unwrap().read_page(page_id);
            {
                let page = page.read().unwrap();
                values.extend_from_slice(page.read_tuples());
            }
            self.buffer_pool_manager
                .write()
                .unwrap()
                .unpin_page(page_id, false);
            if self.last_page_id > page_id {
                page_id += 1;
            } else {
                break;
            }
        }
        values
    }
}

impl Debug for Database {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "{:?}", self.buffer_pool_manager)?;
        Ok(())
    }
}
fn main() {
    println!("<prev_example>");
    prev_example();
    println!("<concurrent_example>");
    concurrent_example();
}

fn prev_example() {
    let mut database = Database::init("db", "log", 10);

    println!("______________________");
    let mut transaction = database.begin();
    println!("Start transaction");
    database.insert(&mut transaction, 10);
    println!("Insert 10");
    database.insert(&mut transaction, 20);
    println!("Insert 20");
    database.commit(&mut transaction);
    println!("Commit\n");

    let values = database.read_all();
    println!("Read all");
    println!("  values: {:?}\n", values);

    let mut transaction = database.begin();
    println!("Start transaction");
    database.insert(&mut transaction, 30);
    println!("Insert 30");
    let values = database.read_all();
    println!("Read all");
    println!("  values: {:?}", values);
    database.abort(&mut transaction);
    println!("Abort\n");

    let values = database.read_all();
    println!("Read all");
    println!("  values: {:?}\n", values);

    let mut transaction = database.begin();
    println!("Start transaction");
    database.insert(&mut transaction, 40);
    println!("Insert 40");
    let values = database.read_all();
    println!("Read all");
    println!("  values: {:?}", values);
    println!("Not commit and shutdown.\n");

    println!("______________________");
    println!("Open existing database.");
    let mut database = Database::load("db", "log", 10);
    let values = database.read_all();
    println!("Read all");
    println!("  values: {:?}", values);
}

fn concurrent_example() {
    let mut database = Database::init("db", "log", 10);

    println!("______________________");
    let mut transaction1 = database.begin();
    println!("Start transaction1");
    let mut transaction2 = database.begin();
    println!("Start transaction2");

    database.insert(&mut transaction1, 10);
    println!("Insert 10 by transaction1");
    database.insert(&mut transaction2, 20);
    println!("Insert 20 by transaction2");

    database.commit(&mut transaction1);
    println!("Commit transaction1");
    println!("Not commit transaction2 and shutdown.\n");

    let mut database = Database::load("db", "log", 10);
    let values = database.read_all();
    println!("Read all");
    println!("  values: {:?}", values);
}
