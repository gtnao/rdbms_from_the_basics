use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
};

struct Database {
    file: File,
}

impl Database {
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
    fn write(&mut self, value: u8) {
        self.file.seek(SeekFrom::End(0)).unwrap();
        self.file.write_all(&[value]).unwrap();
        self.file.sync_all().unwrap();
    }
    fn read_all(&mut self) -> Vec<u8> {
        self.file.seek(SeekFrom::Start(0)).unwrap();
        let mut values = Vec::new();
        self.file.read_to_end(&mut values).unwrap();
        values
    }
    fn read(&mut self, index: usize) -> u8 {
        self.file.seek(SeekFrom::Start(index as u64)).unwrap();
        let mut values = [0];
        self.file.read_exact(&mut values).unwrap();
        values[0]
    }
}

fn main() {
    let mut database = Database::init("db");
    database.write(10);
    println!("Write 10");
    database.write(20);
    println!("Write 20");
    let values = database.read_all();
    println!("Read all");
    println!("  values: {:?}", values);
    database.write(30);
    println!("Write 30");
    let values = database.read_all();
    println!("Read all");
    println!("  values: {:?}", values);
    println!("Read index 1(0-based)");
    let value = database.read(1);
    println!("  value: {:?}", value);

    println!("__________________________");
    println!("Load existing database.");
    let mut database = Database::load("db");
    let values = database.read_all();
    println!("Read all");
    println!("  values: {:?}", values);
}
