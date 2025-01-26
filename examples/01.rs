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
    pub fn load(file_name: &str) -> Self {
        Self {
            file: OpenOptions::new()
                .read(true)
                .write(true)
                .open(file_name)
                .unwrap(),
        }
    }
    fn append(&mut self, value: u8) {
        self.file.seek(SeekFrom::End(0)).unwrap();
        self.file.write_all(&[value]).unwrap();
        self.file.flush().unwrap();
    }
    fn read_all(&mut self) -> Vec<u8> {
        let mut values = Vec::new();
        self.file.seek(SeekFrom::Start(0)).unwrap();
        self.file.read_to_end(&mut values).unwrap();
        values
    }
}

fn main() {
    let mut database = Database::init("db");
    database.append(10);
    println!("Append 10");
    database.append(20);
    println!("Append 20");
    let values = database.read_all();
    println!("Read all");
    println!("  values: {:?}", values);
    database.append(30);
    println!("Append 30");
    let values = database.read_all();
    println!("Read all");
    println!("  values: {:?}", values);

    println!("__________________________");
    println!("Load existing database.");
    let mut database = Database::load("db");
    let values = database.read_all();
    println!("Read all");
    println!("  values: {:?}", values);
}
