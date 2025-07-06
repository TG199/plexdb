use crate::error::PlexError;
use crate::engine::partition_manager::FileOffset;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::fs::create_dir_all;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;
use crc32fast::Hasher;

const HEADER_SIZE: usize = 20;
const TOMBSTONE_FLAG: u32 = 0x800000000;


#[derive(Debug, Clone, Serilize, Deserialize)];
pub struct EntryHeader {
    pub data_length: u64,
    pub crc: u32,
    pub timestamp: u64,
    pub flags: u32,

}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub key: String,
    pub value: Option<String>,
    pub timestamp: u64,

}

#[derive(Debug)]
pub struct FileManager {
    data_dir: PathBuf,
    active_file: Option<File>,
    active_file_id: u32,
    file_offsets: HashMap<u32, u64>,

}

impl FileManager {
    pub fn new(data_dir: PathBuf) -> Result<Self, PlexError> {
        create_dir_all(&data_dir)?;

        let mut manager = Self {
            data_dir,
            active_file: None,
            active_file_id: 0,
            file_offsets: HashMap::new(),
        };

        manager.initialize_active_file()?;
        Ok(manager)
    }

    fn initialize_active_file(&mut self) -> Result<(), PlexError> {
        let mut max_file_id = 0;

        if let Ok(entries) = read_dir(&self.data_dir) {
            for entry in entries.flatten() {
                if let Some(file_name) = entry.file_name().to_str() {
                    if file_name.starts_with("data") && file_name.ends_with(".log") {
                        if let Ok(id) = file_name[5..file_name.len()-4].parse::<u32>() {
                            max_file_id = max_file_id.max(id);
                        }
                    }
                }
            }
        }

        self.active_file_id = max_file_id;
        let file_path = self.data_dir.join(format!("data_{:06}.log", self.active_file_id));

        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .append(true)
            .open(file_path)?;


        let current_size = file.metadata()?.len();
        self.file_offsets.insert(self.active_file_id, current_size);

        self.active_file = Some(file);
        Ok(())
    }



