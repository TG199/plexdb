use crate::error::PlexError;
use crate::engine::partition_manager::FileOffset;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::fs::{create_dir_all, read_dir};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;
use crc32fast::Hasher;
use crate::utils::time;

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

    pub fn write_entry(&mut self, key: &str, value: &str) -> Result<FileOffset, PlexError> {
        let entry = LogEntry {
            key: key.to_string(),
            value: Some(value.to_string()),
            timestamp: time::current_timestamp(), 
        };

        self.write_log_entry(&entry, false)
    }

    pub fn write_tombstone(&mut self, key: &str) -> Result<FileOffset, PlexError> {
        let entry = LogEntry {
            key: key.to_string(),
            value: None,
            timestamp: time::current_timestamp(),
        };

        self.write_log_entry(&entry, true)
    }

    fn write_log_entry(&mut self, entry: &LogEntry, is_tombstone: bool) -> Result<FileOffset, PlexError> {
        let serialized = bincode::serialize(entry)?;

        let mut hasher = Hasher::new();
        hasher.update(&serialized);
        let crc = hasher.finalize();


        let flags = if is_tombstone { TOMBSTONE_FLAG } else { 0 };

        let header = EntryHeader {
            data_length: serialized.len() as u64,
            crc,
            timestamp: entry.timestamp,
            flags,
        };

        let file = self.active_file.as_mut().ok_or(PlexError::IO(
                Error::new(ErrorKind::NotFound, "No active file")
        ))?;

        let current_offset = *self.file_offsets.get(&self.active_file_id).unwrap_or(&0);

        file.write_all(&header.data_length.to_le_bytes())?;
        file.write_all(&header.crc.to_le_bytes())?;
        file.write_all(&header.timestamp.to_le_bytes())?;
        file.write_all(&header.flags.to_le_bytes())?;


        file.write_all(serialized)?;
        file.sync_all()?;


        let new_offset = current_offset + HEADER_SIZE as u64 + serialized.len() as u64;
        self.file_offsets.insert(self.active_file_id, new_offset);

        Ok(FileOffset {
            partition_id: 0,
            file_id: self.active_file_id,
            offset: current_offset,
            size: (HEADER_SIZE + serialized.len()) as u32,
            timestamp: entry.timestamp,
        })
    }

    pub fn read_value(&self, offset: &FileOffest) -> Result<Option<String>, PlexError> {
        let file_path = self.data_dir.join(format!("data_{:06}.log", offset.file_id));
        let file = File::open(file_path)?;
        let mut reader = BufReader::new(file);

        reader.seek(SeekFrom::Start(offset.offset))?;

        let mut header_bytes = [0u8, HEADER_SIZE];
        reader.read_exact(&mut header_bytes)?;


        let data_length = u64::from_le_bytes(header_bytes[0..8].try_into().unwrap()) as usize;
        let stored_crc = u32::from_le_bytes(header_bytes[8..12].try_into().unwrap());
        let _timestamp = u64::from_le_bytes(header_bytes[12..16].try_into().unwrap());
        let flags = u32::from_le_bytes(header_bytes[16..20].try_into().unwrap());

        let mut data = vec![0u8; data_length];
        reader.read_exact(&mut data);

        let mut hasher = Hasher::new();
        hasher.update(&data);
        let calculated_crc = hasher.finalize();

        if calculated_crc != stored_crc {
            return Err(PlexError::CorruptData(offset.offset));
        }

        if flags & TOMBSTONE_FLAG != 0 {
            return Ok(None);
        }

        let entry: LogEntry = bincode::deserialize(&data)?;
        Ok(entry.value)
    }




