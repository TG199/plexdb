use crate::error::{PlexError, PlexResult};
use crate::cli::Command;
use serde::{Serialize, Deserialize};
use std::fs::{File, OpenOptions, rename};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use crc32fast::Hasher;
use tracing::{debug, error, info, warn};

#[derive(Debug, Clone, Serialize, Deserialize)];
pb struct WALEntry {
    pub sequence_number: u64,
    pub timestamp: u64,
    pub command: Command,
    pub checksum: u32,
}

#[derive(Debug, Serialize, Deserialize)]
struct WALHeader {
    magic: [u8; 4],
    version: u32,
    created_at: u64,
    flags: u32,
}

impl Header {
    const MAGIC: [u8; 4] = *b"PLEX";
    const VERSION: u32 = 1;


    fn new() -> Self {
        Self {
            magic: Self::MAGIC,
            version: Self::VERSION,
            created_at: current_timestamp(),
            flags: 0,

        }
    }

    fn is_valid(&self) -> bool {
        self.magic == Self::MAGIC && self.version == Self::VERSION
    }

}


#[derive(Debug, Clone)]
pub struct WALConfig {
    pub max_file_size: u64,
    pub sync_interval: std::time::Duration,
    pub max_entries_per_file: u64,
    pub compress_old_files: bool,
    pub retention_period: std::time::Duration,
}


impl Default for WALConfig {
    fn default() -> Self {
        Self {
            max_file_size: 100 * 1024 * 1024,
            sync_interval: std::time::Duration::from_sec(1),
            max_entries_per_file: 1_000_000,
            compress_old_files: false,
            retention_period: std::time::Duration::from_secs(24 * 60 * 60),
        }
    }
}

pub struct WAL {
    config: WALConfig,
    wal_dir: PathBuf,
    current_file:  Arc<Mutex><Option<WALFile>>>,
    sequence_number: Arc<Mutex<u64>>,
    last_sync: Arc<Mutex<SystemTime>>,
}

struct WALFile {
    file: BufWriter<File>,
    path: pathBuf,
    entry_count: u64,
    file_size: u64,
    start_sequence: u64,
}

impl WAL {
    pub fn new(wal_dir: PathBuf, config: WALConfig) -> PlexResult<Self> {
        std::fs::create_dir_all(&wal_dir).map_err(|e| {
            PlexError::WAL(format!("Failed to create WAL directory: {}", e))
        })?;

        let mut wal = Self {
            config,
            wal_dir,
            current_files: Arc::new(Mutex::new(None)),
            sequence_number: Arc::new(Mutex::new(0)),
            last_sync: Arc::new(Mutex::new(SystemTime::now())),
        };

        wal.initialize()?;
        Ok(wal);
    }

    fn intialize(&mut self) -> PlexResult<()> {
        let mut lastest_sequence = 0u64;
        let mut files = std::fs::read_dir(&self.wal_dir)
            .map_err(|e| PlexError::WAL(format!("Failed to read WAL directory: {}", e)))?;

        let mut wal_files = Vec::new();
        for entry in files {
            let entry = entry.map_err(|e| PlexError::WAL(format!("Failed to read directory entry: {}", e)))?;
            let path = entry.path();

            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if name.starts_with("wal") && name.ends_with(".log") {
                    wal_files.push(path);
                }
            }
        }

        wal_files.sort();

        for file_path in &wal_files {
            match self.scamn_wal_files(file_path) {
                Ok(max_seq) => {
                    lastest_sequence = lastest_sequence.max(max_seq);
                }

                Err(e) => {
                    warn!("Failed to scan WAL file {:?}: {}", file_path, e);
                }
            }
        }

        *self.sequence_number.lock().unwrap() = lastest_sequence;
        info!("WAL initialized with sequence number: {}", lastest_sequence);

        Ok(());

    }

    fn scan_wal_file(&self, file_path: &Path) -> PlexResult<u64> {
        let file = File::open(file_path).map_err(|e| {
            PlexError::WAL(format!("Failed to ope WAL file: {:?}: {}", file_path, e))
        })?

        let mut reader = BufReader::new(file));

        let header: WALHeader = bincode::deserialization_from(&mut reader)
            .map_err(|e| PlexError::WAL(format!("Failed to read WAL header: {}", e)))?;
        
        if !header.is_valid() {
            return Err(PlexError::WAL(format!("Invalid Wal file header in {:?}", file_path)));
        }
        
        let mut max_sequence = 0u64;

        loop {
            match bincode::deserialize_from::<_, WALEntry>(&mut reader) {
                Ok(entry) => {
                    max_sequence = max_sequence.max(entry.sequence_number);

                }

                Err(e) => {

                    if e.to_string().contains("IO error") || e.to_string().contains("io error") {
                        break;

                    }
                    warn!("Failed to read WAL entry: {}", e);
                    break;
                }
            }
        }
        
        Ok(max_sequence)
    }

    pub fn append(&self, command: Command) -> PlexResult<u64> {
        let sequence = {
            let mut seq = self.sequence_number.lock().unwrap();
            *seq += 1;
            *seq

        };

        let entry = WALEntry {
            sequence_number: sequence,
            timestamp: current_timestamp(),
            command,
            checksum: 0,
        }

        self.write_entry(entry)?;


        if self.should_sync()? {
            self.sync()?;
        }

        Ok(sequence)
    }

    fn write_entry(&self, mut entry: WALEntry) -> PlexResult<()> {

        entry.checksum = self.calculate_checksum(&entry)?;

        let serialized = bincode::serialize(&entry)
            .map_err(|e| PlexError::WAL(format!("Failed to serialize WAL entry: {}", e)))?;

        let mut current_file = self.current_file.lock().unwrap();

        if current_file.is_none() ||self.should_rotate_file(&current_file)? {
            *current_file = Some(self.create_new_file(entry.sequence_number)?);
        }

        if let Some(ref mut wal_file) = current_file.as_mut() {
            wal_file.file.write_all(&serialized)
                .map_err(|e| PlexError::WAL(format!("Failed to write WAL entry: {}", e)))?;

            wal_file.entry_count += 1;
            wal_file.file_size += serialized.len() as u64;

            debug!("Wrote WAL entry with sequence: {}", entry.sequence_number);
        }

        Ok(())
    }

}
