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

