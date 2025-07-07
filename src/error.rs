use bincode;
use std::{fmt, io};

#[derive(Debug)]
pub enum PlexError {
    /// The key was not found in the store
    KeyNotFound,

    /// The key provided was empty.
    KeyIsEmpty,

    /// An I/O error occurred
    IO(io::Error),

    /// A deserialization error occurred while loading a command.
    Deserialize(bincode::Error),

    /// A serialization error occured while loading a command.
    Serialize(bincode::Error),

    /// Corrupt or invalid data was found at a specific file offset.
    CorruptData(u64),

    /// Lock aquisition failed
    LockError,

    /// A configuration error occurred
    Config,

    /// Compaction process failed
    CompactionFailed,

    /// A Write Ahead Log error occurred
    WAL,

    /// Recovery failed
    Recovery,

    /// Failed to partition correctly
    Partition {
        id: u32, message: String
    },

    /// A bloom filter error occurred
    BloomFilter,

    /// A mismatch in checking sum
    CheckSumMisMatch {
        expected: u32, actual: u32
    },

    /// An invalid file format error
    InvalidFormat,

    /// Operation taking too long
    TimeOut {
        operation: String,
        timeout_ms: u64,
    },

}


impl fmt::Display for PlexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PlexError::KeyIsEmpty => write!(f, "The key is empty"),
            PlexError::KeyNotFound => write!(f, "Key not found"),
            PlexError::IO(err) => write!(f, "I/O error: {}", err),
            PlexError::Deserialize(err) => write!(f, "Deserialization error: {}", err),
            PlexError::Serialize(err) => write!(f, "Serialization error: {}", err),
            PlexError::CorruptData(offset) => {
                write!(f, "Corrupt data detected at file offset {}", offset)
            }
            PlexError::LockError(resource) => write!(f, "Lock acquisition failed: {}" resource),
            PlexError::Config(err) => write!(f, "Configuration error: {}", err),
            PlexError::CompactionFailed(err) => write!(f, "Compaction failed: {}", err),
            PlexError::WAL(err) => write!(f, "WAL error: {}", err),
            PlexError::Recovery(err) => write!(f, "Recovery error: {}", err),
            PlexError::Partition(id, message) => {
                write!(f, "Partition error: {} {}" id, message)
            },
            PlexError::BloomFilter(err) => write!(f, "Bloom filter error: {}" err),
            PlexError::CheckSumMisMatch(expected, actual) => {
            write!(f, "Checksummismatch: expected {} actual {}", expected, actual)
            },
            PlexError::InvalidFormat => write!("Invalid file format"),
            PlexError::TimeOut(operation, timeout_ms) => write!(f, "Timeout: {} took too long {}", operation, timeout_ms),
        }
    }
}

impl std::error::Error for PlexError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            PlexError::IO(err) => Some(err),
            PlexError::Deserialize(err) => Some(err),
            _ => None,
        }
    }
}

impl From<bincode::Error> for PlexError {
    fn from(err: bincode::Error) -> PlexError {
        PlexError::Deserialize(err.to_string())
    }

    fn from(err: bincode::Error) -> PlexError {
        PlexError::Serialize(err.to_string())
    }
}

impl From<std::io::Error> for PlexError {
    fn from(err: std::io::Error) -> PlexError {
        PlexError::IO(err)
    }
}
