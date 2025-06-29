use bincode;
use std::{fmt, io};

#[derive(Debug)]
pub enum KvError {
    /// The key was not found in the store
    KeyNotFound,

    /// The key provided was empty.
    KeyIsEmpty,

    /// An I/O error occurred
    IO(io::Error),

    /// A deseerialization error occurred while loading a command.
    Deserialize(bincode::Error),

    /// Corrupt ot invalid data was found at a specific file offset.
    CorruptData(u64),
}

impl fmt::Display for KvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KvError::KeyIsEmpty => write!(f, "The key is empty"),
            KvError::KeyNotFound(key) => write!(f, "Key {} not found",  key),
            KvError::IO(err) => write!(f, "I/O error: {}", err),
            KvError::Deserialize(err) => write!(f, "Deserialization error: {}", err),
            KvError::CorruptData(offset) => {
                write!(f, "Corrupt data detected at file offset {}", offset)
            }
        }
    }
}

impl std::error::Error for KvError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            KvError::IO(err) => Some(err),
            KvError::Deserialize(err) => Some(err),
            _ => None,
        }
    }
}

impl From<bincode::Error> for KvError {
    fn from(err: bincode::Error) -> KvError {
        KvError::Deserialize(err)
    }
}

impl From<std::io::Error> for KvError {
    fn from(err: std::io::Error) -> KvError {
        KvError::IO(err)
    }
}
