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

    /// A deseerialization error occurred while loading a command.
    Deserialize(bincode::Error),

    /// Corrupt ot invalid data was found at a specific file offset.
    CorruptData(u64),
}

impl fmt::Display for PlexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PlexError::KeyIsEmpty => write!(f, "The key is empty"),
            PlexError::KeyNotFound => write!(f, "Key not found"),
            PlexError::IO(err) => write!(f, "I/O error: {}", err),
            PlexError::Deserialize(err) => write!(f, "Deserialization error: {}", err),
            PlexError::CorruptData(offset) => {
                write!(f, "Corrupt data detected at file offset {}", offset)
            }
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
        PlexError::Deserialize(err)
    }
}

impl From<std::io::Error> for PlexError {
    fn from(err: std::io::Error) -> PlexError {
        PlexError::IO(err)
    }
}
