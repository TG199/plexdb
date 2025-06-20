use std::io;
use thiserror::Error;


#[derive(Error, Debug)]
pub enum KvError {
    #[error("Key not found")]
    KeyNotFound,

    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    #[error("Serialization error: {0}")]
    Serde(#[from] bincode::Error),


    #[error("Unexpected error: {0}")]
    Unexpected(String),
}



