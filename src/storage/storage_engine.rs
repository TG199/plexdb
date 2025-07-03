use std::result::Result;

use crate::error::PlexError;

pub trait StorageEngine {
    /* Trait that defines the storage engine */

    fn get(&self, key: &str) -> Result<Option<String>, PlexError>;

    fn set(&mut self, key: &str, value: &str) -> Result<(), PlexError>;

    fn delete(&mut self, key: &str) -> Result<(), PlexError>;
}
