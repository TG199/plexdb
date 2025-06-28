use std::result::Result;

use crate::error::KvError;

pub trait StorageEngine {
    /* Trait that defines the storage engine */

    fn get(&self, key: &str) -> Result<Option<String>, KvError>;

    fn set(&mut self, key: &str, value: &str) -> Result<(), KvError>;

    fn delete(&mut self, key: &str) -> Result<(), KvError>;
}
