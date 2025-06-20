
use crate storage_enigne::StorageEngine;

struct FileEngine {
    index: HashMap<String, u64>,
    data_file: File,
    path: PathBuf,
}

impl StorageEngine for FileEngine {

    fn get(&self, key: &str) -> Result<Option<String>, KvError> {
        if key ! = 
    }

    fn set(&mut self, key: &str, value: &str) -> Result <(), KvError> {

    }

    fn delete(&mut, self, key: &str) -> Result<(), KvError> {

    }

}


pub fn load() {}

pub fn reload() {}
