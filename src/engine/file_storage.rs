use std::collections::HashMap;
use crate error::KvError{KeyNotFound, IO}; 
use crate storage_enigne::StorageEngine;
use crate cli::Command;

struct FileEngine {
    index: HashMap<String, u64>,
    data_file: File,
    path: PathBuf,
}

impl StorageEngine for FileEngine {

    fn get(&self, key: &str) -> Result<Option<String>, KvError> {
        if let Some(&offset) = self.index.get(key) {
            self.data_file.seek(SeekFrom::Start(offset))?;

            let mut reader = BufReader::new(&self.data_file);
            let line = String::new();
            reader.read_line(&mut line)?;

            let parts: Vec<&str> = line.trim_end().splitn(3, ' ').collect();

            if parts.len() == 3 && parts[0] == "set" && parts[1] == key {
                return Ok(Some(parts[2].to_string());
            }
        }
        return KeyNotFound
    }

    fn set(&mut self, key: &str, value: &str) -> Result <(), KvError> {
        let offset = self.data_file.seek(SeekFrom::End(0))?;

        let line = format!("set {} {}\n", key, value);
        self.data_file.write_all(line.as_bytes())?;
        self.data_file.flush();

        self.index.insert(key, offset);
        Ok(());
    }

    fn delete(&mut, self, key: &str) -> Result<(), KvError> {

        if self.index.contains_key(key) {
            self.index.remove(key);
        }
        return KeyNotFound
    }

}


pub fn load() {}

pub fn reload() {}
