use crate::cli::Command;
use crate::error::KvError;
use crate::storage_engine::StorageEngine;
use serde::Serialize;
use std::collections::HashMap;
use std::fs::{File, OpenOptions, rename};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

#[derive(Serialize, Debug)]
pub struct FileEngine {
    index: HashMap<String, u64>,

    #[serde(skip_serializing, skip_deserializing)]
    data_file: File,

    path: PathBuf,
}

impl StorageEngine for FileEngine {
    fn get(&self, key: &str) -> Result<Option<String>, KvError> {
        if key.is_empty() {
            return Err(KvError::KeyIsEmpty);
        }

        let Some(&offset) = self.index.get(key) else {
            return Ok(None);
        };

        let mut reader = BufReader::new(&self.data_file);

        reader.seek(SeekFrom::Start(offset))?;

        let mut length_bytes = [0u8; 8];

        let _ = reader.read_exact(&mut length_bytes);

        let length = u64::from_le_bytes(length_bytes) as usize;

        let mut command_bytes = vec![0u8; length];
        let _ = reader.read_exact(&mut command_bytes);

        let command: Command = bincode::deserialize(&command_bytes)?;

        match command {
            Command::Set { key: k, value: v} if k == key => Ok(Some(v)),
            Command::Set { key: _, value: _} | Command::Delete { key: _} => {
                eprintln!(
                    "Index points to mismatched or deleted command at offset {} for key '{}'",
                    offset, key
                );
                Ok(None)
            }

            Command::Get { key: _ } | Command::Compact => todo!(),

        }
    }


    fn set(&mut self, key: &str, value: &str) -> Result<(), KvError> {
        if key.is_empty() || value.is_empty() {
            return Err(KvError::KeyIsEmpty);
        }

        let command = Command::Set { key: key.to_string(), value: value.to_string()};
        let serialized = bincode::serialize(&command)?;

        let length = serialized.len() as u64;
        let length_bytes = length.to_le_bytes();

        let offset = self.data_file.seek(SeekFrom::End(0))?;

        self.data_file.write_all(&length_bytes)?;
        self.data_file.write_all(&serialized)?;
        self.data_file.flush()?;

        self.index.insert(key.to_string(), offset);

        Ok(())
    }

    fn delete(&mut self, key: &str) -> Result<(), KvError> {
        if key.is_empty() {
            return Err(KvError::KeyIsEmpty);
        }

        if self.index.contains_key(key) {
            let command = Command::Delete {key: key.to_string()};

            let serialized = bincode::serialize(&command)?;
            let length = serialized.len() as u64;
            let length_bytes = length.to_le_bytes();

            self.data_file.seek(SeekFrom::End(0))?;
            self.data_file.write_all(&length_bytes)?;
            self.data_file.write_all(&serialized)?;

            self.data_file.flush()?;
            self.index.remove(key);

            return Ok(());
        }
        Err(KvError::KeyNotFound)
    }
}

impl FileEngine {
    pub fn new(path: PathBuf) -> Result<Self, KvError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;

        let mut engine = FileEngine {
            index: HashMap::new(),
            data_file: file,
            path,
        };
        engine.load()?;

        Ok(engine)
    }

    pub fn load(&mut self) -> Result<(), KvError> {
        let mut offset = 0u64;
        let mut reader = BufReader::new(&self.data_file);
        reader.seek(SeekFrom::Start(0))?;

        loop {
            let mut length_bytes = [0u8; 8];

            match reader.read_exact(&mut length_bytes) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(KvError::IO(e)),
            }

            let length = u64::from_le_bytes(length_bytes) as usize;
            let mut command_bytes = vec![0u8; length];

            reader.read_exact(&mut command_bytes).map_err(KvError::IO)?;

            let command: Command =
                bincode::deserialize(&command_bytes).map_err(|_| KvError::CorruptData(offset))?;

            match command {
                Command::Set { key: k, value: _} => {
                    self.index.insert(k, offset);
                }

                Command::Delete { key: k} => {
                    self.index.remove(&k);
                }

                Command::Get { key: _} | Command::Compact => todo!(),
            }

            offset += 8 + length as u64;
        }

        Ok(())
    }

    pub fn compact(&mut self) -> Result<(), KvError> {

        let compact_path = self.path.with_extension("compacting");
        let mut compact_file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .truncate(true)
            .open(&compact_path)?;

        let mut new_index = HashMap::new();


        for (key, &offset) in &self.index {
            let mut reader = BufReader::new(&self.data_file);
            reader.seek(SeekFrom::Start(offset))?;

            let mut length_bytes = [0u8; 8];
            let _ = reader.read_exact(&mut length_bytes)?;
            let length = u64::from_le_bytes(length_bytes) as usize;

            let mut command_bytes = vec![0u8; length];
            let _ = reader.read_exact(&mut command_bytes)?;


            let _: Command = bincode::deserialize(&command_bytes)?;


            let new_offset = compact_file.seek(SeekFrom::End(0))?;
            let mut writer = BufWriter::new(&compact_file);
            /*let new_offset = compact_file.seek(SeekFrom::End(0))?;*/
            writer.write_all(&length_bytes)?;
            writer.write_all(&command_bytes)?;
            writer.flush()?;

            new_index.insert(key.clone(), new_offset);
        }

        compact_file.flush()?;
        drop(compact_file);

        rename(&compact_path, &self.path)?;

        self.data_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.path)?;

        self.index = new_index;

        Ok(())
    }

}
