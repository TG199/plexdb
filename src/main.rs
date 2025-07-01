use kaydb::KvError;
use kaydb::StorageEngine;
use kaydb::engine::file_storage::FileEngine;
use kaydb::cli::{CliArgs, Command};
use clap::Parser;
use anyhow::bail;

fn main() -> anyhow::Result<()> {
    env_logger::init();

    let args = CliArgs::parse();
    let path = args.data_dir.join("./data.log");
    let mut store = FileEngine::new(path)?;

    match args.command {
        Command::Set { key, value} => {
            store.set(&key, &value)?;
            println!("Set '{}' = '{}'", key, value);
        }

        Command::Get { key } => {
            match store.get(&key)? {
                Some(val) => println!("{}", val),
                None => bail!(KvError::KeyNotFound),
            }
        }

        Command::Delete { key } => {
            store.delete(&key)?;
            println!("Deleted '{}'", key);
        
        }

        Command::Compact => {
            store.compact()?;
            println!("Compaction complete.");
        }
    }

    Ok(())
}
