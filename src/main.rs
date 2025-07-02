use plexdb::PlexError;
use plexdb::StorageEngine;
use plexdb::engine::plex_engine::PlexEngine;
use plexdb::cli::{CliArgs, Command};
use clap::Parser;
use anyhow::bail;

fn main() -> anyhow::Result<()> {
    env_logger::init();

    let args = CliArgs::parse();
    let path = args.data_dir.join("./data.log");
    let mut store = PlexEngine::new(path)?;

    match args.command {
        Command::Set { key, value} => {
            store.set(&key, &value)?;
            println!("Set '{}' = '{}'", key, value);
        }

        Command::Get { key } => {
            match store.get(&key)? {
                Some(val) => println!("{}", val),
                None => bail!(PlexError::KeyNotFound),
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
