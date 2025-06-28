use kivistore::KvError;
use kivistore::engine::file_storage::FileEngine;

fn main() -> anyhow::Result<()> {
    env_logger::init();

    let args = CliArgs::parse();
    let mut store = KayStore::open(&args.data_dir)?;

    match args.command {
        cli::Command::Set { key, value } => {
            store.set(key, value)?;
        }

        cli::Command::Get { key } => match store.get(key)? {
            Some(val) => println!("{}", val),
            None => println!("Key not found"),
        },

        cli::Command::Delete { key } => {
            store.delete(key)?;
        }
    }
    ok(())
}
