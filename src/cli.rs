use clap::{Parser, Subcommand};
use std::path::pathBuf;

#[derive(Parser)]
#[command(name = "kaydb")]
#[command(about = "A Rust based key-value store", long_about = None)]

pub struct CliArgs {
    #[arg(short, long, default_value = "./data")]
    pub data_dir: PathBuf,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    Set { 
        key: String,
        value: String,
    },

    Get {
        key: String
    },

    Delete {
        key: String
    },
}
