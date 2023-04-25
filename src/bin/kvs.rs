use clap::{Parser, Subcommand};

use kvs::{KvStore, Result};

#[derive(Parser)]
#[command(version)]
struct Args {
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    Set { key: String, value: String },
    Get { key: String },
    Rm { key: String },
}

fn main() -> Result<()> {
    let args = Args::parse();

    // TODO Accept CLI Argument
    let path = std::env::current_dir()?;
    let mut store = KvStore::open(path)?;
    match args.command {
        Command::Set { key, value } => {
            store.set(key, value)?;
        }
        Command::Get { key } => match store.get(key)? {
            Some(value) => println!("{value}"),
            None => println!("Key not found"),
        },
        Command::Rm { key } => {
            if let Err(e) = store.remove(key) {
                println!("{e}");
                std::process::exit(1);
            }
        }
    }
    Ok(())
}
