use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use kvs_client::Client;
use tracing::info;

#[derive(Parser)]
#[command(version)]
struct Args {
    #[command(subcommand)]
    command: Command,

    #[clap(long, default_value = "127.0.0.1:4000", global = true)]
    addr: String,
}

#[derive(Subcommand)]
enum Command {
    Set { key: String, value: String },
    Get { key: String },
    Rm { key: String },
}

fn main() -> Result<()> {
    logging::configure();

    let args = Args::parse();

    info!(?args.addr, "Connecting to server");
    let address = args
        .addr
        .parse()
        .context(format!("Invalid address {:?}", args.addr))?;
    let mut client = Client::new(address);

    match &args.command {
        Command::Set { key, value } => client.set(key, value)?,
        Command::Rm { key } => client.rm(key)?,
        Command::Get { key } => match client.get(key)? {
            Some(s) => println!("{s}"),
            None => println!("Key not found"),
        },
    }

    Ok(())
}
