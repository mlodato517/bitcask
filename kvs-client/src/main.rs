use std::borrow::Cow;
use std::io::Read;
use std::net::{Shutdown, TcpStream};

use anyhow::Result;
use clap::{Parser, Subcommand};
use protocol::{Cmd, Response};
use tracing::{debug, info};

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
    let mut connection = TcpStream::connect(args.addr)?;
    // TODO set_read_timeout?
    debug!("Connected to server");

    let cmd = match &args.command {
        Command::Set { key, value } => Cmd::Set(Cow::Borrowed(key), Cow::Borrowed(value)),
        Command::Get { key } => Cmd::Get(Cow::Borrowed(key)),
        Command::Rm { key } => Cmd::Rm(Cow::Borrowed(key)),
    };
    info!(?cmd, "Writing to server");
    cmd.write(&mut connection)?;
    // NB We don't need to `.flush()?` because, for `TcpStream`, that's a no-op
    debug!("Wrote to server. Shutting down write half.");
    connection.shutdown(Shutdown::Write)?;
    debug!("Shut down write half");

    let mut result = Vec::with_capacity(1024);
    debug!("Reading from server");
    if let Err(e) = connection.read_to_end(&mut result) {
        eprintln!("Server error: {e}");
        std::process::exit(1);
    }

    match Response::from_bytes(&result) {
        Response::SuccessfulSet | Response::SuccessfulRm => {}
        Response::SuccessfulGet(s) => println!("{s}"),
        Response::KeyNotFound => println!("Key not found"),
        Response::Err(e) => {
            eprintln!("User error: {e}");
            std::process::exit(1);
        }
    }

    Ok(())
}
