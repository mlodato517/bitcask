use std::{net::SocketAddr, path::PathBuf};

use kvs_server::{Engine, EngineType, Server};

use anyhow::{Context, Result};
use clap::Parser;
use tracing::{debug, info, trace};

#[derive(Parser)]
#[command(version)]
struct Args {
    /// Address to bind to and receive connections from.
    #[clap(long, default_value = "127.0.0.1:4000")]
    addr: SocketAddr,

    /// Type of underlying key-value storage to use. Either "kvs" or "sled".
    #[clap(long)]
    engine: Option<EngineType>,

    /// Directory for engine to store data files in.
    #[clap(long)]
    dir: Option<PathBuf>,
}

fn main() -> Result<()> {
    logging::configure();

    let mut args = Args::parse();

    trace!(?args.dir, "Determining directory");
    let dir = match args.dir.take() {
        Some(dir) => dir,
        None => std::env::current_dir().context("Failed to find current directory")?,
    };
    debug!(?dir, "Using directory");

    debug!(?args.engine, "Opening engine");
    let kvs = Engine::new_in(args.engine, &dir)?;
    info!(
        ?args.addr,
        ?dir,
        engine_type = %kvs.engine_type(),
        version=env!("CARGO_PKG_VERSION"),
        "Starting server",
    );

    Server::new(kvs, args.addr).run()
}
