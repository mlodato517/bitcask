use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use protocol::Cmd;
use tracing::{debug, info, warn};

use kvs_server::KvsServer;

#[derive(Parser)]
#[command(version)]
struct Args {
    #[clap(long, default_value = "127.0.0.1:4000")]
    addr: String,

    #[clap(long)]
    engine: Option<String>,

    #[clap(long)]
    dir: Option<PathBuf>,
}

fn main() -> Result<()> {
    logging::configure();

    let mut args = Args::parse();

    debug!(?args.addr, "Binding server");
    let listener = TcpListener::bind(&args.addr).context("Failed to bind address")?;
    debug!("Server bound");

    let dir = match args.dir.take() {
        Some(dir) => dir,
        None => std::env::current_dir().context("Failed to find current directory")?,
    };
    debug!(?args.engine, ?dir, "Opening engine");
    let mut kvs = KvsServer::open(args.engine.as_deref(), &dir).context("Failed to open engine")?;
    info!(?args.addr, ?dir, engine=%kvs, version=env!("CARGO_PKG_VERSION"), "Starting server");

    for stream in listener.incoming() {
        let mut stream = stream?;
        info!(?stream, "Received connection");

        // TODO BufReader? Read timeout? Read incrementally? Use len to read more if needed?
        let mut buf = vec![];
        stream.read_to_end(&mut buf)?;
        debug!(?buf, "Current buffer");

        let cmd = match Cmd::parse(&buf) {
            Ok(cmd) => cmd,
            Err(e) => {
                warn!(?e, "Failed to parse command");
                let e = e.to_string();
                stream.write_all(e.as_bytes())?;
                stream.flush()?;
                break;
            }
        };

        info!(?cmd, "Parsed command");
        kvs.handle_cmd(cmd, &mut stream)?;
        stream.flush()?;
    }

    Ok(())
}
