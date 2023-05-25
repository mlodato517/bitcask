use std::borrow::Borrow;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use clap::Parser;
use tracing::metadata::LevelFilter;
use tracing::{debug, info, warn};
use tracing_subscriber::prelude::*;
use tracing_subscriber::{fmt, EnvFilter};

use kvs::{Cmd, KvStore, KvsEngine};
use sled_engine::SledDb;

mod sled_engine;

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
    tracing_subscriber::registry()
        .with(fmt::layer().with_writer(std::io::stderr).with_ansi(false))
        .with(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .init();

    let mut args = Args::parse();

    debug!(?args.addr, "Binding server");
    let listener = TcpListener::bind(&args.addr).context("Failed to bind address")?;
    debug!("Server bound");

    let dir = match args.dir.take() {
        Some(dir) => dir,
        None => std::env::current_dir().context("Failed to find current directory")?,
    };
    debug!(?args.engine, ?dir, "Opening engine");
    let mut kvs = open_engine(args.engine.as_deref(), &dir).context("Failed to open engine")?;
    let engine = match &kvs {
        Engine::Kvs(_) => "kvs",
        Engine::Sled(_) => "sled",
    };
    info!(?args.addr, ?dir, ?engine, version=env!("CARGO_PKG_VERSION"), "Starting server");

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
        match cmd {
            Cmd::Set(k, v) => match kvs.set(k.to_string(), &v) {
                Ok(_) => {
                    stream.write_all(b"s")?;
                }
                Err(e) => {
                    warn!(?e, "Failed to set key to value");
                    write!(stream, "e{e}")?;
                }
            },
            Cmd::Get(k) => match kvs.get(k) {
                Ok(Some(val)) => write!(stream, "s{val}")?,
                Ok(None) => stream.write_all(b"n")?,
                Err(e) => write!(stream, "e{e}")?,
            },
            Cmd::Rm(k) => match kvs.remove(k) {
                Ok(()) => {
                    stream.write_all(b"s")?;
                }
                Err(e) => {
                    warn!(?e, "Failed to remove key");
                    write!(stream, "e{e}")?;
                }
            },
        }
        stream.flush()?;
    }

    Ok(())
}

enum Engine {
    Kvs(KvStore),
    Sled(sled_engine::SledDb),
}
impl KvsEngine for Engine {
    fn set<V: AsRef<str>>(&mut self, key: String, value: V) -> kvs::Result<()> {
        match self {
            Self::Kvs(k) => k.set(key, value),
            Self::Sled(s) => s.set(key, value),
        }
    }
    fn get<K: Borrow<str>>(&mut self, key: K) -> kvs::Result<Option<String>> {
        match self {
            Self::Kvs(k) => k.get(key),
            Self::Sled(s) => s.get(key),
        }
    }
    fn remove<K: Borrow<str>>(&mut self, key: K) -> kvs::Result<()> {
        match self {
            Self::Kvs(k) => k.remove(key),
            Self::Sled(s) => s.remove(key),
        }
    }
}
enum PreviousEngine {
    Kvs,
    None,
    Sled,
}
fn open_engine(engine: Option<&str>, p: impl AsRef<Path>) -> Result<Engine> {
    let mut prev_engine = PreviousEngine::None;
    for entry in std::fs::read_dir(p.as_ref())? {
        let entry = entry?;
        let file_name = entry.file_name();
        if file_name == "conf" || file_name == "db" {
            prev_engine = PreviousEngine::Sled;
            break;
        } else if file_name.to_string_lossy().ends_with(".pingcap") {
            prev_engine = PreviousEngine::Kvs;
            break;
        }
    }
    match (prev_engine, engine) {
        (PreviousEngine::Sled, Some("kvs")) => bail!("Can't open kvs engine in sled directory"),
        (_, Some("kvs")) => Ok(Engine::Kvs(KvStore::open(p.as_ref())?)),

        (PreviousEngine::Kvs, Some("sled")) => bail!("Can't open sled engine in kvs directory"),
        (_, Some("sled")) => Ok(Engine::Sled(SledDb(sled::open(p)?))),

        (_, Some(_)) => bail!("Invalid engine type!"),

        (PreviousEngine::Sled, None) => Ok(Engine::Sled(SledDb(sled::open(p.as_ref())?))),
        (PreviousEngine::Kvs | PreviousEngine::None, None) => {
            Ok(Engine::Kvs(KvStore::open(p.as_ref())?))
        }
    }
}
