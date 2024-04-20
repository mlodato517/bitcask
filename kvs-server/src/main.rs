use std::borrow::Borrow;
use std::io::Write;
use std::net::TcpListener;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use clap::Parser;
use kvs::{KvStore, KvsEngine};
use protocol::{Cmd, Reader, Response};
use tracing::{debug, info, warn};

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
    let mut kvs = open_engine(args.engine.as_deref(), &dir).context("Failed to open engine")?;
    let engine = match &kvs {
        Engine::Kvs(_) => "kvs",
        Engine::Sled(_) => "sled",
    };
    info!(?args.addr, ?dir, ?engine, version=env!("CARGO_PKG_VERSION"), "Starting server");

    let mut reader = Reader::new();

    for stream in listener.incoming() {
        let mut stream = stream?;
        info!(?stream, "Received connection");

        let cmd = match reader.read_cmd(&mut stream) {
            Ok(Some(cmd)) => cmd,
            Ok(None) => {
                warn!("Response had no data");
                stream.write_all(b"Response had no data")?;
                stream.flush()?;
                break;
            }
            Err(e) => {
                warn!(?e, "Failed to parse command");
                let e = e.to_string();
                stream.write_all(e.as_bytes())?;
                stream.flush()?;
                break;
            }
        };

        info!(?cmd, "Parsed command");
        let response = match cmd.into_cmd() {
            Cmd::Set(k, v) => handle_set(&mut kvs, k.to_string(), &v),
            Cmd::Get(k) => handle_get(&mut kvs, &k),
            Cmd::Rm(k) => handle_rm(&mut kvs, &k),
        };
        response.write(&mut stream)?;
        stream.flush()?;
    }

    Ok(())
}

/// Executes a set command on the passed KvsEngine, returning a response.
fn handle_set(kvs: &mut impl KvsEngine, key: String, value: &str) -> Response<'static> {
    match kvs.set(key, value) {
        Ok(_) => Response::SuccessfulSet,
        Err(e) => {
            warn!(?e, "Failed to set key to value");
            // TODO These .to_string()s are kind of sad. We should be able to write these
            // bytes directly into the stream. Maybe these should be static methods like
            // `response::write_err(impl Display)` or something?
            Response::Err(e.to_string().into())
        }
    }
}

/// Executes a get command on the passed KvsEngine, returning a response.
fn handle_get(kvs: &mut impl KvsEngine, key: &str) -> Response<'static> {
    match kvs.get(key) {
        Ok(Some(val)) => Response::SuccessfulGet(val.into()),
        Ok(None) => Response::KeyNotFound,
        Err(e) => {
            warn!(?e, "Failed to get key");
            Response::Err(e.to_string().into())
        }
    }
}

/// Executes a remove command on the passed KvsEngine, returning a response.
fn handle_rm(kvs: &mut impl KvsEngine, key: &str) -> Response<'static> {
    match kvs.remove(key) {
        Ok(_) => Response::SuccessfulRm,
        Err(e) => {
            warn!(?e, "Failed to remove key");
            Response::Err(e.to_string().into())
        }
    }
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
