use std::borrow::Borrow;
use std::fmt;
use std::io::Write;
use std::path::Path;

use anyhow::{bail, Result};
use kvs::{KvStore, KvsEngine};
use protocol::Cmd;
use tracing::warn;

use crate::sled_engine::SledDb;

mod sled_engine;

pub struct KvsServer {
    engine: Engine,
}
impl KvsServer {
    pub fn open(engine: Option<&str>, p: impl AsRef<Path>) -> Result<Self> {
        enum PreviousEngine {
            Kvs,
            None,
            Sled,
        }
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
        let engine = match (prev_engine, engine) {
            (PreviousEngine::Sled, Some("kvs")) => bail!("Can't open kvs engine in sled directory"),
            (_, Some("kvs")) => Engine::Kvs(KvStore::open(p.as_ref())?),

            (PreviousEngine::Kvs, Some("sled")) => bail!("Can't open sled engine in kvs directory"),
            (_, Some("sled")) => Engine::Sled(SledDb(sled::open(p)?)),

            (_, Some(_)) => bail!("Invalid engine type!"),

            (PreviousEngine::Sled, None) => Engine::Sled(SledDb(sled::open(p.as_ref())?)),
            (PreviousEngine::Kvs | PreviousEngine::None, None) => {
                Engine::Kvs(KvStore::open(p.as_ref())?)
            }
        };
        Ok(Self { engine })
    }

    pub fn handle_cmd(&mut self, cmd: Cmd, mut out: impl Write) -> Result<()> {
        match cmd {
            Cmd::Set(k, v) => match self.engine.set(k.to_string(), &v) {
                Ok(_) => {
                    out.write_all(b"s")?;
                }
                Err(e) => {
                    warn!(?e, "Failed to set key to value");
                    write!(out, "e{e}")?;
                }
            },
            Cmd::Get(k) => match self.engine.get(k) {
                Ok(Some(val)) => write!(out, "s{val}")?,
                Ok(None) => out.write_all(b"n")?,
                Err(e) => write!(out, "e{e}")?,
            },
            Cmd::Rm(k) => match self.engine.remove(k) {
                Ok(()) => {
                    out.write_all(b"s")?;
                }
                Err(e) => {
                    warn!(?e, "Failed to remove key");
                    write!(out, "e{e}")?;
                }
            },
        }
        Ok(())
    }
}

impl fmt::Display for KvsServer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.engine {
            Engine::Kvs(_) => f.write_str("kvs"),
            Engine::Sled(_) => f.write_str("sled"),
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