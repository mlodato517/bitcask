use std::borrow::Borrow;
use std::fmt;
use std::path::Path;

use anyhow::{bail, Result};
use kvs::{CompactionPolicy, KvStore, KvsEngine, MaxFilePolicy};
use protocol::Cmd;
use response::Response;
use tracing::warn;

use crate::sled_engine::SledDb;

pub mod response;
mod sled_engine;

pub struct KvsServer<C> {
    engine: Engine<C>,
}
impl KvsServer<MaxFilePolicy> {
    pub fn open(engine: Option<&str>, p: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_policy(engine, p, MaxFilePolicy::default())
    }
}
impl<C: CompactionPolicy> KvsServer<C> {
    pub fn open_with_policy(
        engine: Option<&str>,
        p: impl AsRef<Path>,
        compaction_policy: C,
    ) -> Result<Self> {
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
            (_, Some("kvs")) => {
                Engine::Kvs(KvStore::open_with_policy(p.as_ref(), compaction_policy)?)
            }

            (PreviousEngine::Kvs, Some("sled")) => bail!("Can't open sled engine in kvs directory"),
            (_, Some("sled")) => Engine::Sled(SledDb(sled::open(p)?)),

            (_, Some(_)) => bail!("Invalid engine type!"),

            (PreviousEngine::Sled, None) => Engine::Sled(SledDb(sled::open(p.as_ref())?)),
            (PreviousEngine::Kvs | PreviousEngine::None, None) => {
                Engine::Kvs(KvStore::open_with_policy(p.as_ref(), compaction_policy)?)
            }
        };
        Ok(Self { engine })
    }

    pub fn handle_cmd(&mut self, cmd: Cmd) -> Response {
        match cmd {
            Cmd::Set(k, v) => match self.engine.set(k.to_string(), &v) {
                Ok(_) => Response::Ok,
                Err(e) => {
                    warn!(?e, "Failed to set key to value");
                    Response::Err(e)
                }
            },
            Cmd::Get(k) => match self.engine.get(k) {
                Ok(Some(val)) => Response::OkGet(val),
                Ok(None) => Response::KeyNotFound,
                Err(e) => Response::Err(e),
            },
            Cmd::Rm(k) => match self.engine.remove(k) {
                Ok(()) => Response::Ok,
                Err(e) => {
                    warn!(?e, "Failed to remove key");
                    Response::Err(e)
                }
            },
        }
    }
}

impl<C> fmt::Display for KvsServer<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.engine {
            Engine::Kvs(_) => f.write_str("kvs"),
            Engine::Sled(_) => f.write_str("sled"),
        }
    }
}

enum Engine<C> {
    Kvs(KvStore<C>),
    Sled(sled_engine::SledDb),
}
impl<C: CompactionPolicy> KvsEngine for Engine<C> {
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
