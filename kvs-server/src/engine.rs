//! An [`Engine`] type that can process requests to the database.

use std::borrow::Borrow;
use std::fmt;
use std::path::Path;
use std::str::FromStr;

use anyhow::{anyhow, bail, Context, Error, Result};
use kvs::{KvStore, KvsEngine};

use sled_engine::SledDb;

mod sled_engine;

/// Represents a type of backing engine store.
#[derive(Debug, Clone, Copy)]
pub enum EngineType {
    Kvs,
    Sled,
}

impl fmt::Display for EngineType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Kvs => f.write_str("kvs"),
            Self::Sled => f.write_str("sled"),
        }
    }
}

impl FromStr for EngineType {
    type Err = Error;

    fn from_str(str: &str) -> Result<Self, Self::Err> {
        match str {
            "kvs" => Ok(Self::Kvs),
            "sled" => Ok(Self::Sled),
            other => Err(anyhow!("unknown engine type {other:?}")),
        }
    }
}

/// Static dispatch enum for `KvsEngine` implementations.
pub enum Engine {
    Kvs(KvStore),
    Sled(SledDb),
}

/// Enum describing inferred engine types from a directory's contents.
enum PreviousEngine {
    /// Data in the directory indicates the previous engine was of type `KvStore`.
    Kvs,
    /// Data in the directory indicates the previous engine was of type `SledDb`.
    Sled,
    /// Data in the directory does not indicate any engine type.
    None,
}

impl Engine {
    /// Opens a new `Engine` of the specified type in the specified directory:
    /// - If no type is specified, data in the existing directory is used to infer the engine type.
    /// - If the specified engine type doesn't match data in the existing directory, an error is
    ///   returned.
    /// - If no type is specified, and no previous data exists, [`KvStore`] is used by default.
    pub fn new_in(engine: Option<EngineType>, p: impl AsRef<Path>) -> Result<Self> {
        let prev_engine = Self::determine_previous_engine(p.as_ref())?;
        match (prev_engine, engine) {
            (PreviousEngine::Sled, Some(EngineType::Kvs)) => {
                bail!("Can't open kvs engine in sled directory")
            }
            (PreviousEngine::Kvs, Some(EngineType::Sled)) => {
                bail!("Can't open sled engine in kvs directory")
            }

            (_, Some(EngineType::Kvs)) => Ok(Engine::Kvs(KvStore::open(p.as_ref())?)),
            (_, Some(EngineType::Sled)) => Ok(Engine::Sled(SledDb(sled::open(p)?))),

            (PreviousEngine::Sled, None) => Ok(Engine::Sled(SledDb(sled::open(p.as_ref())?))),
            (PreviousEngine::Kvs | PreviousEngine::None, None) => {
                Ok(Engine::Kvs(KvStore::open(p.as_ref())?))
            }
        }
    }

    /// Reports the type of the engine.
    pub fn engine_type(&self) -> EngineType {
        match self {
            Self::Kvs(_) => EngineType::Kvs,
            Self::Sled(_) => EngineType::Sled,
        }
    }

    /// Checks the given directory path for files indicating which engine was previously used
    /// there.
    fn determine_previous_engine(p: &Path) -> Result<PreviousEngine> {
        for entry in std::fs::read_dir(p).context("reading previous engine dir")? {
            let file_name = entry?.file_name();
            if file_name == "conf" || file_name == "db" {
                return Ok(PreviousEngine::Sled);
            } else if file_name.to_string_lossy().ends_with(".pingcap") {
                return Ok(PreviousEngine::Kvs);
            }
        }

        Ok(PreviousEngine::None)
    }
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
