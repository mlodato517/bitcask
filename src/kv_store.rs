//! A key-value store. This has an API similar to the standard library's `HashMap`.

use std::borrow::{Borrow, Cow};
use std::collections::HashMap;
use std::fs::File;
use std::hash::Hash;
use std::io::{BufRead, BufReader, Seek, SeekFrom, Write};
use std::path::PathBuf;

use thiserror::Error;

use crate::Command;

const COMPACTION_THRESHOLD: u64 = 4096 * 1000;

/// A key-value store to associate values with keys. Key-value pairs can be inserted, looked up,
/// and removed.
pub struct KvStore {
    index: HashMap<String, u64>,
    // TODO Multiple files.
    // https://github.com/basho/bitcask/blob/develop/doc/bitcask-intro.pdf
    active_file: File,
    active_path: PathBuf,
}

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] std::io::Error),

    // TODO Realistically this is IO for writing to disk...
    #[error(transparent)]
    Serialize(#[from] serde_json::Error),

    #[error("Key not found")]
    KeyNotFound,

    #[error("Not a directory")]
    InvalidDirectory,
}

pub type Result<T> = std::result::Result<T, Error>;

impl KvStore {
    /// Create a new key-value store which derives its state from the passed logfile. If the path
    /// is a directory, the state is stored in the file "log.log".
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let mut path = path.into();
        if !path.is_dir() {
            return Err(Error::InvalidDirectory);
        }

        path.push("log.log");
        let file = std::fs::File::options()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)?;
        let mut this = Self {
            index: Default::default(),
            active_file: file,
            active_path: path,
        };
        this.hydrate()?;
        Ok(this)
    }

    /// Associate the passed value with the passed key in the store. This can later be retrieved
    /// with `get`.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Command::Set(key.into(), value.into());
        self.write_cmd(cmd)
    }

    /// Gets the value currently associated with the key, if there is one.
    pub fn get<K>(&self, key: K) -> Result<Option<String>>
    where
        K: Borrow<str> + Eq + Hash,
    {
        match self.index.get(key.borrow()) {
            Some(pos) => {
                let mut f = BufReader::new(&self.active_file);
                f.seek(SeekFrom::Start(*pos))?;
                let line = f.lines().next().expect("Should be a line here")?;

                // TODO Consider -O mode or something to switch from JSON to something tighter
                let cmd: Command = serde_json::from_str(&line)?;
                match cmd {
                    Command::Set(_, value) => Ok(Some(value.into_owned())),
                    Command::Rm(_) => Ok(None),
                }
            }
            None => Ok(None),
        }
    }

    /// Removes the associated value for the specified key.
    pub fn remove(&mut self, key: String) -> Result<()> {
        match self.get(&*key)? {
            Some(_) => {
                let cmd = Command::Rm(key.into());
                self.write_cmd(cmd)
            }
            None => Err(Error::KeyNotFound),
        }
    }

    /// Builds an index of log pointers from the stored path. After this, gets are optimized to
    /// just read the most recent command for the key in the file.
    fn hydrate(&mut self) -> Result<()> {
        let f = &mut self.active_file;
        f.rewind()?;
        let mut f = BufReader::new(&self.active_file);

        let mut line = String::new();
        let mut pos = 0;
        while f.read_line(&mut line)? > 0 {
            let line_len = line.len();
            let trimmed = line.trim_end();

            let command: Command = serde_json::from_str(trimmed)?;
            match command {
                Command::Set(key, _) => self.index.insert(key.into_owned(), pos),
                Command::Rm(key) => self.index.insert(key.into_owned(), pos),
            };

            pos += line_len as u64;
            line.clear();
        }
        Ok(())
    }

    /// Appends the command to he end of the file with a trailing newline
    fn write_cmd(&mut self, cmd: Command) -> Result<()> {
        let f = &mut self.active_file;
        let len = f.metadata()?.len();
        writeln!(f, "{}", serde_json::to_string(&cmd)?)?;

        let key = match cmd {
            Command::Rm(key) => key,
            Command::Set(key, _) => key,
        };

        self.index.insert(key.into_owned(), len);

        if len > COMPACTION_THRESHOLD {
            self.compactify()?;
        }
        Ok(())
    }

    // TODO More atomically?
    fn compactify(&mut self) -> Result<()> {
        let old_path = self.active_path.clone();
        let new_path = &mut self.active_path;
        new_path.pop();
        new_path.push("log.new");
        let mut new_file = std::fs::File::options()
            .create(true)
            .read(true)
            .append(true)
            .open(&new_path)?;
        for key in self.index.keys() {
            if let Some(value) = self.get(&**key)? {
                let cmd = Command::Set(Cow::Borrowed(key), Cow::Owned(value));
                writeln!(&mut new_file, "{}", serde_json::to_string(&cmd)?)?;
            }
        }
        self.active_file = new_file;
        std::fs::rename(&self.active_path, old_path)?;
        self.index.clear();
        self.hydrate()?;

        Ok(())
    }
}
