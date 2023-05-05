//! A key-value store. This has an API similar to the standard library's `HashMap`.

use std::borrow::{Borrow, Cow};
use std::collections::HashMap;
use std::fs::File;
use std::hash::Hash;
use std::io::{BufRead, BufReader, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use thiserror::Error;

use crate::Command;

// TODO Configurable? Factors to keep in mind:
// 1. don't want too many open files probably
// 2. don't want to have files so big that rewriting the index takes so much memory
// 3. don't want files so small that we have to open new ones all the time
//
// Also, we should probably compact based on some knowledge of dead values vs the number of open
// files. If all the files are open but contain unique data then we'll just be retriggering
// compactions with no benefit. We could, when we update the file index of a value, increment some
// counter and when we have enough "dead" lines we can compact. This assumes that a majority of
// those dead lines are in immutable files but we could also keep track of that because, when we
// update a value
const FILE_SIZE_LIMIT: u64 = 1024 * 1024;
const FILE_LIMIT: usize = 8;

const ACTIVE_FILE_IDX: usize = usize::MAX;

/// A key-value store to associate values with keys. Key-value pairs can be inserted, looked up,
/// and removed.
pub struct KvStore {
    index: HashMap<String, Index>,
    active_file: LogFile,
    immutable_files: Vec<LogFile>,
    dir: PathBuf,
}
struct Index {
    file_idx: usize,
    file_offset: u64,
}
struct LogFile {
    path: PathBuf,
    file: File,
}
impl LogFile {
    fn new(path: PathBuf) -> Result<Self> {
        open_file(&path).map(|file| Self { path, file })
    }
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
    /// TODO
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let dir_path = path.into();
        let dir = std::fs::read_dir(&dir_path)?;
        let mut paths = dir
            .into_iter()
            .map(|dir_entry| Ok(dir_entry.map(|dir| dir.path())?))
            .collect::<Result<Vec<_>>>()?;
        paths.sort_unstable();

        let active_file = paths.pop().unwrap_or_else(|| {
            let mut buf = dir_path.clone();
            // TODO versioning
            buf.push(file_name());
            buf
        });
        let active_file = LogFile::new(active_file)?;

        let immutable_files = paths
            .into_iter()
            .map(LogFile::new)
            .collect::<Result<Vec<_>>>()?;

        let mut this = Self {
            index: Default::default(),
            active_file,
            immutable_files,
            dir: dir_path,
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
            Some(Index {
                file_offset,
                file_idx,
            }) => {
                let file = match *file_idx {
                    ACTIVE_FILE_IDX => &self.active_file.file,
                    idx => &self.immutable_files[idx].file,
                };
                seek_file_for_value(file, *file_offset)
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
        for (file_idx, f) in self.immutable_files.iter_mut().enumerate() {
            Self::hydrate_file(&mut self.index, &mut f.file, file_idx)?;
        }
        Self::hydrate_file(&mut self.index, &mut self.active_file.file, ACTIVE_FILE_IDX)?;
        Ok(())
    }

    fn hydrate_file(
        in_memory_index: &mut HashMap<String, Index>,
        file: &mut File,
        file_idx: usize,
    ) -> Result<()> {
        file.rewind()?;
        let mut f = BufReader::new(file);

        let mut line = String::new();
        let mut file_offset = 0;
        while f.read_line(&mut line)? > 0 {
            let line_len = line.len();
            let trimmed = line.trim_end();

            let command: Command = serde_json::from_str(trimmed)?;
            let index = Index {
                file_idx,
                file_offset,
            };
            match command {
                Command::Set(key, _) => in_memory_index.insert(key.into_owned(), index),
                Command::Rm(key) => in_memory_index.insert(key.into_owned(), index),
            };

            file_offset += line_len as u64;
            line.clear();
        }
        Ok(())
    }

    /// Appends the command to the end of the file with a trailing newline
    fn write_cmd(&mut self, cmd: Command) -> Result<()> {
        let f = &mut self.active_file;
        let file_offset = f.file.metadata()?.len();
        writeln!(f.file, "{}", serde_json::to_string(&cmd)?)?;

        let key = match cmd {
            Command::Rm(key) => key,
            Command::Set(key, _) => key,
        };

        let index = Index {
            file_offset,
            file_idx: ACTIVE_FILE_IDX,
        };

        self.index.insert(key.into_owned(), index);

        if file_offset > FILE_SIZE_LIMIT {
            let mut next_file = self.dir.clone();
            next_file.push(file_name());
            let file = LogFile::new(next_file)?;
            let old_file = std::mem::replace(&mut self.active_file, file);
            self.immutable_files.push(old_file);

            // Any indexed values for the active file now get moved to reference the immutable file
            // list.
            for file_index in self.index.values_mut() {
                if file_index.file_idx == ACTIVE_FILE_IDX {
                    file_index.file_idx = self.immutable_files.len() - 1;
                }
            }
        }
        if self.immutable_files.len() > FILE_LIMIT {
            println!("Compacting files");
            self.compactify()?;
        }

        Ok(())
    }

    // TODO More atomically? How do we handle concurrent compaction requests? Should probably take
    // `&self` or work on a separate thread or something.
    // One option here could be to:
    //     1. hydrate an in-memory index from the immutable file list
    //     2. write a new file with the final values
    //     3. use an AtomicUsize in all the Index structs so we can atomically update them to point
    //        to this new file
    //     4. lock a mutex to go ahead and delete all the compacted files
    //       a. technically it seems fine to delete the files while we hold handles to them
    //          so maybe we just do that? The file list in memory might grow unboundedly but
    //          that should take up much much less space than the files on disk? Then we'd
    //          need to introduce probably an AtomicBool to keep track of which files have
    //          been compacted so we don't count them when seeing if we have "too many files".
    // TODO hint file?
    // https://github.com/basho/bitcask/blob/develop/doc/bitcask-intro.pdf
    fn compactify(&mut self) -> Result<()> {
        // TODO Hack to ensure we don't consider this file active the next time around
        let compacted_file_name = format!("0000-{}", file_name());
        let mut compacted_path = self.dir.clone();
        compacted_path.push(compacted_file_name);
        let mut compacted_file = LogFile::new(compacted_path)?;

        for (key, file_index) in &mut self.index {
            // Only compact immutable files
            if file_index.file_idx == ACTIVE_FILE_IDX {
                continue;
            }
            // TODO Extract with write_cmd
            let file_offset = compacted_file.file.metadata()?.len();
            let file = &self.immutable_files[file_index.file_idx].file;
            let value = seek_file_for_value(file, file_index.file_offset)?
                .expect("Values in the index should be present");
            let cmd = Command::Set(Cow::Borrowed(key), Cow::Owned(value));
            writeln!(&mut compacted_file.file, "{}", serde_json::to_string(&cmd)?)?;

            *file_index = Index {
                file_idx: 0,
                file_offset,
            };
        }
        for log_file in self.immutable_files.drain(..) {
            std::fs::remove_file(log_file.path)?;
        }
        self.immutable_files.push(compacted_file);

        Ok(())
    }
}

fn open_file(path: impl AsRef<Path>) -> Result<File> {
    Ok(std::fs::File::options()
        .create(true)
        .read(true)
        .append(true)
        .open(path)?)
}

fn file_name() -> String {
    // TODO versioning
    format!(
        "{}.log",
        time::OffsetDateTime::now_utc()
            .format(&time::format_description::well_known::Rfc3339)
            .expect("RFC-3339 is a valid format")
    )
}
fn seek_file_for_value(file: &File, file_offset: u64) -> Result<Option<String>> {
    let mut f = BufReader::new(file);
    f.seek(SeekFrom::Start(file_offset))?;
    let line = f.lines().next().expect("Should be a line here")?;

    // TODO Consider -O mode or something to switch from JSON to something tighter
    let cmd: Command = serde_json::from_str(&line)?;
    match cmd {
        Command::Set(_, value) => Ok(Some(value.into_owned())),
        Command::Rm(_) => Ok(None),
    }
}
