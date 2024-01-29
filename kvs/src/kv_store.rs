//! A key-value store. This has an API similar to the standard library's `HashMap`.

use std::borrow::{Borrow, Cow};
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::PathBuf;

use hashbrown::HashMap;
use protocol::Cmd;
use tracing::debug;

use crate::compaction_policy::{CompactionContext, CompactionPolicy, MaxFilePolicy};
use crate::engine::KvsEngine;
use crate::file_util;
use crate::Command;
use crate::{Error, Result};
use active_file::ActiveFile;

mod active_file;

// TODO Need to find a balance between:
//     1. Not opening too many files (i.e. larger files)
//     2. Having files be quick to read in (i.e. smaller files)
const FILE_SIZE_LIMIT: u64 = 1024 * 1024;
const ACTIVE_FILE_IDX: usize = usize::MAX;

/// A key-value store to associate values with keys. Key-value pairs can be inserted, looked up,
/// and removed.
pub struct KvStore<C = MaxFilePolicy> {
    active_file: ActiveFile,
    compaction_policy: C,
    dead_data_count: usize,
    dir: PathBuf,
    immutable_files: Vec<LogFile>,
    index: HashMap<String, Index>,
}
struct Index {
    file_idx: usize,
    file_offset: u64,
}
struct LogFile {
    path: PathBuf,
    file: File,
    len: u64,
}
impl LogFile {
    fn new(path: PathBuf) -> Result<Self> {
        file_util::open_file(&path).and_then(|file| {
            let len = file.metadata()?.len();
            Ok(Self { path, file, len })
        })
    }
}

impl KvStore<MaxFilePolicy> {
    /// TODO
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore<MaxFilePolicy>> {
        Self::open_with_policy(path, MaxFilePolicy::default())
    }
}

impl<C> KvStore<C> {
    /// TODO
    pub fn open_with_policy(path: impl Into<PathBuf>, compaction_policy: C) -> Result<Self> {
        let dir_path = path.into();
        let dir = std::fs::read_dir(&dir_path)?;
        let mut paths = dir
            .into_iter()
            .map(|dir_entry| Ok(dir_entry.map(|dir| dir.path())?))
            .collect::<Result<Vec<_>>>()?;
        paths.sort_unstable();

        let active_file = paths
            .pop()
            .unwrap_or_else(|| dir_path.join(file_util::file_name()));
        let active_file = ActiveFile::new(active_file)?;

        let immutable_files = paths
            .into_iter()
            .map(LogFile::new)
            .collect::<Result<Vec<_>>>()?;

        let mut this = KvStore {
            index: Default::default(),
            active_file,
            immutable_files,
            dir: dir_path,
            dead_data_count: 0,
            compaction_policy,
        };

        this.hydrate()?;

        Ok(this)
    }

    /// Builds an index of log pointers from the stored path. After this, gets are optimized to
    /// just read the most recent command for the key in the file.
    fn hydrate(&mut self) -> Result<()> {
        for (file_idx, f) in self.immutable_files.iter_mut().enumerate() {
            // Count up the size of dead records in immutable files. When there are "enough", we
            // can compact all the immutable files into a single file.
            self.dead_data_count +=
                Self::hydrate_from_reader(&mut self.index, &mut f.file, file_idx)?;
        }
        Self::hydrate_from_reader(
            &mut self.index,
            self.active_file.as_reader(),
            ACTIVE_FILE_IDX,
        )?;
        Ok(())
    }

    fn hydrate_from_reader(
        in_memory_index: &mut HashMap<String, Index>,
        reader: impl Read,
        file_idx: usize,
    ) -> Result<usize> {
        let mut dead_data_count = 0;
        let mut f = BufReader::new(reader);

        let mut line = Vec::new();
        let mut file_offset = 0;
        while f.read_until(b'\n', &mut line)? > 0 {
            let line_len = line.len();

            let command: Command = Cmd::parse(&line)?.try_into()?;
            let index = Index {
                file_idx,
                file_offset,
            };
            let previous_value = match command {
                Command::Set(key, _) => in_memory_index.insert(key.into_owned(), index),
                Command::Rm(key) => in_memory_index.remove(key.as_ref()),
            };
            if let Some(_previous_value) = previous_value {
                dead_data_count += 1;
            }

            file_offset += line_len as u64;
            line.clear();
        }

        Ok(dead_data_count)
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
        let compacted_file_name = format!("0000-{}", file_util::file_name());
        let mut compacted_path = self.dir.clone();
        compacted_path.push(compacted_file_name);
        let mut compacted_file = LogFile::new(compacted_path)?;

        for (key, file_index) in &mut self.index {
            // Only compact immutable files
            if file_index.file_idx == ACTIVE_FILE_IDX {
                continue;
            }
            let file = &self.immutable_files[file_index.file_idx].file;
            let value = file_util::seek_file_for_value(file, file_index.file_offset)?
                .expect("Values in the index should be present");
            let cmd = Command::Set(Cow::Borrowed(key), Cow::Owned(value));

            // TODO Extract with write_cmd
            let file_offset = compacted_file.len;

            let bytes_written = cmd.as_cmd().writeln(&mut compacted_file.file)?;
            compacted_file.len += bytes_written as u64;

            *file_index = Index {
                file_idx: 0,
                file_offset,
            };
        }
        for log_file in self.immutable_files.drain(..) {
            std::fs::remove_file(log_file.path)?;
        }
        self.immutable_files.push(compacted_file);

        // TODO LIES
        self.dead_data_count = 0;

        Ok(())
    }
}

impl<C: CompactionPolicy> KvStore<C> {
    /// Appends the command to the end of the file with a trailing newline
    fn write_cmd(&mut self, cmd: Command) -> Result<()> {
        let file_offset = self.active_file.write(&cmd)?;

        match cmd {
            Command::Rm(key) => {
                if let Some(previous_value) = self.index.remove(&*key) {
                    if previous_value.file_idx != ACTIVE_FILE_IDX {
                        self.dead_data_count += 1;
                    }
                }
            }
            Command::Set(key, _) => {
                let key = key.into_owned();
                let index = Index {
                    file_offset,
                    file_idx: ACTIVE_FILE_IDX,
                };
                if let Some(previous_value) = self.index.insert(key, index) {
                    if previous_value.file_idx != ACTIVE_FILE_IDX {
                        self.dead_data_count += 1;
                    }
                }
            }
        }

        // TODO Configure?
        if file_offset > FILE_SIZE_LIMIT {
            let next_file = self.dir.join(file_util::file_name());
            let file = ActiveFile::new(next_file)?;
            let old_file = std::mem::replace(&mut self.active_file, file);
            self.immutable_files.push(old_file.into_log_file()?);

            // Any indexed values for the active file now get moved to reference the immutable file
            // list.
            // TODO Consider stable indices
            for file_index in self.index.values_mut() {
                if file_index.file_idx == ACTIVE_FILE_IDX {
                    file_index.file_idx = self.immutable_files.len() - 1;
                }
            }
        }

        let state = CompactionContext {
            open_immutable_files: self.immutable_files.len(),
            dead_commands: self.dead_data_count,
        };

        if CompactionPolicy::should_compact(&self.compaction_policy, state) {
            self.compactify()?;
        }

        Ok(())
    }
}

impl<C: CompactionPolicy> KvsEngine for KvStore<C> {
    /// Gets the value currently associated with the key, if there is one.
    fn get<K: Borrow<str>>(&mut self, key: K) -> Result<Option<String>> {
        match self.index.get(key.borrow()) {
            Some(Index {
                file_offset,
                file_idx,
            }) => match *file_idx {
                ACTIVE_FILE_IDX => self.active_file.read_at(*file_offset),
                idx => {
                    let file = &self.immutable_files[idx].file;
                    file_util::seek_file_for_value(file, *file_offset)
                }
            },
            None => Ok(None),
        }
    }

    /// Associate the passed value with the passed key in the store. This can later be retrieved
    /// with `get`.
    fn set<V: AsRef<str>>(&mut self, key: String, value: V) -> Result<()> {
        let cmd = Command::Set(key.into(), Cow::Borrowed(value.as_ref()));
        self.write_cmd(cmd)
    }

    /// Removes the associated value for the specified key.
    fn remove<K: Borrow<str>>(&mut self, key: K) -> Result<()> {
        match self.get(key.borrow())? {
            Some(_) => {
                debug!("Key found, deleting it");
                self.write_cmd(Command::Rm(key.borrow().into()))
            }
            None => {
                debug!("Key to remove not found");
                Err(Error::msg("Key not found"))
            }
        }
    }
}
