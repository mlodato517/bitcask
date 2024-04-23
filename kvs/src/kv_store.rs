//! A key-value store. This has an API similar to the standard library's `HashMap`.

use std::borrow::{Borrow, Cow};
use std::collections::HashMap;
use std::fs::File;
use std::io::{Seek, SeekFrom};
use std::path::PathBuf;

use protocol::{Cmd, Reader};
use tracing::{debug, trace};

use crate::compaction_policy::{CompactionContext, CompactionPolicy, MaxFilePolicy};
use crate::engine::KvsEngine;
use crate::file_util;
use crate::{Error, Result};

// TODO Need to find a balance between:
//     1. Not opening too many files (i.e. larger files)
//     2. Having files be quick to read in (i.e. smaller files)
const FILE_SIZE_LIMIT: u64 = 1024 * 1024;
const ACTIVE_FILE_IDX: usize = usize::MAX;

/// A key-value store to associate values with keys. Key-value pairs can be inserted, looked up,
/// and removed.
pub struct KvStore<C = MaxFilePolicy> {
    active_file: LogFile,
    compaction_policy: C,
    dead_data_count: usize,
    dir: PathBuf,
    immutable_files: Vec<LogFile>,
    index: HashMap<String, Index>,
    cmd_reader: Reader,
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
        let active_file = LogFile::new(active_file)?;

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
            cmd_reader: Default::default(),
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
                Self::hydrate_file(&mut self.index, &mut self.cmd_reader, &mut f.file, file_idx)?;
        }
        Self::hydrate_file(
            &mut self.index,
            &mut self.cmd_reader,
            &mut self.active_file.file,
            ACTIVE_FILE_IDX,
        )?;
        Ok(())
    }

    fn hydrate_file(
        in_memory_index: &mut HashMap<String, Index>,
        reader: &mut Reader,
        file: &mut File,
        file_idx: usize,
    ) -> Result<usize> {
        let mut dead_data_count = 0;
        file.rewind()?;

        let mut file_offset = 0;
        while let Some(read_result) = reader.read_cmd(&mut *file)? {
            let bytes_read = read_result.bytes_read();
            let index = Index {
                file_idx,
                file_offset,
            };
            let previous_value = match read_result.into_cmd() {
                Cmd::Set(key, _) => in_memory_index.insert(key.into_owned(), index),
                Cmd::Rm(key) => in_memory_index.remove(key.as_ref()),

                // TODO Should there be another type to prevent this confusion?
                Cmd::Get(_) => panic!("Found Get command stored in file!"),
            };
            if let Some(_previous_value) = previous_value {
                dead_data_count += 1;
            }

            file_offset += bytes_read as u64;
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

        for file_index in self.index.values_mut() {
            // Only compact immutable files
            if file_index.file_idx == ACTIVE_FILE_IDX {
                continue;
            }
            let mut file = &self.immutable_files[file_index.file_idx].file;

            file.seek(SeekFrom::Start(file_index.file_offset))?;
            let cmd = self
                .cmd_reader
                .read_cmd(file)?
                .expect("Should be command at position indicated by index")
                .into_cmd();

            // TODO Extract with write_cmd
            let file_offset = compacted_file.len;
            let bytes_written = cmd.write(&mut compacted_file.file)?;
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
    fn write_cmd(&mut self, cmd: Cmd) -> Result<()> {
        let f = &mut self.active_file;
        let file_offset = f.len;

        let len = cmd.write(&f.file)?;
        f.len += len as u64;

        let key = match cmd {
            // TODO Should there be another type to prevent this confusion?
            Cmd::Rm(key) | Cmd::Get(key) => key,
            Cmd::Set(key, _) => key,
        };

        let index = Index {
            file_offset,
            file_idx: ACTIVE_FILE_IDX,
        };

        if let Some(previous_value) = self.index.insert(key.into_owned(), index) {
            if previous_value.file_idx != ACTIVE_FILE_IDX {
                self.dead_data_count += 1;
            }
        }

        // TODO Configure?
        if file_offset > FILE_SIZE_LIMIT {
            let next_file = self.dir.join(file_util::file_name());
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
            }) => {
                let mut file = match *file_idx {
                    ACTIVE_FILE_IDX => &self.active_file.file,
                    idx => &self.immutable_files[idx].file,
                };
                file.seek(SeekFrom::Start(*file_offset))?;

                // TODO This copies from file -> reader -> String output.
                // We should be able to save a copy by copying directly to the String...
                match self
                    .cmd_reader
                    .read_cmd(file)?
                    .expect("Should be command at position indicated by index")
                    .into_cmd()
                {
                    Cmd::Set(_, value) => Ok(Some(value.into_owned())),
                    Cmd::Rm(_) => panic!("Rm'ved keys shouldn't be in the index!"),
                    // TODO Should there be another type to prevent this confusion?
                    Cmd::Get(_) => panic!("Get commands shouldn't be written!"),
                }
            }
            None => Ok(None),
        }
    }

    /// Associate the passed value with the passed key in the store. This can later be retrieved
    /// with `get`.
    fn set<V: AsRef<str>>(&mut self, key: String, value: V) -> Result<()> {
        let cmd = Cmd::Set(key.into(), Cow::Borrowed(value.as_ref()));
        self.write_cmd(cmd)
    }

    /// Removes the associated value for the specified key.
    fn remove<K: Borrow<str>>(&mut self, key: K) -> Result<()> {
        match self.get(key.borrow())? {
            Some(_) => {
                debug!("Key found, deleting it");
                let result = self.write_cmd(Cmd::Rm(key.borrow().into()));
                if result.is_ok() {
                    trace!(key = ?key.borrow(), "Removing key from in-memory index");
                    self.index.remove(key.borrow());
                }
                result
            }
            None => {
                debug!("Key to remove not found");
                Err(Error::msg("Key not found"))
            }
        }
    }
}
