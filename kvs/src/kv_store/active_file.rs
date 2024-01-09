use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use anyhow::Result;
use protocol::Cmd;
use tracing::error;

use crate::command::Command;

use super::FILE_SIZE_LIMIT;

pub(crate) struct ActiveFile {
    path: PathBuf,
    file: File,
    len: u64,
    memory_map: Vec<u8>,
}

impl Drop for ActiveFile {
    fn drop(&mut self) {
        if let Err(e) = self.file.write_all(&self.memory_map[..self.len as usize]) {
            error!(
                ?e,
                "Failed to flush writes -- lost some state ... maybe we should flush \
                   intermittently and/or only flush the parts we changed!"
            );
        }
    }
}

impl ActiveFile {
    pub(crate) fn new(path: PathBuf) -> Result<Self> {
        let mut file = File::options()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)?;
        let len = file.metadata()?.len();

        let memory_map = {
            let mut buf = Vec::with_capacity(FILE_SIZE_LIMIT as usize);
            file.read_to_end(&mut buf)?;
            buf
        };

        Ok(Self {
            path,
            file,
            len,
            memory_map,
        })
    }

    pub(crate) fn as_reader(&self) -> impl Read + '_ {
        &*self.memory_map
    }

    pub(crate) fn read_at(&mut self, offset: u64) -> Result<Option<String>> {
        // TODO Can maybe optimize this now :thinking:
        let file: &[u8] = &self.memory_map[offset as usize..];
        let mut buf = vec![0; 32];
        let cmd = Cmd::read(file, &mut buf)?;
        match Command::try_from(cmd)? {
            Command::Set(_, value) => Ok(Some(value.into_owned())),
            Command::Rm(_) => Ok(None),
        }
    }

    pub(crate) fn write(&mut self, cmd: Command) -> Result<u64> {
        let file_offset = self.len;

        let cmd = Cmd::from(cmd);

        // This should be encapsulated better, but this might be enough to evaluate the performance
        // impact.
        let newline_len = 1;
        let required_len = cmd.len() + newline_len + self.len as usize;
        if required_len > self.memory_map.len() {
            self.memory_map.resize(required_len, 0);
        }

        let bytes_written = cmd.writeln(&mut self.memory_map[self.len as usize..])?;
        self.len += bytes_written as u64;

        Ok(file_offset)
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}
