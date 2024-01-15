use std::fs::File;
use std::io::Read;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};

use anyhow::Result;
use memmap2::MmapMut;
use protocol::Cmd;
use tracing::error;

use crate::command::Command;

use super::FILE_SIZE_LIMIT;

pub(crate) struct ActiveFile {
    path: PathBuf,
    file: File,
    len: u64,
    // This might be a Bad Idea -- https://db.cs.cmu.edu/mmap-cidr2022
    memory_map: MmapMut,
}

impl Drop for ActiveFile {
    fn drop(&mut self) {
        if let Err(e) = self.memory_map.flush_range(0, self.len as usize) {
            error!(
                ?e,
                "Failed to flush writes -- lost some state ... maybe we should flush \
                   intermittently and/or only flush the parts we changed!"
            );
        }
        if let Err(e) = self.file.set_len(self.len) {
            error!(?e, "Failed to truncat file");
        }
    }
}

impl ActiveFile {
    pub(crate) fn new(path: PathBuf) -> Result<Self> {
        let file = File::options()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)?;
        let len = file.metadata()?.len();

        // Need to give us room to write to in our memory map.
        if len < FILE_SIZE_LIMIT {
            file.set_len(FILE_SIZE_LIMIT)?;
        }
        let memory_map = unsafe { MmapMut::map_mut(file.as_raw_fd())? };

        Ok(Self {
            path,
            file,
            len,
            memory_map,
        })
    }

    pub(crate) fn as_reader(&self) -> impl Read + '_ {
        &self.memory_map[..self.len as usize]
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
            // Drop the memory map so it's safe to modify the file again
            self.memory_map = MmapMut::map_anon(0)?;
            self.file.set_len(required_len as u64)?;
            self.memory_map = unsafe { MmapMut::map_mut(self.file.as_raw_fd())? };
        }

        let bytes_written = cmd.writeln(&mut self.memory_map[self.len as usize..])?;
        self.len += bytes_written as u64;

        Ok(file_offset)
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}
