use std::fs::File;
use std::io::{BufWriter, Read, Seek, SeekFrom};
use std::path::PathBuf;

use anyhow::Result;
use protocol::Cmd;

use crate::command::Command;

use super::LogFile;

pub(crate) struct ActiveFile {
    path: PathBuf,
    file: BufWriter<File>,
    len: u64,
}
impl ActiveFile {
    pub(crate) fn new(path: PathBuf) -> Result<Self> {
        let file = File::options()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)?;
        let len = file.metadata()?.len();
        let file = BufWriter::new(file);
        Ok(Self { path, file, len })
    }

    pub(crate) fn as_reader(&self) -> impl Read + '_ {
        // TODO Rewind first?
        self.file.get_ref()
    }

    pub(crate) fn read_at(&mut self, offset: u64) -> Result<Option<String>> {
        // TODO impl Read or FileExt or something?
        self.file.seek(SeekFrom::Start(offset))?;

        let mut buf = vec![0; 32];
        let cmd = Cmd::read(self.file.get_ref(), &mut buf)?;
        match Command::try_from(cmd)? {
            Command::Set(_, value) => Ok(Some(value.into_owned())),
            Command::Rm(_) => Ok(None),
        }
    }

    pub(crate) fn write(&mut self, cmd: &Command) -> Result<u64> {
        let file_offset = self.len;

        let bytes_written = cmd.as_cmd().writeln(&mut self.file)?;
        self.len += bytes_written as u64;

        Ok(file_offset)
    }

    pub(crate) fn into_log_file(self) -> Result<LogFile> {
        Ok(LogFile {
            path: self.path,
            file: self.file.into_inner()?,
            len: self.len,
        })
    }
}
