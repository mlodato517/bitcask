use std::fs::File;
use std::io::{Seek, SeekFrom};
use std::path::Path;

use protocol::Cmd;

use crate::{Command, Result};

pub(crate) fn open_file(path: impl AsRef<Path>) -> Result<File> {
    Ok(std::fs::File::options()
        .create(true)
        .read(true)
        .append(true)
        .open(path)?)
}

pub(crate) fn file_name() -> String {
    // TODO versioning
    format!(
        "{}.pingcap",
        time::OffsetDateTime::now_utc()
            .format(&time::format_description::well_known::Rfc3339)
            .expect("RFC-3339 is a valid format")
    )
}

pub(crate) fn seek_file_for_value(mut file: &File, file_offset: u64) -> Result<Option<String>> {
    file.seek(SeekFrom::Start(file_offset))?;

    let mut buf = vec![0; 32];
    let cmd = Cmd::read(file, &mut buf)?;
    match Command::try_from(cmd)? {
        Command::Set(_, value) => Ok(Some(value.into_owned())),
        Command::Rm(_) => Ok(None),
    }
}
