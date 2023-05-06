use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::path::Path;

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
        "{}.log",
        time::OffsetDateTime::now_utc()
            .format(&time::format_description::well_known::Rfc3339)
            .expect("RFC-3339 is a valid format")
    )
}

pub(crate) fn seek_file_for_value(file: &File, file_offset: u64) -> Result<Option<String>> {
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
