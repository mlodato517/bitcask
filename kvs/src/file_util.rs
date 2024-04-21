use std::fs::File;
use std::path::Path;

use crate::Result;

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
