//! A [`Reader`] can be used for reading multiple [`Cmd`]s from arbitrary [`Read`] implementations.
//!
//! A `Reader` can be beneficial when reading one-command-per-`Read` implementation (e.g. looping
//! over network connections where each connection has one `Cmd`) or multiple-commands-per-`Read`
//! implementation (e.g. a file with multiple `Cmd`s in it).

use std::io::{ErrorKind, Read};

// TODO More specific crate error
use anyhow::{Error, Result};

use super::{Cmd, GET_VALUE_LEN, HEADER_BYTES, RM_VALUE_LEN};

/// Result of reading a command with a [`Reader`]. It communicates the [`Cmd`] and how many bytes
/// were read, as would be expected from a [`Read`] implementation.
#[derive(Debug, PartialEq)]
pub struct ReadResult<'a> {
    cmd: Cmd<'a>,
    bytes_read: usize,
}

impl<'a> ReadResult<'a> {
    /// See how many bytes were read in order to read this command.
    pub fn bytes_read(&self) -> usize {
        self.bytes_read
    }

    /// Consume the `ReadResult` and return the read command.
    pub fn into_cmd(self) -> Cmd<'a> {
        self.cmd
    }
}

/// Reader for facilitating reads of [`Cmd`]s from arbitrary readers. This provides two main
/// benefits:
///
///   1. A single, consolidated allocation to read bytes into.
///   2. A simple interface for reading multiple commands from the same reader.
#[derive(Default)]
pub struct Reader {
    buf: Vec<u8>,
}

impl Reader {
    /// Instantiates a default `Reader`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Attempts to read a [`Cmd`] out of the provided reader.
    ///
    /// If the reader is empty, `Ok(None)` is returned. This allows for calling this in a loop on a
    /// reader to get every command out of it.
    ///
    /// If the reader fails to provide data, or if the reader has data not representing a `Cmd`, an
    /// `Err` is returned.
    pub fn read_cmd(&mut self, mut reader: impl Read) -> Result<Option<ReadResult>> {
        let mut header_bytes = [0u8; HEADER_BYTES];
        let mut total_bytes = 0;
        loop {
            match reader.read(&mut header_bytes[total_bytes..]) {
                Ok(0) => break,
                Ok(n) => total_bytes += n,
                Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(e) => return Err(Error::new(e).context("reading next command")),
            }
        }

        if total_bytes == 0 {
            return Ok(None);
        } else if total_bytes < HEADER_BYTES {
            return Err(Error::msg("not enough data in reader"));
        }

        let (key_len, value_len) = Cmd::parse_header(header_bytes);

        let total_len = match value_len {
            GET_VALUE_LEN | RM_VALUE_LEN => key_len as usize,
            value_len => key_len as usize + value_len as usize,
        };

        // Resize the buffer. This can result in truncation. This is useful because it ensures that
        // this next read call doesn't "over-read", which allows the Reader to read multiple
        // commands from the same source.
        self.buf.resize(total_len, 0);

        reader
            .read_exact(&mut self.buf)
            .map_err(|e| Error::new(e).context("reading cmd body"))?;

        let cmd = Cmd::parse_body(key_len, value_len, &self.buf)?;

        Ok(Some(ReadResult {
            cmd,
            bytes_read: HEADER_BYTES + total_len,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_each_cmd() {
        let mut bytes = Vec::new();

        let set = Cmd::Set("foo".into(), "foobar".into());
        set.write(&mut bytes).unwrap();

        let get = Cmd::Get("foo".into());
        get.write(&mut bytes).unwrap();

        let mut reader = Reader::new();
        let result = reader.read_cmd(&*bytes).unwrap().unwrap();

        assert_eq!(result.bytes_read(), 21);
        assert_eq!(result.into_cmd(), set);

        let result = reader.read_cmd(&bytes[21..]).unwrap().unwrap();

        assert_eq!(result.bytes_read(), 15);
        assert_eq!(result.into_cmd(), get);

        let result = reader.read_cmd(&bytes[36..]).unwrap();

        assert!(result.is_none());
    }
}
