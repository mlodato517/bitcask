//! A [`Reader`] can be used for reading multiple [`Cmd`]s from arbitrary [`Read`] implementations.
//!
//! A `Reader` can be beneficial when reading one-command-per-`Read` implementation (e.g. looping
//! over network connections where each connection has one `Cmd`) or multiple-commands-per-`Read`
//! implementation (e.g. a file with multiple `Cmd`s in it).

use std::io::{ErrorKind, Read};

// TODO More specific crate error
use anyhow::{Context, Result};

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

struct CmdReader<R> {
    reader: R,
    state: CmdReadState,
}

enum CmdReadState {
    ReadingHeader {
        header_bytes: [u8; HEADER_BYTES],
        bytes_read: usize,
    },
    ReadingCmd {
        expected_len: usize,
        bytes_read: usize,
    },
}

impl<R> CmdReader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            state: CmdReadState::ReadingHeader {
                header_bytes: [0; HEADER_BYTES],
                bytes_read: 0,
            },
        }
    }
}

impl<R: Read> Read for CmdReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match &mut self.state {
            CmdReadState::ReadingHeader {
                header_bytes,
                bytes_read,
            } => {
                let bytes_left = header_bytes.len() - *bytes_read;
                let bytes_to_read = bytes_left.min(buf.len());
                let new_bytes_read = loop {
                    match self
                        .reader
                        .read(&mut header_bytes[*bytes_read..*bytes_read + bytes_to_read])
                    {
                        // Caller passed in an empty buffer, so we can't read anything.
                        Ok(0) => return Ok(0),
                        Ok(n) => break n,
                        Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                        Err(e) => return Err(e),
                    }
                };

                // Copy the read bytes into the caller's buffer.
                buf[..new_bytes_read]
                    .copy_from_slice(&header_bytes[*bytes_read..*bytes_read + new_bytes_read]);

                // Move to the next state if it's time.
                *bytes_read += new_bytes_read;
                if *bytes_read == header_bytes.len() {
                    let (key_len, value_len) = Cmd::parse_header(*header_bytes);
                    let total_len = match value_len {
                        GET_VALUE_LEN | RM_VALUE_LEN => key_len as usize,
                        value_len => key_len as usize + value_len as usize,
                    };

                    self.state = CmdReadState::ReadingCmd {
                        bytes_read: 0,
                        expected_len: total_len,
                    };
                }

                Ok(new_bytes_read)
            }
            CmdReadState::ReadingCmd {
                bytes_read,
                expected_len,
            } => {
                let bytes_left = *expected_len - *bytes_read;
                let bytes_to_read = bytes_left.min(buf.len());
                if bytes_to_read == 0 {
                    return Ok(0);
                }
                let new_bytes_read = loop {
                    match self.reader.read(&mut buf[..bytes_to_read]) {
                        Ok(n) => break n,
                        Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                        Err(e) => return Err(e),
                    }
                };

                *bytes_read += new_bytes_read;
                Ok(new_bytes_read)
            }
        }
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
    pub fn read_cmd(&mut self, reader: impl Read) -> Result<Option<ReadResult>> {
        let mut cmd_reader = CmdReader::new(reader);

        // Clear buffer because `read_to_end` appends bytes.
        self.buf.clear();
        let bytes_read = cmd_reader
            .read_to_end(&mut self.buf)
            .context("reading command bytes")?;

        if bytes_read == 0 {
            return Ok(None);
        }

        let (header_bytes, body_bytes) = self.buf.split_at(HEADER_BYTES);
        let (key_len, value_len) =
            Cmd::parse_header(header_bytes.try_into().expect("split at correct length"));

        let cmd =
            Cmd::parse_body(key_len, value_len, body_bytes).context("parsing command body")?;

        Ok(Some(ReadResult { cmd, bytes_read }))
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
