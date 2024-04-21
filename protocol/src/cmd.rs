//! [`Cmd`] represents a request to perform an action on a key.

use std::borrow::Cow;
use std::io::Write;

// TODO More specific crate error
use anyhow::{Error, Result};

pub use reader::Reader;

mod reader;

/// Enumeration of actions that can be performed.
//
// Implementation details:
//
// The current protocol is:
//   1. All commands start with 4 bytes for the key length and 8 bytes for the value length.
//   2. `Get` commands always specify a value length of `GET_VALUE_LEN`. Similarly for `Rm`.
//   3. Following this header, the key is stored.
//   4. Finally, for `Set` commands, the value is stored.
//
// TODO Can we make these comments unnecessary with a descriptive trait?
#[derive(Debug, PartialEq)]
pub enum Cmd<'a> {
    /// Command to set a key to a value.
    Set(Cow<'a, str>, Cow<'a, str>),
    /// Command to get the value of a key, if present.
    Get(Cow<'a, str>),
    /// Command to remove a key.
    Rm(Cow<'a, str>),
}

const HEADER_KEY_BYTES: usize = 4;
const HEADER_VALUE_BYTES: usize = 8;
const HEADER_BYTES: usize = HEADER_KEY_BYTES + HEADER_VALUE_BYTES;

const GET_VALUE_LEN: u64 = u64::MAX;
const RM_VALUE_LEN: u64 = GET_VALUE_LEN - 1;

impl<'a> Cmd<'a> {
    /// Writes the `Cmd` into the provided writer and returns the number of bytes written.
    pub fn write<W: Write>(&self, mut w: W) -> Result<usize> {
        match self {
            Self::Set(key, value) => {
                // TODO is the any value in buffering these?
                w.write_all(&(key.len() as u32).to_be_bytes())?;
                w.write_all(&(value.len() as u64).to_be_bytes())?;
                w.write_all(key.as_bytes())?;
                w.write_all(value.as_bytes())?;
                Ok(HEADER_BYTES + key.len() + value.len())
            }
            Self::Get(key) => {
                w.write_all(&(key.len() as u32).to_be_bytes())?;
                w.write_all(&GET_VALUE_LEN.to_be_bytes())?;
                w.write_all(key.as_bytes())?;
                Ok(HEADER_BYTES + key.len())
            }
            Self::Rm(key) => {
                w.write_all(&(key.len() as u32).to_be_bytes())?;
                w.write_all(&RM_VALUE_LEN.to_be_bytes())?;
                w.write_all(key.as_bytes())?;
                Ok(HEADER_BYTES + key.len())
            }
        }
    }

    /// Parses the passed bytes into key and value lengths.
    pub(crate) fn parse_header(header: [u8; HEADER_BYTES]) -> (u32, u64) {
        let (key_len, value_len) = header.split_at(HEADER_KEY_BYTES);

        let key_len = u32::from_be_bytes(key_len.try_into().expect("specified 4 bytes"));
        let value_len = u64::from_be_bytes(value_len.try_into().expect("specified 8 bytes"));

        (key_len, value_len)
    }

    /// Parses the passed bytes into a command, using the provided key and value lengths.
    pub(crate) fn parse_body(key_len: u32, value_len: u64, bytes: &'a [u8]) -> Result<Self> {
        if bytes.len() < key_len as usize {
            return Err(Error::msg("Insufficient data for key"));
        }

        let (key_bytes, value_bytes) = bytes.split_at(key_len as usize);
        let key =
            std::str::from_utf8(key_bytes).map_err(|e| Error::new(e).context("Non-UTF8 key"))?;

        match value_len {
            GET_VALUE_LEN => Ok(Self::Get(key.into())),
            RM_VALUE_LEN => Ok(Self::Rm(key.into())),
            value_len => {
                let value_bytes = value_bytes
                    .get(..value_len as usize)
                    .ok_or_else(|| Error::msg("Insufficient data for value"))?;
                let value = std::str::from_utf8(value_bytes)
                    .map_err(|e| Error::new(e).context("Non-UTF8 value"))?;
                Ok(Self::Set(key.into(), value.into()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper for tests
    fn parse(cmd: &[u8]) -> Result<Cmd> {
        if cmd.len() < HEADER_BYTES {
            return Err(Error::msg("Missing proper header"));
        }

        let (header, rest) = cmd.split_at(HEADER_BYTES);
        let (key_len, value_len) =
            Cmd::parse_header(header.try_into().expect("specified 12 bytes"));

        Cmd::parse_body(key_len, value_len, rest)
    }

    #[test]
    fn set() {
        let mut bytes = Vec::new();
        bytes.extend(3u32.to_be_bytes());
        bytes.extend(6u64.to_be_bytes());
        bytes.extend(b"foofoobar");

        let actual = parse(&bytes).unwrap();
        let expected = Cmd::Set("foo".into(), "foobar".into());
        assert_eq!(actual, expected);
    }

    #[test]
    fn rm() {
        let mut bytes = Vec::new();
        bytes.extend(3u32.to_be_bytes());
        bytes.extend(RM_VALUE_LEN.to_be_bytes());
        bytes.extend(b"foo");

        let actual = parse(&bytes).unwrap();
        let expected = Cmd::Rm("foo".into());
        assert_eq!(actual, expected);
    }

    #[test]
    fn get() {
        let mut bytes = Vec::new();
        bytes.extend(3u32.to_be_bytes());
        bytes.extend(GET_VALUE_LEN.to_be_bytes());
        bytes.extend(b"foo");

        let actual = parse(&bytes).unwrap();
        let expected = Cmd::Get("foo".into());
        assert_eq!(actual, expected);
    }

    #[test]
    fn set_ignore_extra() {
        let mut bytes = Vec::new();
        bytes.extend(3u32.to_be_bytes());
        bytes.extend(6u64.to_be_bytes());
        bytes.extend(b"foofoobar_ignoreme");

        let actual = parse(&bytes).unwrap();
        let expected = Cmd::Set("foo".into(), "foobar".into());
        assert_eq!(actual, expected);
    }

    #[test]
    fn rm_ignore_extra() {
        let mut bytes = Vec::new();
        bytes.extend(3u32.to_be_bytes());
        bytes.extend(RM_VALUE_LEN.to_be_bytes());
        bytes.extend(b"foo_ignoreme");

        let actual = parse(&bytes).unwrap();
        let expected = Cmd::Rm("foo".into());
        assert_eq!(actual, expected);
    }

    #[test]
    fn get_ignore_extra() {
        let mut bytes = Vec::new();
        bytes.extend(3u32.to_be_bytes());
        bytes.extend(GET_VALUE_LEN.to_be_bytes());
        bytes.extend(b"foo_ignoreme");

        let actual = parse(&bytes).unwrap();
        let expected = Cmd::Get("foo".into());
        assert_eq!(actual, expected);
    }

    #[test]
    fn set_identity() {
        let key = "abc";
        let value = "defg";
        let proto = Cmd::Set(Cow::Borrowed(key), Cow::Borrowed(value));

        let mut buf = vec![];

        assert_eq!(proto.write(&mut buf).unwrap(), 19);

        assert_eq!(parse(&buf).unwrap(), proto);
    }

    #[test]
    fn get_identity() {
        let key = "abc";
        let proto = Cmd::Get(Cow::Borrowed(key));

        let mut buf = vec![];

        assert_eq!(proto.write(&mut buf).unwrap(), 15);

        assert_eq!(parse(&buf).unwrap(), proto);
    }

    #[test]
    fn rm_identity() {
        let key = "abc";
        let proto = Cmd::Rm(Cow::Borrowed(key));

        let mut buf = vec![];

        assert_eq!(proto.write(&mut buf).unwrap(), 15);

        assert_eq!(parse(&buf).unwrap(), proto);
    }

    mod len_check_tests {
        use super::*;

        #[test]
        fn key_len_chec() {
            let mut bytes = Vec::new();
            bytes.extend(5u32.to_be_bytes());
            bytes.extend(GET_VALUE_LEN.to_be_bytes());
            bytes.extend(b"fo");

            assert!(parse(&bytes).is_err());
        }

        #[test]
        fn set_checks_len() {
            let mut bytes = Vec::new();
            bytes.extend(5u32.to_be_bytes());
            bytes.extend(5u64.to_be_bytes());
            bytes.extend(b"foolsbars");

            assert!(parse(&bytes).is_err());
        }

        #[test]
        fn rm_checks_len() {
            let mut bytes = Vec::new();
            bytes.extend(5u32.to_be_bytes());
            bytes.extend(RM_VALUE_LEN.to_be_bytes());
            bytes.extend(b"fool");

            assert!(parse(&bytes).is_err());
        }
        #[test]
        fn get_checks_len() {
            let mut bytes = Vec::new();
            bytes.extend(5u32.to_be_bytes());
            bytes.extend(GET_VALUE_LEN.to_be_bytes());
            bytes.extend(b"fool");

            assert!(parse(&bytes).is_err());
        }
    }

    #[test]
    fn empty_command() {
        assert!(parse(b"").is_err());
    }
}
