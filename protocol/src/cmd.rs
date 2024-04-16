//! [`Cmd`] represents a request to perform an action on a key.

use std::borrow::Cow;
use std::io::Write;

use tracing::debug;
// TODO More specific crate error
use anyhow::{Error, Result};

/// Enumeration of actions that can be performed.
//
// Implementation details:
//
// The current protocol is:
//   1. `Set` commands are encoded as "{},{},{}{}" where the first bracket is the key length in
//      ASCII, the second is the value length in ASCII, the third is the key, and the fourth is the
//      value.
//   2. `Get` commands are encoded as "g{},{}" where the first bracket is the key length in
//      ASCII and the second is the key.
//   3. `Rm` commands are encoded the same as `Get` commands, except with the tag `r`.
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

const RM_BYTE: u8 = b'r';
const GET_BYTE: u8 = b'g';

impl<'a> Cmd<'a> {
    /// Parses a `Cmd` out of the provided byte buffer.
    pub fn parse(cmd: &'a [u8]) -> Result<Self> {
        debug!(?cmd, "Received command");
        match cmd.first().copied() {
            None => Err(Error::msg("Invalid empty command")),
            Some(b @ GET_BYTE | b @ RM_BYTE) => {
                let mut parts = cmd[1..].splitn(2, |b| *b == b',');

                let key_part = parts.next().ok_or_else(|| Error::msg("Missing key len"))?;
                let key_len = parse_ascii_len(key_part)?;

                let rest = parts.next().ok_or_else(|| Error::msg("Missing data"))?;
                let key = rest
                    .get(..key_len)
                    .ok_or_else(|| Error::msg("Not enough data for specified data lengths"))?;

                let key =
                    std::str::from_utf8(key).map_err(|e| Error::new(e).context("Invalid key"))?;

                if b == GET_BYTE {
                    Ok(Self::Get(Cow::Borrowed(key)))
                } else {
                    Ok(Self::Rm(Cow::Borrowed(key)))
                }
            }
            _ => {
                let mut parts = cmd.splitn(3, |b| *b == b',');

                let key_part = parts.next().ok_or_else(|| Error::msg("Missing key len"))?;
                let key_len = parse_ascii_len(key_part)?;

                let value_part = parts
                    .next()
                    .ok_or_else(|| Error::msg("Missing value len"))?;
                let value_len = parse_ascii_len(value_part)?;

                let rest = parts.next().ok_or_else(|| Error::msg("Missing data"))?;
                let rest = rest
                    .get(..key_len + value_len)
                    .ok_or_else(|| Error::msg("Not enough data for specified data lengths"))?;

                let (key, value) = rest.split_at(key_len);
                let key =
                    std::str::from_utf8(key).map_err(|e| Error::new(e).context("Invalid key"))?;
                let value = std::str::from_utf8(value)
                    .map_err(|e| Error::new(e).context("Invalid value"))?;

                Ok(Self::Set(Cow::Borrowed(key), Cow::Borrowed(value)))
            }
        }
    }

    /// Writes the `Cmd` into the provided writer.
    pub fn write<W: Write>(&self, w: &mut W) -> Result<()> {
        match self {
            Self::Set(key, value) => write!(w, "{},{},{key}{value}", key.len(), value.len())?,
            Self::Get(key) => {
                // TODO These are a bit unwieldy ... could they be improved?
                w.write_all(&[GET_BYTE])?;
                write!(w, "{},{key}", key.len())?;
            }
            Self::Rm(key) => {
                w.write_all(&[RM_BYTE])?;
                write!(w, "{},{key}", key.len())?;
            }
        }
        Ok(())
    }
}

fn parse_ascii_len(len: &[u8]) -> Result<usize> {
    if len.len() >= 10 {
        return Err(Error::msg("Too much data")); // ?
    }
    let mut l = 0;
    let mut base = 1;
    for &b in len.iter().rev() {
        l += base * (b - b'0') as usize;
        base *= 10;
    }
    Ok(l)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set() {
        let actual = Cmd::parse(b"3,6,foofoobar").unwrap();
        let expected = Cmd::Set("foo".into(), "foobar".into());
        assert_eq!(actual, expected);
    }

    #[test]
    fn rm() {
        let actual = Cmd::parse(b"r3,foo").unwrap();
        let expected = Cmd::Rm("foo".into());
        assert_eq!(actual, expected);
    }

    #[test]
    fn get() {
        let actual = Cmd::parse(b"g3,foo").unwrap();
        let expected = Cmd::Get("foo".into());
        assert_eq!(actual, expected);
    }

    #[test]
    fn set_ignore_extra() {
        let actual = Cmd::parse(b"3,6,foofoobar_ignoreme").unwrap();
        let expected = Cmd::Set("foo".into(), "foobar".into());
        assert_eq!(actual, expected);
    }

    #[test]
    fn rm_ignore_extra() {
        let actual = Cmd::parse(b"r3,foo_ignoreme").unwrap();
        let expected = Cmd::Rm("foo".into());
        assert_eq!(actual, expected);
    }

    #[test]
    fn get_ignore_extra() {
        let actual = Cmd::parse(b"g3,foo_ignoreme").unwrap();
        let expected = Cmd::Get("foo".into());
        assert_eq!(actual, expected);
    }

    #[test]
    fn set_identity() {
        let key = "abc";
        let value = "defg";
        let proto = Cmd::Set(Cow::Borrowed(key), Cow::Borrowed(value));

        let mut buf = vec![];

        proto.write(&mut buf).unwrap();

        assert_eq!(Cmd::parse(&buf).unwrap(), proto);
    }

    #[test]
    fn get_identity() {
        let key = "abc";
        let proto = Cmd::Get(Cow::Borrowed(key));

        let mut buf = vec![];

        proto.write(&mut buf).unwrap();

        assert_eq!(Cmd::parse(&buf).unwrap(), proto);
    }

    #[test]
    fn rm_identity() {
        let key = "abc";
        let proto = Cmd::Rm(Cow::Borrowed(key));

        let mut buf = vec![];

        proto.write(&mut buf).unwrap();

        assert_eq!(Cmd::parse(&buf).unwrap(), proto);
    }

    #[test]
    fn len_doesnt_match() {
        assert!(Cmd::parse(b"5,5,foolsbars").is_err());
        assert!(Cmd::parse(b"g5,foo").is_err());
        assert!(Cmd::parse(b"r5,foo").is_err());
    }

    #[test]
    fn missing_commas() {
        assert!(Cmd::parse(b"55,foolsbars").is_err());
        assert!(Cmd::parse(b"5,5foolsbars").is_err());
        assert!(Cmd::parse(b"g5foo").is_err());
        assert!(Cmd::parse(b"r5foo").is_err());
    }

    #[test]
    fn excessive_data() {
        assert!(Cmd::parse(b"g1000000000,lolnoway").is_err());
    }

    #[test]
    fn empty_command() {
        assert!(Cmd::parse(b"").is_err());
    }
}
