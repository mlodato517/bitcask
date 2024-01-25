use std::borrow::Cow;
use std::io::{Read, Write};

use tracing::debug;
// TODO More specific crate error
use anyhow::{Context, Error, Result};

// Sentinal value lengths to differentiate non-Set commands
const GET_LEN: u64 = u64::MAX;
const RM_LEN: u64 = u64::MAX - 1;

/// Representation of a user command that interacts with the store in any way, whether that
/// modifies data, or just queries it.
#[derive(Clone, Debug, PartialEq)]
pub enum Cmd<'a> {
    Set(Cow<'a, str>, Cow<'a, str>),
    Get(Cow<'a, str>),
    Rm(Cow<'a, str>),
}

impl<'a> Cmd<'a> {
    pub fn read<R: Read>(mut reader: R, result: &'a mut Vec<u8>) -> Result<Self> {
        let header_len = 12;
        if result.len() < header_len {
            result.resize(header_len, 0);
        }
        reader.read_exact(&mut result[..header_len])?;

        let key_len = &result[0..4];
        let val_len = &result[4..12];

        let key_len = u32::from_be_bytes(key_len.try_into().expect("sliced 0..4"));
        let val_len = u64::from_be_bytes(val_len.try_into().expect("sliced 4..12"));

        let payload_len = match val_len {
            GET_LEN | RM_LEN => key_len,
            val_len => key_len as usize + val_len as usize,
        };

        // Make sure we have enough to read the whole thing.
        if result.len() < payload_len + header_len {
            result.resize(payload_len + header_len, 0);
        }

        reader
            .read_exact(&mut result[header_len..header_len + payload_len])
            .context("Failed to read data")?;

        Self::parse(result)
    }

    /// # Panics
    ///
    /// Panics if cmd is too short. This could mean it's empty, or it doesn't have correct key/len
    /// fields.
    ///
    /// # TODO
    ///
    /// Handle errors more gracefully
    pub fn parse(cmd: &'a [u8]) -> Result<Self> {
        debug!(?cmd, "Received command");

        let key_len = &cmd[0..4];
        let val_len = &cmd[4..12];

        let key_len = u32::from_be_bytes(key_len.try_into().expect("split at 4"));
        let val_len = u64::from_be_bytes(val_len.try_into().expect("split at 8"));

        let (key, val) = cmd[12..].split_at(key_len as usize);

        match val_len {
            GET_LEN => {
                let key =
                    std::str::from_utf8(key).map_err(|e| Error::new(e).context("Invalid key"))?;
                Ok(Self::Get(Cow::Borrowed(key)))
            }
            RM_LEN => {
                let key =
                    std::str::from_utf8(key).map_err(|e| Error::new(e).context("Invalid key"))?;
                Ok(Self::Rm(Cow::Borrowed(key)))
            }
            val_len => {
                let key =
                    std::str::from_utf8(key).map_err(|e| Error::new(e).context("Invalid key"))?;

                let val = &val[..val_len as usize];
                let val =
                    std::str::from_utf8(val).map_err(|e| Error::new(e).context("Invalid val"))?;

                Ok(Self::Set(Cow::Borrowed(key), Cow::Borrowed(val)))
            }
        }
    }

    /// Writes the current `Cmd` into the passed writer, returning the bytes read.
    pub fn write<W: Write>(&self, w: &mut W) -> Result<usize> {
        match self {
            Self::Set(key, val) => {
                let key_len = (key.len() as u32).to_be_bytes();
                let val_len = (val.len() as u64).to_be_bytes();
                let header = [
                    key_len[0], key_len[1], key_len[2], key_len[3], val_len[0], val_len[1],
                    val_len[2], val_len[3], val_len[4], val_len[5], val_len[6], val_len[7],
                ];

                w.write_all(&header)?;
                w.write_all(key.as_bytes())?;
                w.write_all(val.as_bytes())?;

                Ok(header.len() + key.len() + val.len())
            }
            Self::Get(key) => {
                let key_len = (key.len() as u32).to_be_bytes();
                let val_len = GET_LEN.to_be_bytes();
                let header = [
                    key_len[0], key_len[1], key_len[2], key_len[3], val_len[0], val_len[1],
                    val_len[2], val_len[3], val_len[4], val_len[5], val_len[6], val_len[7],
                ];

                w.write_all(&header)?;
                w.write_all(key.as_bytes())?;

                Ok(header.len() + key.len())
            }
            // TODO Could model "rm" as a "set" whose value is a single null byte. Then we only
            // use the tag to identify a Get, which might save bytes overall :thinking:
            Self::Rm(key) => {
                let key_len = (key.len() as u32).to_be_bytes();
                let val_len = RM_LEN.to_be_bytes();
                let header = [
                    key_len[0], key_len[1], key_len[2], key_len[3], val_len[0], val_len[1],
                    val_len[2], val_len[3], val_len[4], val_len[5], val_len[6], val_len[7],
                ];

                w.write_all(&header)?;
                w.write_all(key.as_bytes())?;

                Ok(header.len() + key.len())
            }
        }
    }

    /// Writes the current `Cmd` into the passed writer, with a trailing newline, returning the
    /// bytes read.
    pub fn writeln<W: Write>(&self, w: &mut W) -> Result<usize> {
        let data_len = self.write(w)?;

        // TODO Maybe handle carriage returns? `std::writeln!` doesn't care :shrug:
        w.write_all(b"\n")?;

        Ok(data_len + 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
