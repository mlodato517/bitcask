use std::borrow::Cow;
use std::io::{Read, Write};

use tracing::debug;
// TODO More specific crate error
use anyhow::{Context, Error, Result};

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
        // We want to make sure we can at least read a tag, key length, and value length. So we
        // need at least 9 bytes. But `read_to_end` uses 32, so let's do that to reduce resizes.
        if result.len() < 32 {
            result.resize(32, 0);
        }

        let mut total_read = 0;
        loop {
            let read = reader
                .read(&mut result[total_read..])
                .context("Failed to read data")?;
            total_read += read;

            // Done reading or the buffer is full. Since the buffer has at least 32 bytes in it, we
            // should have at least enough to determine the key/value lengths.
            if read == 0 {
                break;
            }

            match total_read {
                // The spec for `read` says this is either the reader hitting EOF or the buffer
                // being empty. We know the buffer isn't empty, since we resized it at the
                // beginning. So we hit EOF without reading any bytes ... we could spin here in
                // case data gets added, but this is more likely an error.
                0 => return Err(Error::msg("Received unexpected, early EOF")),

                // We have a few bytes, but we're gonna need more
                // TODO Encapsulate these required lengths better.
                1..=3 => continue,
                4..=8 => {
                    // We have a few bytes. If this is a set command, we need more, but we have
                    // enough for get/remove.
                    if result[0] == b's' {
                        continue;
                    } else {
                        break;
                    }
                }
                // This is enough for any command
                9.. => break,
            }
        }

        debug!(?result, "Buffer before length read");

        // At this point, `result` should have enough data in it for us to read the lengths we need
        let (len, offset) = Self::read_len(result)?;
        let len = len as usize;

        if len + offset > total_read {
            // Make sure we have enough to read the whole thing.
            if result.capacity() < len + offset {
                result.resize(len + offset, 0);
            }

            reader
                .read_exact(&mut result[total_read..len + offset])
                .context("Failed to read data")?;
        }

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
        let tag = cmd[0];
        let cmd = &cmd[1..];
        match tag {
            b @ b'g' | b @ b'r' => {
                let key_len_bytes = [cmd[0], cmd[1], cmd[2], cmd[3]];
                let cmd = &cmd[4..];

                // TODO Big or Little endian?
                let key_len = u32::from_be_bytes(key_len_bytes) as usize;

                let key = cmd
                    .get(..key_len)
                    .ok_or_else(|| Error::msg("Not enough data for specified data lengths"))?;

                let key =
                    std::str::from_utf8(key).map_err(|e| Error::new(e).context("Invalid key"))?;

                if b == b'g' {
                    Ok(Self::Get(Cow::Borrowed(key)))
                } else {
                    Ok(Self::Rm(Cow::Borrowed(key)))
                }
            }
            b's' => {
                let key_len_bytes = [cmd[0], cmd[1], cmd[2], cmd[3]];
                let val_len_bytes = [cmd[4], cmd[5], cmd[6], cmd[7]];
                let cmd = &cmd[8..];

                // TODO Big or Little endian?
                let key_len = u32::from_be_bytes(key_len_bytes) as usize;
                let val_len = u32::from_be_bytes(val_len_bytes) as usize;

                let key = cmd
                    .get(..key_len)
                    .ok_or_else(|| Error::msg("Not enough data for specified key"))?;
                let cmd = &cmd[key_len..];

                let val = cmd
                    .get(..val_len)
                    .ok_or_else(|| Error::msg("Not enough data for specified value"))?;

                let key =
                    std::str::from_utf8(key).map_err(|e| Error::new(e).context("Invalid key"))?;
                let val =
                    std::str::from_utf8(val).map_err(|e| Error::new(e).context("Invalid value"))?;

                Ok(Self::Set(Cow::Borrowed(key), Cow::Borrowed(val)))
            }
            _ => Err(Error::msg("Invalid command tag")),
        }
    }

    /// Writes the current `Cmd` into the passed writer, returning the bytes read.
    pub fn write<W: Write>(&self, w: &mut W) -> Result<usize> {
        match self {
            Self::Set(key, val) => {
                w.write_all(b"s")?;
                w.write_all(&(key.len() as u32).to_be_bytes())?;
                w.write_all(&(val.len() as u32).to_be_bytes())?;
                w.write_all(key.as_bytes())?;
                w.write_all(val.as_bytes())?;

                Ok(1 + 4 + 4 + key.len() + val.len())
            }
            Self::Get(key) => {
                w.write_all(b"g")?;
                w.write_all(&(key.len() as u32).to_be_bytes())?;
                w.write_all(key.as_bytes())?;

                Ok(1 + 4 + key.len())
            }
            // TODO Could model "rm" as a "set" whose value is a single null byte. Then we only
            // use the tag to identify a Get, which might save bytes overall :thinking:
            Self::Rm(key) => {
                w.write_all(b"r")?;
                w.write_all(&(key.len() as u32).to_be_bytes())?;
                w.write_all(key.as_bytes())?;

                Ok(1 + 4 + key.len())
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

    /// Given a buffer, determine how much data should be read, starting at what offset.
    fn read_len(cmd: &[u8]) -> Result<(u32, usize)> {
        let tag = cmd[0];
        let cmd = &cmd[1..];
        match tag {
            b'g' | b'r' => {
                // TODO Big or Little endian?
                let key_len_bytes = [cmd[0], cmd[1], cmd[2], cmd[3]];
                let key_len = u32::from_be_bytes(key_len_bytes);

                // Read key, skipping the tag and the key_len prefix
                Ok((key_len, 5))
            }
            b's' => {
                let key_len_bytes = [cmd[0], cmd[1], cmd[2], cmd[3]];
                let val_len_bytes = [cmd[4], cmd[5], cmd[6], cmd[7]];

                // TODO Big or Little endian?
                let key_len = u32::from_be_bytes(key_len_bytes);
                let val_len = u32::from_be_bytes(val_len_bytes);

                // Read key and value, skipping the tag and the key_len and val_len prefixes
                Ok((key_len + val_len, 9))
            }
            _ => Err(Error::msg("Invalid command tag")),
        }
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

    #[test]
    fn insufficient_data() {
        // key_len = 3, but data is 2 bytes
        let too_short_for_key = [b's', 0, 0, 0, 3, 0, 0, 0, 4, b'k', b'e'];
        assert!(Cmd::parse(&too_short_for_key).is_err());

        // key_len + val_len = 3 + 4, but data is 6 bytes
        let too_short_for_val = [
            b's', 0, 0, 0, 3, 0, 0, 0, 4, b'k', b'e', b'y', b'v', b'a', b'l',
        ];
        assert!(Cmd::parse(&too_short_for_val).is_err());

        // key_len = 3, but data is 2 bytes
        let too_short_for_key = [b'g', 0, 0, 0, 3, b'k', b'e'];
        assert!(Cmd::parse(&too_short_for_key).is_err());

        // key_len = 3, but data is 2 bytes
        let too_short_for_key = [b'r', 0, 0, 0, 3, b'k', b'e'];
        assert!(Cmd::parse(&too_short_for_key).is_err());
    }

    #[test]
    fn missing_tag() {
        let no_tag = [0, 0, 0, 3, b'k', b'e', b'y'];
        assert!(Cmd::parse(&no_tag).is_err());
    }
}
