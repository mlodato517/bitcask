use std::borrow::Cow;
use std::io::{Read, Write};

use kvs::{Error, Result};
use tracing::debug;

pub enum Response<'a> {
    Ok(&'a str),
    KeyNotFound,
    Err(&'a str),
}

impl<'a> Response<'a> {
    pub fn read<R: Read>(server: &mut R, result: &'a mut Vec<u8>) -> Result<Self> {
        result.clear();
        // TODO Incremental read in case someone just sends us a massive blob of garbage.
        // Can just take the first 25 bytes or something to check lengths and commas and whatever.
        match server.read_to_end(result) {
            Ok(len) => len,
            Err(e) => return Err(Error::new(e).context("Failed to read data")),
        };
        let result = std::str::from_utf8(result);
        match result {
            Ok(response) => match &response[0..1] {
                "s" => Ok(Self::Ok(&response[1..])),
                "n" => Ok(Self::KeyNotFound),
                "e" => Ok(Self::Err(&response[1..])),
                _ => Ok(Self::Err("Invalid start byte from server")),
            },
            Err(_) => Err(Error::msg("Invalid utf8 from server")),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum Cmd<'a> {
    Set(Cow<'a, str>, Cow<'a, str>),
    Get(Cow<'a, str>),
    Rm(Cow<'a, str>),
}

impl<'a> Cmd<'a> {
    pub fn parse(cmd: &'a [u8]) -> Result<Self> {
        debug!(?cmd, "Received command");
        match cmd[0] {
            b @ b'g' | b @ b'r' => {
                let mut parts = cmd[1..].splitn(2, |b| *b == b',');

                let key_part = parts.next().ok_or_else(|| Error::msg("Missing key len"))?;
                let key_len = parse_ascii_len(key_part)?;

                let rest = parts.next().ok_or_else(|| Error::msg("Missing data"))?;
                let key = rest
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

    pub fn write<W: Write>(&self, w: &mut W) -> Result<()> {
        match self {
            Self::Set(key, value) => Ok(write!(w, "{},{},{key}{value}", key.len(), value.len())?),
            Self::Get(key) => Ok(write!(w, "g{},{key}", key.len())?),
            Self::Rm(key) => Ok(write!(w, "r{},{key}", key.len())?),
        }
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
}
