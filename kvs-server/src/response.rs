use std::io::{Read, Write};

use anyhow::{Error, Result};

pub enum Response {
    Ok,
    OkGet(String),
    KeyNotFound,
    Err(Error),
}

impl Response {
    pub fn read<R: Read>(mut server: R, result: &mut Vec<u8>) -> Result<Self> {
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
                "s" if response.len() == 1 => Ok(Self::Ok),
                "s" => Ok(Self::OkGet(response[1..].to_string())),
                "n" => Ok(Self::KeyNotFound),
                "e" => Ok(Self::Err(Error::msg(response[1..].to_string()))),
                _ => Ok(Self::Err(Error::msg("Invalid start byte from server"))),
            },
            Err(_) => Err(Error::msg("Invalid utf8 from server")),
        }
    }

    pub fn write<W: Write>(self, mut writer: W) -> Result<()> {
        match self {
            Self::Ok => writer.write_all(b"s")?,
            Self::OkGet(val) => write!(writer, "s{val}")?,
            Self::KeyNotFound => writer.write_all(b"n")?,
            Self::Err(e) => write!(writer, "e{e}")?,
        }
        Ok(())
    }
}
