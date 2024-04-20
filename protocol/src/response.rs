//! Responses to a [`Cmd`][crate::Cmd].

use std::borrow::Cow;
use std::io::Write;

use anyhow::Result;

// Implementation details:
//
// The current protocol is:
//   1. Successful `Set` responses are encoded as a single `s`
//   2. Successful `Rm` responses are encoded as a single `r`
//   3. Successful `Get` responses are encoded as an `g` followed by the value for the key
//   4. Unsuccessful `Get` responses are encoded as an `n` (for "not found")
//   5. Errors are encoded as an `e` followed by the error
//
// TODO Can we make these comments unnecessary with a descriptive trait?
const SUCCESSFUL_SET_BYTE: u8 = b's';
const SUCCESSFUL_RM_BYTE: u8 = b'r';
const SUCCESSFUL_GET_BYTE: u8 = b'g';
const NOT_FOUND_BYTE: u8 = b'n';
const ERROR_BYTE: u8 = b'e';

/// A response to a [`Cmd`][crate::Cmd].
#[derive(Debug, PartialEq)]
pub enum Response<'a> {
    /// The Set command was successful. Think of this like HTTP status code 201.
    SuccessfulSet,
    /// The Rm command was successful. Think of this like HTTP status code 204.
    SuccessfulRm,
    /// The value for the requested key. Think of this like HTTP status code 200.
    SuccessfulGet(Cow<'a, str>),
    /// The Get command was requested for an unknown key. Think of this like HTTP status code 404.
    KeyNotFound,
    /// An error occurred while processing the command. Think of this like HTTP status code 500.
    Err(Cow<'a, str>),
}

impl<'a> Response<'a> {
    /// Parses a `Response` from the given bytes.
    pub fn from_bytes(bytes: &'a [u8]) -> Self {
        match std::str::from_utf8(bytes) {
            Ok(response) => match bytes.first().copied() {
                Some(SUCCESSFUL_SET_BYTE) => Self::SuccessfulSet,
                Some(SUCCESSFUL_RM_BYTE) => Self::SuccessfulRm,
                Some(SUCCESSFUL_GET_BYTE) => Self::SuccessfulGet(response[1..].into()),
                Some(NOT_FOUND_BYTE) => Self::KeyNotFound,
                Some(ERROR_BYTE) => Self::Err(response[1..].into()),
                Some(_) | None => Self::Err("Invalid start byte".into()),
            },
            Err(_) => Self::Err("Invalid utf8".into()),
        }
    }

    /// Writes the `Response` into a writer.
    pub fn write<W: Write>(&self, mut writer: W) -> Result<()> {
        match self {
            Self::SuccessfulSet => writer.write_all(&[SUCCESSFUL_SET_BYTE])?,
            Self::SuccessfulRm => writer.write_all(&[SUCCESSFUL_RM_BYTE])?,
            Self::SuccessfulGet(val) => {
                // TODO Can these be combined into a single call?
                writer.write_all(&[SUCCESSFUL_GET_BYTE])?;
                writer.write_all(val.as_bytes())?;
            }
            Self::KeyNotFound => writer.write_all(&[NOT_FOUND_BYTE])?,
            Self::Err(e) => {
                writer.write_all(&[ERROR_BYTE])?;
                writer.write_all(e.as_bytes())?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn communicates_set() {
        let mut buf = Vec::new();
        let expected = Response::SuccessfulSet;
        expected.write(&mut buf).unwrap();
        let actual = Response::from_bytes(&buf);

        assert_eq!(actual, expected);
    }

    #[test]
    fn communicates_rm() {
        let mut buf = Vec::new();
        let expected = Response::SuccessfulRm;
        expected.write(&mut buf).unwrap();
        let actual = Response::from_bytes(&buf);

        assert_eq!(actual, expected);
    }

    #[test]
    fn communicates_get() {
        let mut buf = Vec::new();
        let expected = Response::SuccessfulGet("foo".into());
        expected.write(&mut buf).unwrap();
        let actual = Response::from_bytes(&buf);

        assert_eq!(actual, expected);
    }

    #[test]
    fn communicates_empty_get() {
        let mut buf = Vec::new();
        let expected = Response::SuccessfulGet("".into());
        expected.write(&mut buf).unwrap();
        let actual = Response::from_bytes(&buf);

        assert_eq!(actual, expected);
    }

    #[test]
    fn communicates_not_found() {
        let mut buf = Vec::new();
        let expected = Response::KeyNotFound;
        expected.write(&mut buf).unwrap();
        let actual = Response::from_bytes(&buf);

        assert_eq!(actual, expected);
    }

    #[test]
    fn communicates_error() {
        let mut buf = Vec::new();
        let expected = Response::Err("some error".into());
        expected.write(&mut buf).unwrap();
        let actual = Response::from_bytes(&buf);

        assert_eq!(actual, expected);
    }

    mod from_bytes_tests {
        use super::*;

        #[test]
        fn handles_non_utf8() {
            let bytes = [255];
            let actual = Response::from_bytes(&bytes);
            let expected = Response::Err("Invalid utf8".into());

            assert_eq!(actual, expected);
        }

        #[test]
        fn handles_invalid_payload() {
            let bytes = b"blahblah";
            let actual = Response::from_bytes(bytes);
            let expected = Response::Err("Invalid start byte".into());

            assert_eq!(actual, expected);
        }

        #[test]
        fn handles_empty_payload() {
            let bytes = b"";
            let actual = Response::from_bytes(bytes);
            let expected = Response::Err("Invalid start byte".into());

            assert_eq!(actual, expected);
        }
    }
}
