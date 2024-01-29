use std::borrow::Cow;

use protocol::Cmd;

use crate::Error;

/// Representation of a user command that modifies data and is persisted in some way.
#[derive(Debug)]
pub(crate) enum Command<'a> {
    Set(Cow<'a, str>, Cow<'a, str>),
    Rm(Cow<'a, str>),
}

impl Command<'_> {
    pub fn as_cmd(&self) -> Cmd {
        match self {
            Command::Set(key, val) => Cmd::Set(Cow::Borrowed(key), Cow::Borrowed(val)),
            Command::Rm(key) => Cmd::Rm(Cow::Borrowed(key)),
        }
    }
}

impl<'a> TryFrom<Cmd<'a>> for Command<'a> {
    type Error = Error;

    fn try_from(command: Cmd<'a>) -> Result<Self, Self::Error> {
        match command {
            Cmd::Set(key, val) => Ok(Self::Set(key, val)),
            Cmd::Rm(key) => Ok(Self::Rm(key)),
            Cmd::Get(_) => Err(Error::msg("Gets aren't persisted")),
        }
    }
}
