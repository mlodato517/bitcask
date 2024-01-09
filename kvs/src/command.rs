use std::borrow::Cow;

use protocol::Cmd;

use crate::Error;

/// Representation of a user command that modifies data and is persisted in some way.
#[derive(Clone)] // TODO Do we need this?
pub(crate) enum Command<'a> {
    Set(Cow<'a, str>, Cow<'a, str>),
    Rm(Cow<'a, str>),
}

impl<'a> From<Command<'a>> for Cmd<'a> {
    fn from(command: Command<'a>) -> Self {
        match command {
            Command::Set(key, val) => Self::Set(key, val),
            Command::Rm(key) => Self::Rm(key),
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
