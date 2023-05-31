use std::borrow::Cow;

use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
#[serde(untagged)]
pub(crate) enum Command<'a, 'b, 'c> {
    Set(Cow<'a, str>, Cow<'b, str>),
    Rm(Cow<'c, str>),
}
