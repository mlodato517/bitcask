//! I'm not 100% sure what goes here. Either a KV Store or utilities for building one or something.
//! I'll learn more as we go, I'm sure.
//!
//! Built following https://github.com/pingcap/talent-plan/blob/master/courses/rust/README.md.

mod client;
mod command;
mod compaction_policy;
mod engine;
mod file_util;
mod kv_store;
mod protocol;

pub use client::KvsClient;
pub(crate) use command::Command;
pub use engine::KvsEngine;
pub use kv_store::{Error, KvStore, Result};
pub use protocol::{Cmd, Response};
