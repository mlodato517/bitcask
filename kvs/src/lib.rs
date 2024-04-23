//! I'm not 100% sure what goes here. Either a KV Store or utilities for building one or something.
//! I'll learn more as we go, I'm sure.
//!
//! Built following https://github.com/pingcap/talent-plan/blob/master/courses/rust/README.md.

mod compaction_policy;
mod engine;
mod error;
mod file_util;
mod kv_store;

pub use engine::KvsEngine;
pub use error::{Error, Result};
pub use kv_store::KvStore;
