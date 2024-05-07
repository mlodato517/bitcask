//! Ways of communicating with Bitcask databases.
//!
//! The two main mechanisms for this are [`Cmd`], for issuing requests to the database, and
//! [`Response`], for consuming responses from the database.

mod cmd;
mod response;

pub use cmd::{Cmd, CmdReader, Reader};
pub use response::Response;
