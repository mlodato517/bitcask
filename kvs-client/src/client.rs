//! A client for making network requests to a remote `KvsServer`.

use std::borrow::Cow;
use std::io::Read;
use std::net::{Shutdown, SocketAddr, TcpStream};

use anyhow::{anyhow, Context, Result};
use protocol::{Cmd, Response};
use tracing::{debug, info};

/// A client for making network requests to a remote `KvsServer`.
pub struct Client {
    /// Address of remote `KvsServer`.
    addr: SocketAddr,

    /// Buffer to read responses into.
    response_buf: Vec<u8>,
}

impl Client {
    /// Creates a new `Client` that will connect to the provided address.
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            response_buf: Vec::new(),
        }
    }

    /// Issues a set command for the key and value to the remote server. Returns `Ok(())` if it
    /// succeeded and an `Err` otherwise.
    pub fn set(&mut self, key: &str, value: &str) -> Result<()> {
        let cmd = Cmd::Set(key.into(), value.into());
        match self.write_cmd(cmd) {
            Ok(Response::SuccessfulSet) => Ok(()),
            other_response => Err(anyhow!("Unexpected set response {other_response:?}")),
        }
    }

    /// Issues a get command for the key to the remote server. Returns `Ok(Some)` if the command
    /// found a value, `Ok(None)` if the key wasn't present, and an `Err` otherwise.
    pub fn get(&mut self, key: &str) -> Result<Option<Cow<'_, str>>> {
        let cmd = Cmd::Get(key.into());
        match self.write_cmd(cmd) {
            Ok(Response::SuccessfulGet(value)) => Ok(Some(value)),
            Ok(Response::KeyNotFound) => Ok(None),
            other_response => Err(anyhow!("Unexpected get response {other_response:?}")),
        }
    }

    /// Issues an rm command for the key to the remote server. Returns `Ok(())` if the command
    /// succeeded and an `Err` otherwise.
    pub fn rm(&mut self, key: &str) -> Result<()> {
        let cmd = Cmd::Rm(key.into());
        match self.write_cmd(cmd) {
            Ok(Response::SuccessfulRm) => Ok(()),
            other_response => Err(anyhow!("Unexpected rm response {other_response:?}")),
        }
    }

    /// Writes a command to the remote server and reads the response.
    fn write_cmd(&mut self, cmd: Cmd) -> Result<Response> {
        debug!(addr = ?self.addr, "Connecting to server");
        let mut connection = TcpStream::connect(self.addr).context("Connecting to server")?;

        // TODO set timeout?
        info!(?cmd, "Writing to server");
        cmd.write(&mut connection)?;

        // NB We don't need to `.flush()` because, for `TcpStream`, that's a no-op
        debug!("Wrote to server. Shutting down write half.");
        connection.shutdown(Shutdown::Write)?;
        debug!("Shut down write half");

        // TODO set_read_timeout?
        debug!("Reading from server");
        connection
            .read_to_end(&mut self.response_buf)
            .context("Reading response")?;

        Ok(Response::from_bytes(&self.response_buf))
    }
}
