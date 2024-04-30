//! Server to receive requests at an address and handle them with a specified [`Engine`].

use std::io::Write;
use std::net::{SocketAddr, TcpListener};

use anyhow::{Context, Result};
use kvs::KvsEngine;
use protocol::{Cmd, Reader, Response};
use tracing::{debug, info, warn};

use crate::Engine;

pub struct Server {
    addr: SocketAddr,
    engine: Engine,
}

impl Server {
    pub fn new(engine: Engine, addr: SocketAddr) -> Self {
        Self { addr, engine }
    }

    pub fn run(mut self) -> Result<()> {
        debug!(?self.addr, "Binding server");
        let listener = TcpListener::bind(self.addr).context("Failed to bind address")?;
        debug!("Server bound");

        let mut reader = Reader::new();

        for stream in listener.incoming() {
            let mut stream = stream?;
            info!(?stream, "Received connection");

            let cmd = match reader.read_cmd(&mut stream) {
                Ok(Some(cmd)) => cmd,
                Ok(None) => {
                    warn!("Response had no data");
                    stream.write_all(b"Response had no data")?;
                    stream.flush()?;
                    continue;
                }
                Err(e) => {
                    warn!(?e, "Failed to parse command");
                    let e = e.to_string();
                    stream.write_all(e.as_bytes())?;
                    stream.flush()?;
                    continue;
                }
            };

            info!(?cmd, "Parsed command");
            let response = match cmd.into_cmd() {
                Cmd::Set(k, v) => Self::handle_set(&mut self.engine, k.to_string(), &v),
                Cmd::Get(k) => Self::handle_get(&mut self.engine, &k),
                Cmd::Rm(k) => Self::handle_rm(&mut self.engine, &k),
            };
            response.write(&mut stream)?;
            stream.flush()?;
        }

        Ok(())
    }

    /// Executes a set command on the passed KvsEngine, returning a response.
    fn handle_set(kvs: &mut impl KvsEngine, key: String, value: &str) -> Response<'static> {
        match kvs.set(key, value) {
            Ok(_) => Response::SuccessfulSet,
            Err(e) => {
                warn!(?e, "Failed to set key to value");
                // TODO These .to_string()s are kind of sad. We should be able to write these
                // bytes directly into the stream. Maybe these should be static methods like
                // `response::write_err(impl Display)` or something?
                Response::Err(e.to_string().into())
            }
        }
    }

    /// Executes a get command on the passed KvsEngine, returning a response.
    fn handle_get(kvs: &mut impl KvsEngine, key: &str) -> Response<'static> {
        match kvs.get(key) {
            Ok(Some(val)) => Response::SuccessfulGet(val.into()),
            Ok(None) => Response::KeyNotFound,
            Err(e) => {
                warn!(?e, "Failed to get key");
                Response::Err(e.to_string().into())
            }
        }
    }

    /// Executes a remove command on the passed KvsEngine, returning a response.
    fn handle_rm(kvs: &mut impl KvsEngine, key: &str) -> Response<'static> {
        match kvs.remove(key) {
            Ok(_) => Response::SuccessfulRm,
            Err(e) => {
                warn!(?e, "Failed to remove key");
                Response::Err(e.to_string().into())
            }
        }
    }
}
