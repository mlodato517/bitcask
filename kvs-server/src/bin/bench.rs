use kvs_server::response::Response;
use rand::distributions::{Alphanumeric, DistString};
use rand::prelude::*;
use tempfile::TempDir;

use kvs_server::KvsServer;
use protocol::Cmd;

fn main() {
    let mut rng = StdRng::seed_from_u64(4);
    let sets: Vec<_> = (0..100)
        .map(|_| {
            let len = rng.gen_range(1..=100_000);
            let key = Alphanumeric.sample_string(&mut rng, len as usize);
            let value = Alphanumeric.sample_string(&mut rng, len as usize);
            Cmd::Set(key.into(), value.into())
        })
        .collect();
    let mut gets: Vec<_> = sets
        .iter()
        .map(|cmd| match cmd {
            Cmd::Set(k, _) => Cmd::Get(k.clone()),
            _ => unreachable!(),
        })
        .collect();
    gets.shuffle(&mut rng);

    // Must declare dir in here, otherwise `TempDir` will go out of scope and be deleted.
    let dir = TempDir::new().unwrap();
    let mut kvs = KvsServer::open(Some("kvs"), dir.path()).unwrap();

    for cmd in &sets {
        if let Response::Err(e) = kvs.handle_cmd(cmd.clone()) {
            panic!("Unexpected error: {e:?}")
        }
    }

    for _ in 0..1000 {
        for get in &gets {
            if let Response::Err(e) = kvs.handle_cmd(get.clone()) {
                panic!("Unexpected error: {e:?}")
            }
        }
    }
}
