use kvs_server::response::Response;
use rand::distributions::{Alphanumeric, DistString};
use rand::prelude::*;
use tempfile::TempDir;

use kvs_server::KvsServer;
use protocol::Cmd;

fn main() {
    let mut rng = StdRng::seed_from_u64(4);
    let key_values: Vec<_> = (0..100)
        .map(|_| {
            let len = rng.gen_range(1..=100_000);
            let key = Alphanumeric.sample_string(&mut rng, len as usize);
            let value = Alphanumeric.sample_string(&mut rng, len as usize);
            (key, value)
        })
        .collect();

    let mut shuffled = key_values.clone();
    shuffled.shuffle(&mut rng);

    // Must declare dir in here, otherwise `TempDir` will go out of scope and be deleted.
    let dir = TempDir::new().unwrap();
    let mut kvs = KvsServer::open(Some("kvs"), dir.path()).unwrap();

    for _ in 0..1000 {
        for (key, value) in &key_values {
            let cmd = Cmd::Set(key.into(), value.into());
            match kvs.handle_cmd(cmd) {
                Response::Ok => {}
                _ => panic!("Failed to set value!"),
            }
        }
    }

    for _ in 0..1000 {
        for (key, value) in &shuffled {
            let cmd = Cmd::Get(key.into());
            match kvs.handle_cmd(cmd) {
                Response::OkGet(found) => assert_eq!(&found, value),
                _ => panic!("Did not find value!"),
            }
        }
    }
}
