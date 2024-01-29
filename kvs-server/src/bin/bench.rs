use kvs_server::response::Response;
use rand::distributions::{Alphanumeric, DistString};
use rand::prelude::*;
use tempfile::TempDir;

use kvs::NeverPolicy;
use kvs_server::KvsServer;
use protocol::Cmd;

fn main() {
    let key_values = generate_shuffled_key_values();
    bench_writes(&key_values);
    bench_reads(&key_values);
}

fn get_rng() -> StdRng {
    StdRng::seed_from_u64(4)
}

fn generate_shuffled_key_values() -> Vec<(String, String)> {
    let mut rng = get_rng();
    (0..100)
        .map(|_| {
            let len = rng.gen_range(1..=100_000);
            let key = Alphanumeric.sample_string(&mut rng, len as usize);
            let value = Alphanumeric.sample_string(&mut rng, len as usize);
            (key, value)
        })
        .collect()
}

fn bench_writes(key_values: &[(String, String)]) {
    for _ in 0..1_000 {
        let dir = TempDir::new().unwrap();
        let mut kvs = KvsServer::open_with_policy(Some("kvs"), dir.path(), NeverPolicy).unwrap();
        for (key, value) in key_values {
            let cmd = Cmd::Set(key.into(), value.into());
            match kvs.handle_cmd(cmd) {
                Response::Ok => {}
                result => panic!("Failed to set value! {result:?}"),
            }
        }
    }
}

fn bench_reads(key_values: &[(String, String)]) {
    let dir = TempDir::new().unwrap();
    let mut kvs = KvsServer::open_with_policy(Some("kvs"), dir.path(), NeverPolicy).unwrap();
    for (key, value) in key_values {
        let cmd = Cmd::Set(key.into(), value.into());
        match kvs.handle_cmd(cmd) {
            Response::Ok => {}
            result => panic!("Failed to set value! {result:?}"),
        }
    }

    let mut rng = get_rng();
    let mut shuffled = key_values.to_vec();
    shuffled.shuffle(&mut rng);

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
