use std::hint::black_box;
use std::time::Instant;

use criterion::{criterion_group, criterion_main, Criterion};
use kvs::NeverPolicy;
use kvs_server::response::Response;
use kvs_server::KvsServer;
use protocol::Cmd;
use rand::distributions::{Alphanumeric, DistString};
use rand::prelude::*;
use tempfile::TempDir;

fn get_rng() -> StdRng {
    StdRng::seed_from_u64(4)
}

// Test writing 100 random values with keys and values having 1-100_000 bytes.
fn bench_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("write");

    let mut rng = get_rng();
    let key_values: Vec<_> = (0..100)
        .map(|_| {
            let len = rng.gen_range(1..=100_000);
            let key = Alphanumeric.sample_string(&mut rng, len as usize);
            let value = Alphanumeric.sample_string(&mut rng, len as usize);
            (key, value)
        })
        .collect();

    group.bench_function("kvs", |b| {
        b.iter_custom(|iters| {
            // Must declare dir in here, otherwise `TempDir` will go out of scope and be deleted.
            let dir = TempDir::new().unwrap();
            let mut kvs =
                KvsServer::open_with_policy(Some("kvs"), dir.path(), NeverPolicy).unwrap();

            let start = Instant::now();
            for _i in 0..iters {
                for (key, value) in &key_values {
                    let cmd = Cmd::Set(key.into(), value.into());
                    if let Response::Err(e) = kvs.handle_cmd(cmd) {
                        panic!("Unexpected error: {e:?}")
                    }
                }
            }
            start.elapsed()
        })
    });
    group.bench_function("sled", |b| {
        b.iter_custom(|iters| {
            // Must declare dir in here, otherwise `TempDir` will go out of scope and be deleted.
            let dir = TempDir::new().unwrap();
            let mut kvs = KvsServer::open(Some("sled"), dir.path()).unwrap();

            let start = Instant::now();
            for _i in 0..iters {
                for (key, value) in &key_values {
                    let cmd = Cmd::Set(key.into(), value.into());
                    if let Response::Err(e) = kvs.handle_cmd(cmd) {
                        panic!("Unexpected error: {e:?}")
                    }
                }
            }
            start.elapsed()
        })
    });
    group.bench_function("std::HashMap", |b| {
        b.iter_custom(|iters| {
            let mut kvs = std::collections::HashMap::new();

            let start = Instant::now();
            for _i in 0..iters {
                for (key, value) in &key_values {
                    kvs.insert(key.to_owned(), value);
                    kvs = black_box(kvs);
                }
            }
            start.elapsed()
        })
    });
    group.bench_function("hashbrown::HashMap", |b| {
        b.iter_custom(|iters| {
            let mut kvs = hashbrown::HashMap::new();

            let start = Instant::now();
            for _i in 0..iters {
                for (key, value) in &key_values {
                    kvs.insert(key.to_owned(), value);
                    kvs = black_box(kvs);
                }
            }
            start.elapsed()
        })
    });
    group.finish();
}

/// read 1000 values from previously written keys, with keys and values of random length.
fn bench_reads(c: &mut Criterion) {
    let mut group = c.benchmark_group("read");

    let mut rng = get_rng();
    let key_values: Vec<_> = (0..100)
        .map(|_| {
            let len = rng.gen_range(1..=100_000);
            let key = Alphanumeric.sample_string(&mut rng, len as usize);
            let value = Alphanumeric.sample_string(&mut rng, len as usize);
            (key, value)
        })
        .collect();
    let mut keys: Vec<_> = key_values.iter().map(|(key, _)| key.to_owned()).collect();
    keys.shuffle(&mut rng);

    group.bench_function("kvs", |b| {
        b.iter_custom(|iters| {
            // Must declare dir in here, otherwise `TempDir` will go out of scope and be deleted.
            let dir = TempDir::new().unwrap();
            let mut kvs = KvsServer::open(Some("kvs"), dir.path()).unwrap();

            for (key, value) in &key_values {
                let cmd = Cmd::Set(key.into(), value.into());
                if let Response::Err(e) = kvs.handle_cmd(cmd) {
                    panic!("Unexpected error: {e:?}")
                }
            }

            let start = Instant::now();
            for _i in 0..iters {
                for key in &keys {
                    let get = Cmd::Get(key.into());
                    if let Response::Err(e) = kvs.handle_cmd(get) {
                        panic!("Unexpected error: {e:?}")
                    }
                }
            }
            start.elapsed()
        })
    });
    group.bench_function("sled", |b| {
        b.iter_custom(|iters| {
            // Must declare dir in here, otherwise `TempDir` will go out of scope and be deleted.
            let dir = TempDir::new().unwrap();
            let mut kvs = KvsServer::open(Some("sled"), dir.path()).unwrap();

            for (key, value) in &key_values {
                let cmd = Cmd::Set(key.into(), value.into());
                if let Response::Err(e) = kvs.handle_cmd(cmd) {
                    panic!("Unexpected error: {e:?}")
                }
            }

            let start = Instant::now();
            for _i in 0..iters {
                for key in &keys {
                    let get = Cmd::Get(key.into());
                    if let Response::Err(e) = kvs.handle_cmd(get) {
                        panic!("Unexpected error: {e:?}")
                    }
                }
            }
            start.elapsed()
        })
    });
    group.finish();
}

criterion_group!(benches, bench_writes, bench_reads);
criterion_main!(benches);
