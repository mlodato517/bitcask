use std::time::Duration;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use rand::{distributions::Uniform, prelude::*};

use kvs::KvsEngine;
use kvs_server::{Engine, EngineType};

fn write(c: &mut Criterion) {
    let mut group = c.benchmark_group("Write");

    let key_len_range = Uniform::new(1, 100_000);
    let value_len_range = Uniform::new(1, 100_000);
    let char_range = Uniform::new('\0', char::MAX);
    let mut rng = rand::rngs::StdRng::seed_from_u64(0);
    let keys = (0..100)
        .map(|_| {
            let key_len = rng.sample(key_len_range);
            (0..key_len)
                .map(|_| rng.sample(char_range))
                .collect::<String>()
        })
        .collect::<Vec<_>>();
    let values = (0..100)
        .map(|_| {
            let value_len = rng.sample(value_len_range);
            (0..value_len)
                .map(|_| rng.sample(char_range))
                .collect::<String>()
        })
        .collect::<Vec<_>>();

    for engine_type in [EngineType::Kvs, EngineType::Sled] {
        group.bench_with_input(BenchmarkId::new(engine_type.to_string(), ""), "", |b, _| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let dir = tempfile::tempdir().unwrap();
                    let mut engine = Engine::new_in(Some(engine_type), dir.path()).unwrap();
                    let sets = keys.clone().into_iter().zip(&values);
                    let start = std::time::Instant::now();
                    for (key, value) in sets {
                        engine.set(key, value).unwrap();
                    }
                    total += start.elapsed();
                    dir.close().unwrap();
                }
                total
            });
        });
    }
    group.finish();
}

criterion_group!(benches, write);
criterion_main!(benches);
