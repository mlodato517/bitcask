//! Checking various methods for improving UTF-8 validation.
//!
//! The first few ideas were:
//!
//! 1. CRC check
//! 2. SHA check
//! 3. Faster UTF8 validation
//!
//! It looks like a SHA check is slower, so let's ignore that. CRC and `simdutf8` are both viable
//! options, with the latter _feeling_ safer :crossed_fingers:

use std::hint::black_box;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use protocol::Cmd;
use rand::{distributions::Uniform, prelude::*};
use sha3::Digest;

fn utf8(c: &mut Criterion) {
    let mut group = c.benchmark_group("Utf8");

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

    let test_data = Vec::from_iter(keys.into_iter().zip(values).map(|(key, value)| {
        let key_len = key.len();
        let cmd = Cmd::Set(key.into(), value.into());
        let mut buf = Vec::new();
        cmd.write(&mut buf).unwrap();
        let crc = crc32fast::hash(&buf);
        let mut sha_hasher = sha3::Sha3_256::new();
        sha_hasher.update(&buf);
        let sha = sha_hasher.finalize();

        (buf, crc, sha, key_len)
    }));

    group.bench_with_input(BenchmarkId::new("crc", ""), "", |b, _| {
        b.iter(|| {
            for (bytes, crc, _sha, key_len) in &test_data {
                let new_crc = crc32fast::hash(black_box(bytes));
                if new_crc == *crc {
                    let body = &bytes[12..];
                    let (key, value) = body.split_at(*key_len);
                    let _key = unsafe { black_box(std::str::from_utf8_unchecked(key)) };
                    let _value = unsafe { black_box(std::str::from_utf8_unchecked(value)) };
                } else {
                    panic!("These should match!");
                }
            }
        });
    });
    group.bench_with_input(BenchmarkId::new("sha", ""), "", |b, _| {
        b.iter(|| {
            for (bytes, _crc, sha, key_len) in &test_data {
                let mut sha_hasher = sha3::Sha3_256::new();
                sha_hasher.update(bytes);
                let new_sha = sha_hasher.finalize();
                if new_sha == *sha {
                    let body = &bytes[12..];
                    let (key, value) = body.split_at(*key_len);
                    let _key = unsafe { black_box(std::str::from_utf8_unchecked(key)) };
                    let _value = unsafe { black_box(std::str::from_utf8_unchecked(value)) };
                } else {
                    panic!("These should match!");
                }
            }
        });
    });
    group.bench_with_input(BenchmarkId::new("utf8", ""), "", |b, _| {
        b.iter(|| {
            for (bytes, _crc, _sha, key_len) in &test_data {
                let body = &bytes[12..];
                let (key, value) = body.split_at(*key_len);
                let _key = black_box(std::str::from_utf8(key).unwrap());
                let _value = black_box(std::str::from_utf8(value).unwrap());
            }
        });
    });
    group.bench_with_input(BenchmarkId::new("simd", ""), "", |b, _| {
        b.iter(|| {
            for (bytes, _crc, _sha, key_len) in &test_data {
                let body = &bytes[12..];
                let (key, value) = body.split_at(*key_len);
                let _key = black_box(simdutf8::compat::from_utf8(key).unwrap());
                let _value = black_box(simdutf8::compat::from_utf8(value).unwrap());
            }
        });
    });
    group.finish();
}

criterion_group!(benches, utf8);
criterion_main!(benches);
