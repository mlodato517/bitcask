# Pingcap Talent Plan Rust Database

Me following [this talent plan][pingcap] to build a bitcask-esque DB in Rust.

## Testing

Because this is a Cargo workspace, you must run `cargo build` to put binaries
in the right directory for the CLI integration tests (which use
`Command::cargo_bin`) to succeed:

```sh
cargo build --bin kvs-server && cargo build --bin kvs-client && cargo test
```

[pingcap]: https://github.com/pingcap/talent-plan/blob/master/courses/rust/README.md
