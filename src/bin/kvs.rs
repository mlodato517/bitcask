use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(version)]
struct Args {
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    Set { key: String, value: String },
    Get { key: String },
    Rm { key: String },
}

fn main() {
    let _args = Args::parse();
    eprintln!("unimplemented");
    std::process::exit(1);
}
