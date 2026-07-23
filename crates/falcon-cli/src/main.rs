#![forbid(unsafe_code)]

mod cli;
mod client;
mod replication;
mod serve;

use clap::Parser;
use cli::{Cli, Command};

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        // No subcommand: run the server with the top-level serve flags
        // (backward compatible with `falcon --http-bind ...`).
        None => serve::run(cli.serve),
        Some(Command::Serve(args)) => serve::run(args),

        // Client subcommands: talk to a running node over HTTP.
        Some(Command::Get(a)) => client::get(a),
        Some(Command::Put(a)) => client::put(a),
        Some(Command::Del(a)) => client::del(a),
        Some(Command::Scan(a)) => client::scan(a),
        Some(Command::Topic(c)) => client::topic(c),
        Some(Command::Queue(c)) => client::queue(c),
        Some(Command::Stream(c)) => client::stream(c),
        Some(Command::Health(a)) => client::health(a),
        Some(Command::Metrics(a)) => client::metrics(a),
    }
}
