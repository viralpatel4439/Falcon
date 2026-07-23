#![forbid(unsafe_code)]

mod cli;
mod client;
mod features;
mod install;
mod replication;
mod serve;

use anyhow::bail;
use clap::Parser;
use cli::{Cli, Command};
use falcon_core::Feature;

/// Reject a client verb whose product wasn't compiled into this slim binary.
/// The full build compiles everything, so this only bites slim builds.
fn require(feature: Feature) -> anyhow::Result<()> {
    if !features::compiled().contains(feature) {
        bail!(
            "the '{}' product is not part of this build.\n\
             Use the full build, or a build that includes it:\n  \
             cargo build --release --no-default-features --features feat-{}",
            feature,
            feature
        );
    }
    Ok(())
}

/// Key-value verbs (`get/put/del/scan`) belong to both the Cache and KV
/// products, so either being compiled in is enough.
fn require_kv_ops() -> anyhow::Result<()> {
    let c = features::compiled();
    if c.contains(Feature::Kv) || c.contains(Feature::Cache) {
        Ok(())
    } else {
        bail!(
            "key-value verbs need the 'kv' or 'cache' product, not part of this build.\n\
             Use the full build, or: cargo build --release --no-default-features --features feat-cache"
        )
    }
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let profile = cli.profile;

    match cli.command {
        // Profile management (no running node needed).
        Command::Install(a) => install::install(&profile, a),
        Command::Uninstall(a) => install::uninstall(&profile, a),
        Command::Status => install::status(&profile),
        Command::Config(c) => install::config(&profile, c),
        Command::Peers(c) => install::peers(&profile, c),

        // Run a node from the profile.
        Command::Serve(args) => serve::run(&profile, args),

        // Client subcommands: talk to a running node over HTTP. Each is gated
        // on its product being compiled into this binary.
        Command::Get(a) => require_kv_ops().and_then(|_| client::get(a)),
        Command::Put(a) => require_kv_ops().and_then(|_| client::put(a)),
        Command::Del(a) => require_kv_ops().and_then(|_| client::del(a)),
        Command::Scan(a) => require_kv_ops().and_then(|_| client::scan(a)),
        Command::Topic(c) => require(Feature::Pubsub).and_then(|_| client::topic(c)),
        Command::Queue(c) => require(Feature::Queue).and_then(|_| client::queue(c)),
        Command::Stream(c) => require(Feature::Stream).and_then(|_| client::stream(c)),
        Command::Health(a) => client::health(a),
        Command::Metrics(a) => client::metrics(a),
    }
}
