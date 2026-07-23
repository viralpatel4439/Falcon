//! `falcon serve` — run a node. Builds an explicit multi-threaded Tokio
//! runtime (one worker per logical CPU by default, or `--worker-threads N`) so
//! every subsystem — KV, pub/sub, queues, event streams, realtime, and
//! replication — runs concurrently across all cores.

use crate::cli::ServeArgs;
use crate::replication;
use falcon_core::config::{QueueConfig, StreamConfig, TopicConfig, TopicModeConfig};
use falcon_core::{Config, Node};
use std::sync::Arc;

pub fn run(args: ServeArgs) -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new(&args.log_level))
        .init();

    let config = build_config(&args)?;

    // Explicit multi-threaded runtime. Default worker count = logical CPUs.
    let mut rt = tokio::runtime::Builder::new_multi_thread();
    rt.enable_all();
    if let Some(n) = args.worker_threads.filter(|&n| n > 0) {
        rt.worker_threads(n);
    }
    let runtime = rt.build()?;
    let workers = args
        .worker_threads
        .filter(|&n| n > 0)
        .unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1));

    runtime.block_on(async move { serve(config, workers).await })
}

/// Merge config file (if any) with CLI/env overrides. Order: defaults < file <
/// env < flags (clap already applies env before flags for a single field).
fn build_config(args: &ServeArgs) -> anyhow::Result<Config> {
    let mut config = match &args.config {
        Some(path) => Config::from_file(std::path::Path::new(path))?,
        None => Config::default(),
    };

    if let Some(v) = &args.http_bind {
        config.http.bind = v.clone();
    }
    if let Some(v) = &args.wire_bind {
        config.wire.bind = v.clone();
    }
    if args.wire_disabled {
        config.wire.enabled = false;
    }
    if let Some(v) = &args.node_id {
        config.node.id = v.clone();
    }
    if let Some(v) = &args.region {
        config.node.region = v.clone();
    }
    if let Some(v) = &args.data_dir {
        config.storage.data_dir = v.clone();
    }
    if let Some(v) = &args.default_tier {
        config.storage.default_tier = parse_tier(v)?;
        // Apply to the default keyspace(s) that inherit the storage default.
        for ks in &mut config.keyspaces {
            ks.tier = config.storage.default_tier;
        }
    }
    if let Some(v) = &args.api_key {
        config.auth.api_key = v.clone();
    } else if let Ok(legacy) = std::env::var("FALCON_AUTH_TOKEN") {
        config.auth.api_key = legacy;
    }
    if args.subscriptions {
        config.subscriptions.enabled = true;
    }

    // Declarative messaging from flags (added to whatever the file declared).
    for spec in &args.topics {
        let (name, mode) = split2(spec);
        config.topics.push(TopicConfig {
            name,
            mode: match mode.as_deref() {
                Some("durable") => TopicModeConfig::Durable,
                _ => TopicModeConfig::Ephemeral,
            },
            capacity: 1024,
        });
    }
    for spec in &args.queues {
        let (name, ack) = split2(spec);
        config.queues.push(QueueConfig {
            name,
            ack_timeout_secs: ack.and_then(|s| s.parse().ok()).unwrap_or(30),
        });
    }
    for spec in &args.streams {
        let (name, parts) = split2(spec);
        config.streams.push(StreamConfig {
            name,
            partitions: parts.and_then(|s| s.parse().ok()).unwrap_or(1),
            capacity: 1024,
            interval_fsync_ms: 0,
        });
    }

    config.validate()?;
    Ok(config)
}

fn split2(spec: &str) -> (String, Option<String>) {
    match spec.split_once(':') {
        Some((a, b)) => (a.to_string(), Some(b.to_string())),
        None => (spec.to_string(), None),
    }
}

fn parse_tier(s: &str) -> anyhow::Result<falcon_core::TierName> {
    use falcon_core::TierName::*;
    Ok(match s {
        "hot" => Hot,
        "warm" => Warm,
        "cold" => Cold,
        "tiered" => Tiered,
        "sharded" => Sharded,
        other => anyhow::bail!("unknown tier '{other}' (use hot|warm|cold|tiered|sharded)"),
    })
}

async fn serve(config: Config, workers: usize) -> anyhow::Result<()> {
    tracing::info!(
        node_id = %config.node.id,
        region = %config.node.region,
        data_dir = %config.storage.data_dir,
        worker_threads = workers,
        "starting Falcon (multi-core)"
    );

    let node = Arc::new(Node::build(config.clone())?);
    replication::start(node.clone()).await?;

    node.spawn_reaper(std::time::Duration::from_secs(1));
    node.spawn_compactor();

    let (shutdown_tx, _) = tokio::sync::broadcast::channel::<()>(1);

    if config.wire.enabled {
        let wire_bind: std::net::SocketAddr = config.wire.bind.parse()?;
        let wire_node = node.clone();
        let mut wire_shutdown = shutdown_tx.subscribe();
        tokio::spawn(async move {
            let signal = async move {
                let _ = wire_shutdown.recv().await;
            };
            if let Err(e) = falcon_wire::serve_with_shutdown(wire_node, wire_bind, signal).await {
                tracing::error!(error = %e, "wire server exited");
            }
        });
    }

    node.set_ready(true);

    let signal_tx = shutdown_tx.clone();
    tokio::spawn(async move {
        falcon_core::shutdown_signal().await;
        let _ = signal_tx.send(());
    });

    let bind: std::net::SocketAddr = config.http.bind.parse()?;
    tracing::info!(%bind, "dashboard UI at http://{bind}/  ·  metrics at /metrics");
    let mut http_shutdown = shutdown_tx.subscribe();
    let http_signal = async move {
        let _ = http_shutdown.recv().await;
    };
    falcon_api::serve_with_shutdown(node.clone(), bind, http_signal).await?;

    node.set_ready(false);
    let grace = std::time::Duration::from_secs(config.ops.shutdown_grace_secs.max(1));
    match tokio::time::timeout(grace, node.flush_all()).await {
        Ok(()) => tracing::info!("final flush complete; exiting cleanly"),
        Err(_) => tracing::warn!("final flush timed out after {:?}; exiting", grace),
    }
    Ok(())
}
