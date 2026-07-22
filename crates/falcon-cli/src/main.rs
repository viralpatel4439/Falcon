#![forbid(unsafe_code)]

mod cli;
mod replication;

use clap::Parser;
use cli::Cli;
use falcon_core::{Config, Node};
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new(&cli.log_level))
        .init();

    let mut config = match &cli.config {
        Some(path) => Config::from_file(std::path::Path::new(path))?,
        None => Config::default(),
    };

    if let Some(bind) = &cli.http_bind {
        config.http.bind = bind.clone();
    }
    if let Some(bind) = &cli.wire_bind {
        config.wire.bind = bind.clone();
    }
    if cli.wire_disabled {
        config.wire.enabled = false;
    }
    if let Some(id) = &cli.node_id {
        config.node.id = id.clone();
    }
    if let Some(region) = &cli.region {
        config.node.region = region.clone();
    }
    if let Some(dir) = &cli.data_dir {
        config.storage.data_dir = dir.clone();
    }
    if let Some(token) = &cli.auth_token {
        config.auth.api_key = token.clone();
    } else if let Ok(legacy) = std::env::var("FALCON_AUTH_TOKEN") {
        // Backward-compat: the env var was renamed to FALCON_API_KEY.
        config.auth.api_key = legacy;
    }
    config.validate()?;

    tracing::info!(
        node_id = %config.node.id,
        region = %config.node.region,
        data_dir = %config.storage.data_dir,
        "starting Falcon"
    );

    let node = Arc::new(Node::build(config.clone())?);
    replication::start(node.clone()).await?;

    // Background TTL reaper (per-write and per-keyspace expiry).
    node.spawn_reaper(std::time::Duration::from_secs(1));
    // Background WAL compaction (bounds disk + restart-replay time).
    node.spawn_compactor();

    // Broadcast a single shutdown trigger to every server so SIGTERM drains
    // them all before the final durable flush.
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

    // Startup finished: mark ready so /readyz and load balancers admit traffic.
    node.set_ready(true);

    // Fan the OS signal out to all servers, then run the HTTP server until it
    // drains.
    let signal_tx = shutdown_tx.clone();
    tokio::spawn(async move {
        falcon_core::shutdown_signal().await;
        let _ = signal_tx.send(());
    });

    let bind: std::net::SocketAddr = config.http.bind.parse()?;
    let mut http_shutdown = shutdown_tx.subscribe();
    let http_signal = async move {
        let _ = http_shutdown.recv().await;
    };
    falcon_api::serve_with_shutdown(node.clone(), bind, http_signal).await?;

    // All servers have stopped accepting and drained. Stop reporting ready,
    // then perform the authoritative final flush so no acked-but-buffered
    // write (sharded coalesce window, interval-fsync WAL) is lost.
    node.set_ready(false);
    let grace = std::time::Duration::from_secs(config.ops.shutdown_grace_secs.max(1));
    match tokio::time::timeout(grace, node.flush_all()).await {
        Ok(()) => tracing::info!("final flush complete; exiting cleanly"),
        Err(_) => tracing::warn!("final flush timed out after {:?}; exiting", grace),
    }
    Ok(())
}