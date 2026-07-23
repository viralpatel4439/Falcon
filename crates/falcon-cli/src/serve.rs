//! `falcon serve` — run a node from the installed profile.
//!
//! Configuration comes ONLY from the profile file (written by `falcon install`
//! / `falcon config` or the web UI). Falcon reads no environment variables.
//! The `serve` flags may override individual fields for a single run, but the
//! profile remains the durable source of truth.
//!
//! Builds an explicit multi-threaded Tokio runtime (one worker per logical CPU
//! by default) so every active subsystem runs concurrently across all cores.

use crate::cli::ServeArgs;
use crate::features;
use crate::replication;
use falcon_core::{Config, FeatureSet, Node, Profile};
use std::path::PathBuf;
use std::sync::Arc;

pub fn run(profile_flag: &Option<String>, args: ServeArgs) -> anyhow::Result<()> {
    let profile_path: PathBuf = profile_flag
        .clone()
        .map(PathBuf::from)
        .unwrap_or_else(falcon_core::default_profile_path);

    let profile = match Profile::load(&profile_path) {
        Ok(p) => p,
        Err(falcon_core::ProfileError::NotFound(p)) => {
            anyhow::bail!(
                "no profile at {} — install a product first, e.g.:\n  falcon install cache",
                p.display()
            );
        }
        Err(e) => return Err(e.into()),
    };

    if profile.features.is_empty() {
        anyhow::bail!("profile has no products installed — run `falcon install <feature>`");
    }

    // A profile must not activate a product this binary didn't compile in.
    let compiled = features::compiled();
    if let Some(missing) = profile.features.first_uncompiled(&compiled) {
        anyhow::bail!(
            "profile activates '{}', which this build does not include (compiled: {}).\n\
             Run a build that includes it, or the full build.",
            missing,
            compiled
        );
    }

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new(&args.log_level))
        .init();

    // Select the rustls crypto provider once, before any TLS listener is built.
    falcon_core::tls::init_crypto_provider();

    let active = profile.features.clone();
    let config = build_config(&profile, &args)?;

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

    runtime.block_on(async move { serve(config, active, profile_path, workers).await })
}

/// Turn the profile into a runtime `Config`, then apply any one-run `serve`
/// flag overrides. Order: profile < serve flags. No environment layer.
fn build_config(profile: &Profile, args: &ServeArgs) -> anyhow::Result<Config> {
    let mut config = profile.to_config();

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

    config.validate()?;
    Ok(config)
}

async fn serve(
    config: Config,
    active: FeatureSet,
    profile_path: PathBuf,
    workers: usize,
) -> anyhow::Result<()> {
    tracing::info!(
        node_id = %config.node.id,
        region = %config.node.region,
        products = %active,
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
    tracing::info!(%bind, "product UI at http://{bind}/  ·  metrics at /metrics");
    let mut http_shutdown = shutdown_tx.subscribe();
    let http_signal = async move {
        let _ = http_shutdown.recv().await;
    };
    falcon_api::serve_with_shutdown(node.clone(), bind, active, profile_path, http_signal).await?;

    node.set_ready(false);
    let grace = std::time::Duration::from_secs(config.ops.shutdown_grace_secs.max(1));
    match tokio::time::timeout(grace, node.flush_all()).await {
        Ok(()) => tracing::info!("final flush complete; exiting cleanly"),
        Err(_) => tracing::warn!("final flush timed out after {:?}; exiting", grace),
    }
    Ok(())
}
