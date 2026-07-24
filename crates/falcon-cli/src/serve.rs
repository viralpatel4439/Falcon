//! `falcon serve` — run a node from the installed profile.
//!
//! Configuration comes ONLY from the profile file (written by `falcon install`
//! / `falcon config` or the web UI). Falcon reads no environment variables.
//! The `serve` flags may override individual fields for a single run, but the
//! profile remains the durable source of truth.
//!
//! Concurrency is **automatic**. Falcon builds a multi-threaded, work-stealing
//! Tokio runtime sized to the machine — there is no thread/worker/core knob to
//! tune. The async worker pool gets one thread per logical CPU (so every active
//! subsystem runs across all cores), and a separate, elastic blocking pool
//! absorbs the blocking work (WAL/sled fsyncs) without ever starving the async
//! workers. The scheduler load-balances tasks across workers by work-stealing,
//! so the runtime adapts to load on its own rather than to a fixed setting.

use crate::cli::ServeArgs;
use crate::features;
use crate::replication;
use falcon_core::{Config, FeatureSet, Node, Profile};
use std::path::PathBuf;
use std::sync::Arc;

/// How Falcon auto-sized the runtime for this machine. Logged at startup so the
/// chosen concurrency is transparent even though it isn't configurable.
#[derive(Clone, Copy)]
struct RuntimePlan {
    /// Async worker threads = logical CPUs. One per core → all cores usable.
    workers: usize,
    /// Upper bound on the elastic blocking pool (fsync/sled offload). Scaled to
    /// core count with a floor, so a busy blocking path can't stall the async
    /// workers, while an idle node keeps few threads parked.
    max_blocking: usize,
}

impl RuntimePlan {
    /// Derive the plan purely from the hardware — no user input.
    fn detect() -> Self {
        let workers = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        // Blocking work is bursty (a batch of fsyncs), so allow more blocking
        // threads than cores, with a sane floor for low-core machines.
        let max_blocking = (workers * 4).max(8);
        Self {
            workers,
            max_blocking,
        }
    }

    /// Build the multi-threaded runtime this plan describes.
    fn build(self) -> std::io::Result<tokio::runtime::Runtime> {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(self.workers)
            .max_blocking_threads(self.max_blocking)
            .thread_name("falcon-worker")
            .build()
    }
}

pub fn run(profile_flag: &Option<String>, args: ServeArgs) -> anyhow::Result<()> {
    // Advanced/testing path: a full engine config file bypasses the profile.
    if let Some(cfg_path) = args.config.clone() {
        return run_from_config_file(&cfg_path, &args);
    }

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

    let plan = RuntimePlan::detect();
    let runtime = plan.build()?;
    runtime.block_on(async move { serve(config, active, profile_path, plan).await })
}

/// Serve from a full engine config TOML (the `--config` escape hatch). Derives
/// the active product set from what the config declares so the API/UI gate the
/// same way a profile would. Used by the benchmark harness and advanced setups.
fn run_from_config_file(cfg_path: &str, args: &ServeArgs) -> anyhow::Result<()> {
    use falcon_core::Feature;

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new(&args.log_level))
        .init();
    falcon_core::tls::init_crypto_provider();

    let mut config = Config::from_file(std::path::Path::new(cfg_path))?;
    // One-run overrides still apply on top of the file.
    if let Some(v) = &args.http_bind {
        config.http.bind = v.clone();
    }
    if let Some(v) = &args.wire_bind {
        config.wire.bind = v.clone();
    }
    if args.wire_disabled {
        config.wire.enabled = false;
    }
    if let Some(v) = &args.data_dir {
        config.storage.data_dir = v.clone();
    }
    config.validate()?;

    // Derive active features from the declared objects so route gating matches.
    let mut active = falcon_core::FeatureSet::new();
    active.insert(Feature::Kv);
    active.insert(Feature::Cache);
    if !config.topics.is_empty() {
        active.insert(Feature::Pubsub);
    }
    if !config.queues.is_empty() {
        active.insert(Feature::Queue);
    }
    if !config.streams.is_empty() {
        active.insert(Feature::Stream);
    }

    let plan = RuntimePlan::detect();
    let runtime = plan.build()?;
    let profile_path = falcon_core::default_profile_path();
    runtime.block_on(async move { serve(config, active, profile_path, plan).await })
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
    plan: RuntimePlan,
) -> anyhow::Result<()> {
    tracing::info!(
        node_id = %config.node.id,
        region = %config.node.region,
        products = %active,
        data_dir = %config.storage.data_dir,
        worker_threads = plan.workers,
        max_blocking_threads = plan.max_blocking,
        "starting Falcon (auto-sized runtime: one async worker per core, elastic blocking pool)"
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
