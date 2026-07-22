use crate::config::{Config, KeyspaceConfig, TierName, TopicModeConfig};
use crate::keyspace::Keyspace;
use falcon_events::EventBus;
use falcon_messaging::{Messaging, QueueSpec, StreamSpec, TopicMode, TopicSpec};
use falcon_metrics::Metrics;
use falcon_storage::{ColdEngine, HotEngine, StorageEngine, StorageError, TieredEngine, WarmEngine};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

/// Composition root: owns one `Keyspace` per configured keyspace plus the
/// messaging layer (topics/queues), built once at startup from `Config`.
/// `kv-api`, `kv-wire`, and `kv-replication` all hold an `Arc<Node>` and
/// never touch storage/event internals directly.
pub struct Node {
    config: Config,
    keyspaces: HashMap<String, Keyspace>,
    messaging: Arc<Messaging>,
    metrics: Arc<Metrics>,
}

#[derive(Debug, thiserror::Error)]
pub enum NodeError {
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
    #[error("messaging error: {0}")]
    Messaging(#[from] falcon_messaging::MessagingError),
    #[error("unknown keyspace '{0}'")]
    UnknownKeyspace(String),
}

impl Node {
    pub fn build(config: Config) -> Result<Self, NodeError> {
        let data_dir = Path::new(&config.storage.data_dir);
        let mut keyspaces = HashMap::new();

        for ks_cfg in &config.keyspaces {
            let keyspace = build_keyspace(ks_cfg, &config, data_dir)?;
            keyspaces.insert(ks_cfg.name.clone(), keyspace);
        }

        let topic_specs: Vec<TopicSpec> = config
            .topics
            .iter()
            .map(|t| TopicSpec {
                name: t.name.clone(),
                mode: match t.mode {
                    TopicModeConfig::Ephemeral => TopicMode::Ephemeral,
                    TopicModeConfig::Durable => TopicMode::Durable,
                },
                capacity: t.capacity,
            })
            .collect();
        let queue_specs: Vec<QueueSpec> = config
            .queues
            .iter()
            .map(|q| QueueSpec {
                name: q.name.clone(),
                ack_timeout: Duration::from_secs(q.ack_timeout_secs),
            })
            .collect();
        let stream_specs: Vec<StreamSpec> = config
            .streams
            .iter()
            .map(|s| StreamSpec {
                name: s.name.clone(),
                partitions: s.partitions,
                capacity: s.capacity,
            })
            .collect();
        let messaging = Arc::new(Messaging::build(
            data_dir.join("messaging"),
            &topic_specs,
            &queue_specs,
            &stream_specs,
        )?);

        Ok(Self {
            config,
            keyspaces,
            messaging,
            metrics: Arc::new(Metrics::new()),
        })
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn messaging(&self) -> &Arc<Messaging> {
        &self.messaging
    }

    /// The process metrics registry — shared with the HTTP/wire servers so
    /// every request path records into the same counters/histograms that
    /// `/metrics` renders.
    pub fn metrics(&self) -> &Arc<Metrics> {
        &self.metrics
    }

    /// Sum of every durable engine's on-disk size (WAL + object files), for
    /// the `falcon_wal_bytes` gauge and compaction decisions. Best-effort;
    /// missing files count as zero.
    pub fn total_durable_bytes(&self) -> u64 {
        self.keyspaces.values().map(|ks| ks.durable_bytes()).sum()
    }

    /// Force every engine that buffers writes to persist now. Called on
    /// graceful shutdown so no acked-but-unflushed write is lost (the
    /// sharded store's coalesce window, in particular).
    pub async fn flush_all(&self) {
        for ks in self.keyspaces.values() {
            ks.flush().await;
        }
    }

    /// Run one compaction pass over every eligible keyspace. Returns the
    /// number of keyspaces actually compacted.
    ///
    /// Compaction renumbers sequences, which would break a replication
    /// leader's watermark contract with its followers, so replicated
    /// keyspaces are skipped. (Their disk is instead bounded by follower
    /// catch-up + snapshot semantics.)
    pub async fn compact_all(&self) -> usize {
        let replicated: std::collections::HashSet<&str> = self
            .config
            .keyspaces
            .iter()
            .filter(|k| k.replication)
            .map(|k| k.name.as_str())
            .collect();
        let mut n = 0;
        for (name, ks) in &self.keyspaces {
            if replicated.contains(name.as_str()) {
                continue;
            }
            if ks.compact().await {
                n += 1;
                self.metrics.wal_compactions_total.inc();
            }
        }
        n
    }

    pub fn keyspace(&self, name: &str) -> Option<&Keyspace> {
        self.keyspaces.get(name)
    }

    pub fn keyspace_names(&self) -> impl Iterator<Item = &str> {
        self.keyspaces.keys().map(|s| s.as_str())
    }

    pub fn require_keyspace(&self, name: &str) -> Result<&Keyspace, NodeError> {
        self.keyspace(name)
            .ok_or_else(|| NodeError::UnknownKeyspace(name.to_string()))
    }

    /// Sweep every keyspace once, eagerly deleting expired keys. Returns
    /// the total number reaped. Called on an interval by the reaper task.
    pub async fn reap_expired(&self) -> usize {
        let mut total = 0;
        for ks in self.keyspaces.values() {
            total += ks.reap_expired().await;
        }
        total
    }

    /// Spawns a background task that reaps expired keys every `interval`.
    /// Only started if at least one keyspace has a default TTL or if any
    /// per-write TTL is possible (always, so callers can just start it).
    pub fn spawn_reaper(self: &Arc<Self>, interval: std::time::Duration) {
        let node = self.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.tick().await; // first tick is immediate; skip it
            loop {
                ticker.tick().await;
                let reaped = node.reap_expired().await;
                if reaped > 0 {
                    tracing::debug!(reaped, "TTL reaper deleted expired keys");
                }
            }
        });
    }

    /// Spawns the background WAL-compaction task if enabled in config. Every
    /// `compaction_interval_secs`, it compacts each eligible keyspace whose
    /// durable size exceeds `compaction_min_bytes`, keeping disk and
    /// restart-replay time bounded over a long-lived container's life.
    pub fn spawn_compactor(self: &Arc<Self>) {
        let ops = &self.config.ops;
        if !ops.compaction_enabled {
            return;
        }
        let node = self.clone();
        let interval = std::time::Duration::from_secs(ops.compaction_interval_secs.max(1));
        let min_bytes = ops.compaction_min_bytes;
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.tick().await; // skip immediate first tick
            loop {
                ticker.tick().await;
                if node.total_durable_bytes() < min_bytes {
                    continue; // nothing big enough to bother rewriting
                }
                let n = node.compact_all().await;
                if n > 0 {
                    tracing::info!(keyspaces = n, "WAL compaction completed");
                }
            }
        });
    }

    /// Mark the node ready (or not) to serve traffic — drives `/readyz` and
    /// the `falcon_ready` gauge. Called once startup finishes.
    pub fn set_ready(&self, ready: bool) {
        self.metrics.ready.set(if ready { 1 } else { 0 });
    }
}

/// Resolves when the process receives SIGTERM (k8s/docker stop) or Ctrl-C
/// (SIGINT). The single shutdown trigger for graceful drain.
pub async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(e) => tracing::error!(error = %e, "failed to install SIGTERM handler"),
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => tracing::info!("received Ctrl-C, shutting down gracefully"),
        _ = terminate => tracing::info!("received SIGTERM, shutting down gracefully"),
    }
}

fn build_keyspace(
    ks_cfg: &KeyspaceConfig,
    config: &Config,
    data_dir: &Path,
) -> Result<Keyspace, NodeError> {
    let engine: Arc<dyn StorageEngine> = match ks_cfg.tier {
        TierName::Hot => Arc::new(HotEngine::new()),
        TierName::Warm => {
            let path = data_dir.join(format!("{}.wal", ks_cfg.name));
            let policy = if ks_cfg.interval_fsync_ms > 0 {
                falcon_storage::FsyncPolicy::IntervalMs(ks_cfg.interval_fsync_ms)
            } else {
                falcon_storage::FsyncPolicy::Always
            };
            Arc::new(WarmEngine::open_with_policy(&path, policy)?)
        }
        TierName::Cold => {
            let path = data_dir.join(format!("{}_cold", ks_cfg.name));
            Arc::new(ColdEngine::open(&path)?)
        }
        TierName::Tiered => {
            let path = data_dir.join(format!("{}_tiered", ks_cfg.name));
            let capacity_bytes = ks_cfg.hot_capacity_mb * 1024 * 1024;
            Arc::new(TieredEngine::open(&path, capacity_bytes, ks_cfg.evict_sample)?)
        }
        TierName::FilePerKey => {
            let path = data_dir.join(format!("{}_files", ks_cfg.name));
            Arc::new(falcon_storage::FilePerKeyEngine::open_local(&path)?)
        }
        TierName::Sharded => {
            let path = data_dir.join(format!("{}_shards", ks_cfg.name));
            let policy = if ks_cfg.shard_flush_ms > 0 {
                falcon_storage::FlushPolicy::Coalesce {
                    interval_ms: ks_cfg.shard_flush_ms,
                }
            } else {
                falcon_storage::FlushPolicy::Sync
            };
            falcon_storage::ShardedObjectStore::open_local(&path, ks_cfg.shard_buckets, policy)?
        }
    };

    let subscriptions_enabled = config.subscriptions.enabled || ks_cfg.subscriptions;
    let needs_bus = subscriptions_enabled || ks_cfg.replication;
    let events = if needs_bus { Some(EventBus::new()) } else { None };

    let keyspace = Keyspace::new(
        ks_cfg.name.clone(),
        config.node.region.clone(),
        engine,
        events,
        ks_cfg.default_ttl_secs,
    );

    // Multi-leader (active-active) writes converge via HLC last-write-wins.
    // The node id is the HLC region tiebreak (globally unique per node).
    if ks_cfg.write_mode == crate::config::WriteMode::MultiLeader {
        Ok(keyspace.with_multi_leader(config.node.id.clone()))
    } else {
        Ok(keyspace)
    }
}
