use falcon_storage::StorageTier;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeConfig {
    #[serde(default = "default_node_id")]
    pub id: String,
    #[serde(default = "default_region")]
    pub region: String,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            id: default_node_id(),
            region: default_region(),
        }
    }
}

fn default_node_id() -> String {
    "node-1".to_string()
}
fn default_region() -> String {
    "local".to_string()
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HttpConfig {
    #[serde(default = "default_http_bind")]
    pub bind: String,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            bind: default_http_bind(),
        }
    }
}

fn default_http_bind() -> String {
    "0.0.0.0:8080".to_string()
}

/// Optional shared-secret API key. When empty (default), auth is OFF and no
/// checks run anywhere — zero overhead. When set, EVERY client on every
/// path must present the matching key: HTTP (`Authorization: Bearer` or
/// `?api_key=`), the binary wire protocol (an AUTH frame first), and gRPC
/// replication between containers (`authorization` metadata).
///
/// In config, write it as `api_key = "..."` (or the legacy `token = "..."`).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct AuthConfig {
    /// The shared API key. Accepts `api_key` or the legacy `token` key name.
    #[serde(default, alias = "token")]
    pub api_key: String,
}

impl AuthConfig {
    pub fn is_enabled(&self) -> bool {
        !self.api_key.is_empty()
    }

    /// The configured key (empty = auth off).
    pub fn key(&self) -> &str {
        &self.api_key
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WireConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_wire_bind")]
    pub bind: String,
    /// Reserved for the opt-in concurrent-dispatch fast path. 1 = strict
    /// sequential dispatch per pipeline batch (Redis-like ordering).
    #[serde(default = "default_pipeline_concurrency")]
    pub pipeline_concurrency: usize,
}

impl Default for WireConfig {
    fn default() -> Self {
        Self {
            enabled: default_true(),
            bind: default_wire_bind(),
            pipeline_concurrency: default_pipeline_concurrency(),
        }
    }
}

fn default_true() -> bool {
    true
}
fn default_wire_bind() -> String {
    "0.0.0.0:6380".to_string()
}
fn default_pipeline_concurrency() -> usize {
    1
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StorageConfig {
    #[serde(default = "default_data_dir")]
    pub data_dir: String,
    #[serde(default = "default_tier")]
    pub default_tier: TierName,
    /// Max accepted value/body size in bytes (anti-OOM). A PUT larger than
    /// this is rejected with 413. Default 64 MiB; set 0 to disable the cap.
    #[serde(default = "default_max_value_bytes")]
    pub max_value_bytes: usize,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: default_data_dir(),
            default_tier: default_tier(),
            max_value_bytes: default_max_value_bytes(),
        }
    }
}

fn default_data_dir() -> String {
    "./data".to_string()
}
fn default_max_value_bytes() -> usize {
    64 * 1024 * 1024
}
fn default_tier() -> TierName {
    TierName::Warm
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TierName {
    Hot,
    Warm,
    Cold,
    Tiered,
    #[serde(rename = "file-per-key", alias = "fileperkey")]
    FilePerKey,
    Sharded,
}

impl From<TierName> for StorageTier {
    fn from(t: TierName) -> Self {
        match t {
            TierName::Hot => StorageTier::Hot,
            TierName::Warm => StorageTier::Warm,
            TierName::Cold => StorageTier::Cold,
            TierName::Tiered => StorageTier::Tiered,
            TierName::FilePerKey => StorageTier::FilePerKey,
            TierName::Sharded => StorageTier::Sharded,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SubscriptionsConfig {
    #[serde(default)]
    pub enabled: bool,
}

/// Operational tuning for a long-lived, autoscaled container: metrics,
/// background WAL compaction, and graceful-shutdown drain. All ON by default
/// with production-safe values; every field is overridable.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OpsConfig {
    /// Expose Prometheus metrics at `/metrics`. Default true.
    #[serde(default = "default_true")]
    pub metrics_enabled: bool,
    /// Run background WAL compaction. Default true.
    #[serde(default = "default_true")]
    pub compaction_enabled: bool,
    /// How often the compaction task evaluates each keyspace, in seconds.
    #[serde(default = "default_compaction_interval_secs")]
    pub compaction_interval_secs: u64,
    /// Compact a keyspace's WAL only once it exceeds this many bytes, so a
    /// small/idle store is never rewritten needlessly.
    #[serde(default = "default_compaction_min_bytes")]
    pub compaction_min_bytes: u64,
    /// Max seconds to wait for in-flight requests to drain on shutdown before
    /// forcing exit (after which a final flush still runs).
    #[serde(default = "default_shutdown_grace_secs")]
    pub shutdown_grace_secs: u64,
}

impl Default for OpsConfig {
    fn default() -> Self {
        Self {
            metrics_enabled: true,
            compaction_enabled: true,
            compaction_interval_secs: default_compaction_interval_secs(),
            compaction_min_bytes: default_compaction_min_bytes(),
            shutdown_grace_secs: default_shutdown_grace_secs(),
        }
    }
}

fn default_compaction_interval_secs() -> u64 {
    300
}
fn default_compaction_min_bytes() -> u64 {
    64 * 1024 * 1024
}
fn default_shutdown_grace_secs() -> u64 {
    25
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReplicationConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_role")]
    pub role: ReplicationRole,
    #[serde(default = "default_grpc_bind")]
    pub grpc_bind: String,
    #[serde(default)]
    pub leader_addr: Option<String>,
    #[serde(default)]
    pub peers: Vec<PeerConfig>,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            role: default_role(),
            grpc_bind: default_grpc_bind(),
            leader_addr: None,
            peers: Vec::new(),
        }
    }
}

fn default_role() -> ReplicationRole {
    ReplicationRole::Leader
}
fn default_grpc_bind() -> String {
    "0.0.0.0:7070".to_string()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ReplicationRole {
    Leader,
    Follower,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeerConfig {
    pub node_id: String,
    pub addr: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KeyspaceConfig {
    pub name: String,
    #[serde(default = "default_tier")]
    pub tier: TierName,
    #[serde(default)]
    pub subscriptions: bool,
    #[serde(default)]
    pub replication: bool,
    /// Only used when `tier = "tiered"`: max RAM (MB) for the hot cache
    /// in front of the durable on-disk store. The dataset can exceed this;
    /// only the hot working set is held in RAM.
    #[serde(default = "default_hot_capacity_mb")]
    pub hot_capacity_mb: usize,
    /// Only used when `tier = "tiered"`: CLOCK eviction sample size.
    #[serde(default = "default_evict_sample")]
    pub evict_sample: usize,
    /// Only used when `tier = "sharded"`: number of buckets keys hash into
    /// (rounded up to a power of two). Each bucket is one object in the
    /// backing store, so N buckets = N objects regardless of key count —
    /// this is what keeps a request-billed object store cheap. Pick N so a
    /// bucket object stays a comfortable size (e.g. 4096 for millions of
    /// small keys).
    #[serde(default = "default_shard_buckets")]
    pub shard_buckets: usize,
    /// Only used when `tier = "sharded"`: 0 (default) = flush every write
    /// (durable, one object PUT per write). > 0 = coalesce dirty buckets and
    /// flush every `shard_flush_ms` milliseconds — far fewer object writes
    /// under load, at a bounded crash-loss window.
    #[serde(default)]
    pub shard_flush_ms: u64,
    /// Default time-to-live for keys in this keyspace, in seconds. 0 = no
    /// expiry (default). A per-write TTL (via the API) overrides this.
    #[serde(default)]
    pub default_ttl_secs: u64,
    /// Durability policy for the warm tier's WAL. `always` (default) fsyncs
    /// every group commit — fully durable. `interval_fsync_ms` > 0 switches
    /// to interval fsync: faster, but acked writes within one interval can
    /// be lost on crash. Ignored by non-warm tiers.
    #[serde(default)]
    pub interval_fsync_ms: u64,
    /// Write model. `single-leader` (default): one region writes, others
    /// replicate — strong ordering. `multi-leader`: any region accepts
    /// writes, converging via HLC last-write-wins — eventual consistency,
    /// concurrent same-key writes resolve deterministically (one wins).
    /// Requires a durable tier (warm) and replication enabled.
    #[serde(default = "default_write_mode")]
    pub write_mode: WriteMode,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WriteMode {
    SingleLeader,
    MultiLeader,
}

fn default_write_mode() -> WriteMode {
    WriteMode::SingleLeader
}

fn default_hot_capacity_mb() -> usize {
    256
}
fn default_evict_sample() -> usize {
    8
}
fn default_shard_buckets() -> usize {
    4096
}

impl KeyspaceConfig {
    pub fn default_keyspace() -> Self {
        Self {
            name: "default".to_string(),
            tier: TierName::Warm,
            subscriptions: false,
            replication: false,
            hot_capacity_mb: default_hot_capacity_mb(),
            evict_sample: default_evict_sample(),
            shard_buckets: default_shard_buckets(),
            shard_flush_ms: 0,
            default_ttl_secs: 0,
            interval_fsync_ms: 0,
            write_mode: default_write_mode(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TopicModeConfig {
    Ephemeral,
    Durable,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TopicConfig {
    pub name: String,
    #[serde(default = "default_topic_mode")]
    pub mode: TopicModeConfig,
    #[serde(default = "default_topic_capacity")]
    pub capacity: usize,
}

fn default_topic_mode() -> TopicModeConfig {
    TopicModeConfig::Ephemeral
}
fn default_topic_capacity() -> usize {
    1024
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QueueConfig {
    pub name: String,
    /// Seconds a delivered-but-unacked message waits before redelivery.
    #[serde(default = "default_ack_timeout_secs")]
    pub ack_timeout_secs: u64,
}

fn default_ack_timeout_secs() -> u64 {
    30
}

/// One partitioned event stream (Falcon Event Streaming). Records route to
/// partitions by key hash; each partition is a durable, replayable log with
/// per-consumer-group committed offsets.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StreamConfig {
    pub name: String,
    /// Number of partitions (min 1). More partitions = more parallel ordering
    /// domains, but on a single disk each partition fsyncs independently, so
    /// **more partitions trade single-node write throughput for parallelism**
    /// (like Kafka — you add disks/nodes to scale partitions). Default 1. Use
    /// `interval_fsync_ms` to reclaim throughput at higher partition counts.
    #[serde(default = "default_stream_partitions")]
    pub partitions: usize,
    /// Live broadcast buffer per partition (records a slow live subscriber
    /// can lag before it must replay from the durable log).
    #[serde(default = "default_stream_capacity")]
    pub capacity: usize,
    /// Durability policy. 0 (default) = fsync every append (zero acked-write
    /// loss). > 0 = coalesce fsyncs across all partitions on this interval
    /// (ms): much higher throughput, at a bounded crash-loss window of up to
    /// one interval. Same dial as the warm KV tier's `interval_fsync_ms`.
    #[serde(default)]
    pub interval_fsync_ms: u64,
}

fn default_stream_partitions() -> usize {
    1
}
fn default_stream_capacity() -> usize {
    1024
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub node: NodeConfig,
    #[serde(default)]
    pub auth: AuthConfig,
    #[serde(default)]
    pub http: HttpConfig,
    #[serde(default)]
    pub wire: WireConfig,
    #[serde(default)]
    pub replication: ReplicationConfig,
    #[serde(default)]
    pub subscriptions: SubscriptionsConfig,
    #[serde(default)]
    pub storage: StorageConfig,
    #[serde(default)]
    pub ops: OpsConfig,
    #[serde(default = "default_keyspaces", rename = "keyspace")]
    pub keyspaces: Vec<KeyspaceConfig>,
    #[serde(default, rename = "topic")]
    pub topics: Vec<TopicConfig>,
    #[serde(default, rename = "queue")]
    pub queues: Vec<QueueConfig>,
    #[serde(default, rename = "stream")]
    pub streams: Vec<StreamConfig>,
}

fn default_keyspaces() -> Vec<KeyspaceConfig> {
    vec![KeyspaceConfig::default_keyspace()]
}

impl Default for Config {
    fn default() -> Self {
        Self {
            node: NodeConfig::default(),
            auth: AuthConfig::default(),
            http: HttpConfig::default(),
            wire: WireConfig::default(),
            replication: ReplicationConfig::default(),
            subscriptions: SubscriptionsConfig::default(),
            storage: StorageConfig::default(),
            ops: OpsConfig::default(),
            keyspaces: default_keyspaces(),
            topics: Vec::new(),
            queues: Vec::new(),
            streams: Vec::new(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("keyspace '{0}': hot tier does not support replication")]
    HotTierReplicationConflict(String),
    #[error("replication is enabled with role=follower but no leader_addr is set")]
    MissingLeaderAddr,
    #[error("keyspace '{0}': multi-leader requires the warm tier (durable, HLC-persisted)")]
    MultiLeaderTier(String),
    #[error("keyspace '{0}': multi-leader requires replication = true")]
    MultiLeaderNeedsReplication(String),
    #[error("keyspace '{0}': file-per-key tier cannot be a replication leader (no ordered log)")]
    FilePerKeyReplicationLeader(String),
    #[error("keyspace '{0}': sharded tier cannot be a replication leader (no ordered log)")]
    ShardedReplicationLeader(String),
    #[error("failed to parse config: {0}")]
    Parse(#[from] toml::de::Error),
    #[error("io error reading config: {0}")]
    Io(#[from] std::io::Error),
}

impl Config {
    pub fn from_toml_str(s: &str) -> Result<Self, ConfigError> {
        let cfg: Config = toml::from_str(s)?;
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn from_file(path: &std::path::Path) -> Result<Self, ConfigError> {
        let s = std::fs::read_to_string(path)?;
        Self::from_toml_str(&s)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        for ks in &self.keyspaces {
            if ks.tier == TierName::Hot && ks.replication {
                return Err(ConfigError::HotTierReplicationConflict(ks.name.clone()));
            }
            // File-per-key has no ordered replication log, so it can't be a
            // leader source. Reject replication as leader; it can still be a
            // standalone durable store.
            if ks.tier == TierName::FilePerKey
                && ks.replication
                && self.replication.role == ReplicationRole::Leader
            {
                return Err(ConfigError::FilePerKeyReplicationLeader(ks.name.clone()));
            }
            if ks.tier == TierName::Sharded
                && ks.replication
                && self.replication.role == ReplicationRole::Leader
            {
                return Err(ConfigError::ShardedReplicationLeader(ks.name.clone()));
            }
            if ks.write_mode == WriteMode::MultiLeader {
                // HLC persistence is wired for the warm tier only.
                if ks.tier != TierName::Warm {
                    return Err(ConfigError::MultiLeaderTier(ks.name.clone()));
                }
                if !ks.replication {
                    return Err(ConfigError::MultiLeaderNeedsReplication(ks.name.clone()));
                }
            }
        }
        if self.replication.enabled
            && self.replication.role == ReplicationRole::Follower
            && self.replication.leader_addr.as_deref().unwrap_or("").is_empty()
        {
            return Err(ConfigError::MissingLeaderAddr);
        }
        Ok(())
    }
}
