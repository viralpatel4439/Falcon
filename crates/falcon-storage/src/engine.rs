use async_trait::async_trait;
use falcon_events::{ChangeEvent, Sequence};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StorageTier {
    Hot,
    Warm,
    Cold,
    Tiered,
    Sharded,
}

impl StorageTier {
    pub fn as_str(&self) -> &'static str {
        match self {
            StorageTier::Hot => "hot",
            StorageTier::Warm => "warm",
            StorageTier::Cold => "cold",
            StorageTier::Tiered => "tiered",
            StorageTier::Sharded => "sharded",
        }
    }
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[cfg(feature = "cold")]
    #[error("sled error: {0}")]
    Sled(#[from] sled::Error),
    #[error("corrupt WAL record at offset {0}")]
    CorruptWal(u64),
    #[error("hot tier does not support replication")]
    HotTierNotReplicable,
    /// A remote object-store (e.g. S3) backend error.
    #[error("object store backend error: {0}")]
    Backend(String),
}

/// Uniform contract for all storage tiers. Implementations are responsible
/// for allocating their own monotonically increasing sequence number per
/// write and returning it so the caller (kv-core::Keyspace) can build a
/// ChangeEvent for the event bus / replication log.
#[async_trait]
pub trait StorageEngine: Send + Sync {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError>;
    async fn put(&self, key: &[u8], value: &[u8]) -> Result<Sequence, StorageError>;
    async fn delete(&self, key: &[u8]) -> Result<Sequence, StorageError>;
    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError>;

    /// Apply a replicated change idempotently. Must no-op if
    /// `event.sequence <= last_applied_sequence()`.
    async fn apply_replicated(&self, event: &ChangeEvent) -> Result<(), StorageError>;

    fn last_applied_sequence(&self) -> Sequence;
    fn tier(&self) -> StorageTier;

    /// Approximate on-disk size of this engine's durable state, in bytes.
    /// Drives the `falcon_wal_bytes` gauge and compaction thresholds.
    /// Default 0 for engines with no dedicated durable file (e.g. hot).
    fn durable_bytes(&self) -> u64 {
        0
    }

    /// Persist any buffered/coalesced writes durably right now. Most engines
    /// are already durable per-write and no-op here; the sharded store's
    /// coalesce mode overrides this to flush its dirty buckets. Called on
    /// graceful shutdown.
    async fn flush(&self) -> Result<(), StorageError> {
        Ok(())
    }

    /// Compact durable state in place (e.g. rewrite the WAL as a snapshot of
    /// live keys only, dropping superseded/tombstoned records) so disk usage
    /// and restart-replay time stay bounded. Returns whether a compaction
    /// actually ran. Default: not supported, returns `false`.
    async fn compact(&self) -> Result<bool, StorageError> {
        Ok(false)
    }

    /// Allows replication to downcast an `Arc<dyn StorageEngine>` back to
    /// its concrete engine type in order to read its durable log (see
    /// `kv-replication::ReplicationLogReader`). Not used on hot-path CRUD.
    fn as_any_arc(self: std::sync::Arc<Self>) -> std::sync::Arc<dyn std::any::Any + Send + Sync>;
}
