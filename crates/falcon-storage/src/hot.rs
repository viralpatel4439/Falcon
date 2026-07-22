use crate::engine::{StorageEngine, StorageError, StorageTier};
use crate::lock_table::KeyLockTable;
use async_trait::async_trait;
use dashmap::DashMap;
use falcon_events::{ChangeEvent, Sequence};
use std::sync::atomic::{AtomicU64, Ordering};

/// Pure in-memory tier. No persistence, no replication eligibility.
///
/// Writes to different keys run fully concurrently; writes to the *same*
/// key are serialized (one at a time, in arrival order) via `locks`, so
/// sequence allocation and the map mutation it protects can never be
/// reordered relative to another writer of the same key.
pub struct HotEngine {
    map: DashMap<Vec<u8>, Vec<u8>>,
    sequence: AtomicU64,
    locks: KeyLockTable,
}

impl HotEngine {
    pub fn new() -> Self {
        Self {
            map: DashMap::new(),
            sequence: AtomicU64::new(0),
            locks: KeyLockTable::new(),
        }
    }

    fn next_sequence(&self) -> Sequence {
        self.sequence.fetch_add(1, Ordering::SeqCst) + 1
    }
}

impl Default for HotEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StorageEngine for HotEngine {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        Ok(self.map.get(key).map(|v| v.value().clone()))
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> Result<Sequence, StorageError> {
        let _guard = self.locks.lock(key).await;
        let seq = self.next_sequence();
        self.map.insert(key.to_vec(), value.to_vec());
        Ok(seq)
    }

    async fn delete(&self, key: &[u8]) -> Result<Sequence, StorageError> {
        let _guard = self.locks.lock(key).await;
        let seq = self.next_sequence();
        self.map.remove(key);
        Ok(seq)
    }

    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError> {
        Ok(self
            .map
            .iter()
            .filter(|entry| entry.key().starts_with(prefix))
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect())
    }

    async fn apply_replicated(&self, _event: &ChangeEvent) -> Result<(), StorageError> {
        Err(StorageError::HotTierNotReplicable)
    }

    fn last_applied_sequence(&self) -> Sequence {
        self.sequence.load(Ordering::SeqCst)
    }

    fn tier(&self) -> StorageTier {
        StorageTier::Hot
    }

    fn as_any_arc(self: std::sync::Arc<Self>) -> std::sync::Arc<dyn std::any::Any + Send + Sync> {
        self
    }
}
