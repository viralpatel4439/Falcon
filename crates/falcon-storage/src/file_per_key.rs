//! A storage engine that keeps each value as an independent object via an
//! `ObjectStore` backend (local folder today, third-party bucket via the
//! same trait later). Simpler and more portable than the WAL: every key is
//! a standalone durable object. Trades batched-fsync throughput for
//! maintainability and remote-storage friendliness.

use crate::engine::{StorageEngine, StorageError, StorageTier};
use crate::lock_table::KeyLockTable;
use crate::object_store::{LocalDirStore, ObjectStore};
use async_trait::async_trait;
use falcon_events::{ChangeEvent, ChangeValue, Sequence};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub struct FilePerKeyEngine {
    store: Arc<dyn ObjectStore>,
    sequence: AtomicU64,
    locks: KeyLockTable,
}

impl FilePerKeyEngine {
    /// Open a file-per-key engine backed by a local directory (default
    /// backend). Recovers the sequence high-water mark from the existing
    /// objects so `last_applied_sequence` is stable across restarts.
    pub fn open_local(root: &Path) -> Result<Self, StorageError> {
        let store = Arc::new(LocalDirStore::open(root)?);
        Ok(Self::with_store(store))
    }

    /// Open with any object-store backend (the seam for third-party stores).
    pub fn with_store(store: Arc<dyn ObjectStore>) -> Self {
        Self {
            store,
            sequence: AtomicU64::new(0),
            locks: KeyLockTable::new(),
        }
    }

    pub fn backend_description(&self) -> String {
        self.store.describe()
    }

    fn next_sequence(&self) -> Sequence {
        self.sequence.fetch_add(1, Ordering::SeqCst) + 1
    }
}

#[async_trait]
impl StorageEngine for FilePerKeyEngine {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        self.store.get(key).await
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> Result<Sequence, StorageError> {
        let _guard = self.locks.lock(key).await;
        let seq = self.next_sequence();
        self.store.put(key, value).await?;
        Ok(seq)
    }

    async fn delete(&self, key: &[u8]) -> Result<Sequence, StorageError> {
        let _guard = self.locks.lock(key).await;
        let seq = self.next_sequence();
        self.store.delete(key).await?;
        Ok(seq)
    }

    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError> {
        self.store.list_prefix(prefix).await
    }

    async fn apply_replicated(&self, event: &ChangeEvent) -> Result<(), StorageError> {
        let _guard = self.locks.lock(&event.key).await;
        match &event.value {
            ChangeValue::Put(value) => self.store.put(&event.key, value).await?,
            ChangeValue::Delete => self.store.delete(&event.key).await?,
        }
        self.sequence
            .fetch_max(event.sequence, Ordering::SeqCst);
        Ok(())
    }

    fn last_applied_sequence(&self) -> Sequence {
        self.sequence.load(Ordering::SeqCst)
    }

    fn tier(&self) -> StorageTier {
        StorageTier::FilePerKey
    }

    fn as_any_arc(self: std::sync::Arc<Self>) -> std::sync::Arc<dyn std::any::Any + Send + Sync> {
        self
    }
}
