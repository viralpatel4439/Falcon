use crate::cold::ColdEngine;
use crate::engine::{StorageEngine, StorageError, StorageTier};
use crate::lock_table::KeyLockTable;
use async_trait::async_trait;
use dashmap::DashMap;
use falcon_events::{ChangeEvent, ChangeValue, Sequence};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

/// Per-key hot-cache entry. `ref_bit` is the CLOCK/second-chance bit: set
/// cheaply (one relaxed store) on every read, cleared by the eviction
/// sweep. Value is `Arc`-wrapped so promotion/eviction never deep-copy.
struct HotEntry {
    value: Arc<Vec<u8>>,
    ref_bit: AtomicBool,
    size: usize, // key.len() + value.len(), for the byte budget
}

/// Observable tiering stats (surfaced in /healthz to make the cost story
/// visible: a high hit-rate on a hot set far smaller than the dataset is
/// the whole pitch).
#[derive(Debug, Default, Clone)]
pub struct TierStats {
    pub hot_hits: u64,
    pub cold_hits: u64,
    pub promotions: u64,
    pub evictions: u64,
    pub hot_keys: u64,
    pub hot_bytes: u64,
}

impl TierStats {
    pub fn hit_rate(&self) -> f64 {
        let total = self.hot_hits + self.cold_hits;
        if total == 0 {
            0.0
        } else {
            self.hot_hits as f64 / total as f64
        }
    }
}

#[derive(Default)]
struct Metrics {
    hot_hits: AtomicU64,
    cold_hits: AtomicU64,
    promotions: AtomicU64,
    evictions: AtomicU64,
}

/// Automatic hot/cold tiering. A bounded in-RAM `DashMap` caches the hot
/// working set in front of a durable sled-backed `ColdEngine` that holds
/// the full dataset on disk — so a keyspace can hold far more than RAM
/// while serving hot keys at RAM latency. Writes are write-through
/// (durable in cold, cached hot), so eviction is a pure RAM drop with no
/// data loss and the victim promotes back on the next read.
pub struct TieredEngine {
    hot: DashMap<Vec<u8>, HotEntry>,
    cold: ColdEngine,
    capacity_bytes: usize,
    approx_bytes: AtomicUsize,
    evict_sample: usize,
    locks: KeyLockTable,
    metrics: Metrics,
}

impl TieredEngine {
    pub fn open(
        cold_path: &Path,
        capacity_bytes: usize,
        evict_sample: usize,
    ) -> Result<Self, StorageError> {
        let cold = ColdEngine::open(cold_path)?;
        Ok(Self {
            hot: DashMap::new(),
            cold,
            capacity_bytes: capacity_bytes.max(1),
            approx_bytes: AtomicUsize::new(0),
            evict_sample: evict_sample.max(1),
            locks: KeyLockTable::new(),
            metrics: Metrics::default(),
        })
    }

    /// Delegates to the inner durable cold store's replication log so a
    /// tiered keyspace can be a replication leader just like a cold one.
    pub fn read_replog_from(&self, from: Sequence) -> Result<Vec<ChangeEvent>, StorageError> {
        self.cold.read_replog_from(from)
    }

    pub fn stats(&self) -> TierStats {
        TierStats {
            hot_hits: self.metrics.hot_hits.load(Ordering::Relaxed),
            cold_hits: self.metrics.cold_hits.load(Ordering::Relaxed),
            promotions: self.metrics.promotions.load(Ordering::Relaxed),
            evictions: self.metrics.evictions.load(Ordering::Relaxed),
            hot_keys: self.hot.len() as u64,
            hot_bytes: self.approx_bytes.load(Ordering::Relaxed) as u64,
        }
    }

    fn hot_insert(&self, key: &[u8], value: Arc<Vec<u8>>) {
        let size = key.len() + value.len();
        let entry = HotEntry {
            value,
            ref_bit: AtomicBool::new(true),
            size,
        };
        if let Some(old) = self.hot.insert(key.to_vec(), entry) {
            self.approx_bytes.fetch_sub(old.size, Ordering::Relaxed);
        }
        self.approx_bytes.fetch_add(size, Ordering::Relaxed);
    }

    fn hot_remove(&self, key: &[u8]) {
        if let Some((_, old)) = self.hot.remove(key) {
            self.approx_bytes.fetch_sub(old.size, Ordering::Relaxed);
        }
    }

    /// CLOCK / second-chance eviction: sweep entries; a set ref_bit gets a
    /// second chance (cleared), a clear ref_bit is evicted. Runs only when
    /// over budget. Victims are already durable in cold (write-through), so
    /// eviction is a pure RAM drop — no flush, no data loss.
    fn maybe_evict(&self) {
        // Bound the work per call so a writer never stalls unboundedly.
        let mut guard = 0usize;
        let max_iterations = self.hot.len() * 2 + 16;
        while self.approx_bytes.load(Ordering::Relaxed) > self.capacity_bytes {
            guard += 1;
            if guard > max_iterations {
                break;
            }
            // Sample up to `evict_sample` keys; evict the first with a clear
            // ref bit, giving a second chance (clearing the bit) to the rest.
            let mut victim: Option<Vec<u8>> = None;
            for entry in self.hot.iter().take(self.evict_sample) {
                if entry.value().ref_bit.swap(false, Ordering::Relaxed) {
                    // was hot recently: cleared, second chance
                    continue;
                }
                victim = Some(entry.key().clone());
                break;
            }
            match victim {
                Some(key) => {
                    self.hot_remove(&key);
                    self.metrics.evictions.fetch_add(1, Ordering::Relaxed);
                }
                None => {
                    // Every sampled entry had its bit set (all just cleared);
                    // next pass will find a victim. Avoid a tight spin.
                    break;
                }
            }
        }
    }
}

#[async_trait]
impl StorageEngine for TieredEngine {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        // Hot fast path: DashMap read + one relaxed store of the ref bit.
        if let Some(entry) = self.hot.get(key) {
            entry.ref_bit.store(true, Ordering::Relaxed);
            self.metrics.hot_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(Some((*entry.value).clone()));
        }

        // Miss: read durable cold store, promote on hit.
        self.metrics.cold_hits.fetch_add(1, Ordering::Relaxed);
        match self.cold.get(key).await? {
            Some(value) => {
                let _guard = self.locks.lock(key).await;
                let arc = Arc::new(value.clone());
                self.hot_insert(key, arc);
                self.metrics.promotions.fetch_add(1, Ordering::Relaxed);
                self.maybe_evict();
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> Result<Sequence, StorageError> {
        let _guard = self.locks.lock(key).await;
        // Write-through: durable in cold first (inherits sled durability +
        // replog + watermark), then cache hot for fast reads.
        let seq = self.cold.put(key, value).await?;
        self.hot_insert(key, Arc::new(value.to_vec()));
        self.maybe_evict();
        Ok(seq)
    }

    async fn delete(&self, key: &[u8]) -> Result<Sequence, StorageError> {
        let _guard = self.locks.lock(key).await;
        let seq = self.cold.delete(key).await?;
        self.hot_remove(key);
        Ok(seq)
    }

    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError> {
        // Cold holds the full dataset, so scan there for completeness.
        self.cold.scan_prefix(prefix).await
    }

    async fn apply_replicated(&self, event: &ChangeEvent) -> Result<(), StorageError> {
        let _guard = self.locks.lock(&event.key).await;
        self.cold.apply_replicated(event).await?;
        // Keep the hot cache coherent with the replicated change.
        match &event.value {
            ChangeValue::Put(value) => {
                if self.hot.contains_key(&event.key) {
                    self.hot_insert(&event.key, Arc::new(value.clone()));
                }
            }
            ChangeValue::Delete => self.hot_remove(&event.key),
        }
        Ok(())
    }

    fn last_applied_sequence(&self) -> Sequence {
        self.cold.last_applied_sequence()
    }

    fn tier(&self) -> StorageTier {
        StorageTier::Tiered
    }

    fn as_any_arc(self: std::sync::Arc<Self>) -> std::sync::Arc<dyn std::any::Any + Send + Sync> {
        self
    }
}
