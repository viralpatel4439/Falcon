//! A storage engine that groups many keys into a small, fixed number of
//! **buckets** (shards), storing each bucket as a single object in a backing
//! [`ObjectStore`] (a local directory today, a third-party bucket via the same
//! trait tomorrow). This is Falcon's object-store tier, and it is designed to be
//! cheap on request-billed stores: an S3-compatible store bills *per request*,
//! so a naive "one object per key" layout would mean one billed PUT/GET per key.
//! By hashing keys into `N` buckets and persisting one object per bucket,
//! millions of keys cost `N` objects and `N` writes per flush — not millions —
//! and it behaves identically on local disk and remote buckets.
//!
//! ## Strategy (shard / hash / bucket)
//!
//! - **Hash**: keys are hashed with FNV-1a (stable across processes and
//!   platforms — unlike `DefaultHasher`, whose output is not guaranteed
//!   stable) so a key always lands in the same bucket across restarts.
//! - **Bucket**: `bucket = hash(key) & (N - 1)` with `N` a power of two, so
//!   the mapping is a single mask (no modulo) and the distribution is
//!   uniform. Each bucket is one serialized `{key -> value}` object named
//!   `bucket_<i>` in the backing store.
//! - **Shard**: buckets are grouped into independently-locked shards so
//!   writes to different buckets proceed in parallel; same-bucket writes are
//!   serialized (and coalesced) into a single object write.
//!
//! ## Read/write path
//!
//! An in-memory index (one `HashMap` per bucket, behind an `RwLock`) serves
//! reads in O(1) with **zero** object-store round-trips once a bucket is
//! resident. On a cold bucket, the object is fetched once and decoded into
//! the index; subsequent reads are pure memory. Writes update the index and
//! re-serialize the whole bucket object exactly once per flush — with
//! `FlushPolicy::Sync` that is per-write (durable), with
//! `FlushPolicy::Coalesce` a background task batches dirty buckets so a burst
//! of writes to hot buckets collapses into far fewer object writes.

use crate::engine::{StorageEngine, StorageError, StorageTier};
use crate::object_store::{LocalDirStore, ObjectStore};
use async_trait::async_trait;
use falcon_events::{ChangeEvent, ChangeValue, Sequence};
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

/// When (and how) a mutated bucket is persisted to the backing object store.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FlushPolicy {
    /// Persist the affected bucket object on every write before the write is
    /// acked. Fully durable; each write is one object PUT of the bucket.
    Sync,
    /// Mark the bucket dirty and let a background flusher persist it every
    /// `interval_ms`. A burst of writes to the same bucket coalesces into a
    /// single object PUT — far cheaper on request-billed stores — at the cost
    /// of a bounded crash-loss window (writes since the last flush).
    Coalesce { interval_ms: u64 },
}

/// A single bucket: the resident portion of the index plus its dirty flag.
struct Bucket {
    /// Decoded key -> value map. `None` until first loaded from the store.
    resident: RwLock<Option<HashMap<Vec<u8>, Vec<u8>>>>,
    /// Serializes load/flush of this bucket so we never race two writers
    /// re-encoding the same object.
    io: Mutex<()>,
    dirty: AtomicBool,
}

impl Bucket {
    fn new() -> Self {
        Self {
            resident: RwLock::new(None),
            io: Mutex::new(()),
            dirty: AtomicBool::new(false),
        }
    }
}

pub struct ShardedObjectStore {
    store: Arc<dyn ObjectStore>,
    buckets: Vec<Bucket>,
    mask: u64,
    policy: FlushPolicy,
    sequence: AtomicU64,
    /// Set on `spawn_flusher`; lets the background task see the same buckets.
    shutdown: Arc<AtomicBool>,
}

impl ShardedObjectStore {
    /// Open a sharded store over a local directory with `bucket_count`
    /// buckets (rounded up to the next power of two, min 1). Use
    /// [`Self::with_store`] to back it with a third-party object store.
    pub fn open_local(
        root: &Path,
        bucket_count: usize,
        policy: FlushPolicy,
    ) -> Result<Arc<Self>, StorageError> {
        let store = Arc::new(LocalDirStore::open(root)?);
        Self::with_store(store, bucket_count, policy)
    }

    /// Open over any object-store backend (the seam for third-party stores).
    /// Recovers the sequence high-water mark by scanning resident bucket
    /// counts lazily; sequence starts at 0 and only needs to be monotonic.
    pub fn with_store(
        store: Arc<dyn ObjectStore>,
        bucket_count: usize,
        policy: FlushPolicy,
    ) -> Result<Arc<Self>, StorageError> {
        let n = bucket_count.max(1).next_power_of_two();
        let buckets = (0..n).map(|_| Bucket::new()).collect();
        let me = Arc::new(Self {
            store,
            buckets,
            mask: (n as u64) - 1,
            policy,
            sequence: AtomicU64::new(0),
            shutdown: Arc::new(AtomicBool::new(false)),
        });
        if let FlushPolicy::Coalesce { interval_ms } = policy {
            me.spawn_flusher(interval_ms.max(1));
        }
        Ok(me)
    }

    pub fn bucket_count(&self) -> usize {
        self.buckets.len()
    }

    pub fn backend_description(&self) -> String {
        format!("sharded[{}]:{}", self.buckets.len(), self.store.describe())
    }

    fn next_sequence(&self) -> Sequence {
        self.sequence.fetch_add(1, Ordering::SeqCst) + 1
    }

    fn bucket_index(&self, key: &[u8]) -> usize {
        (fnv1a(key) & self.mask) as usize
    }

    fn object_name(index: usize) -> Vec<u8> {
        format!("bucket_{index}").into_bytes()
    }

    /// Ensure the bucket's index map is resident (loading it from the object
    /// store on first touch), holding the bucket's io lock for the load.
    async fn ensure_resident(&self, index: usize) -> Result<(), StorageError> {
        {
            let guard = self.buckets[index].resident.read().await;
            if guard.is_some() {
                return Ok(());
            }
        }
        let _io = self.buckets[index].io.lock().await;
        // Re-check: another task may have loaded it while we waited.
        {
            let guard = self.buckets[index].resident.read().await;
            if guard.is_some() {
                return Ok(());
            }
        }
        let raw = self.store.get(&Self::object_name(index)).await?;
        let map = match raw {
            Some(bytes) => decode_bucket(&bytes)?,
            None => HashMap::new(),
        };
        *self.buckets[index].resident.write().await = Some(map);
        Ok(())
    }

    /// Serialize the bucket's current map and write it as one object. Clears
    /// the dirty flag on success.
    async fn flush_bucket(&self, index: usize) -> Result<(), StorageError> {
        let _io = self.buckets[index].io.lock().await;
        // Clear the dirty flag BEFORE snapshotting the map. A concurrent write
        // that lands after this point re-sets dirty and is caught by the next
        // flush; one that landed before is already in the snapshot. Clearing
        // after the snapshot would let such a write be silently dropped.
        self.buckets[index].dirty.store(false, Ordering::Release);
        let bytes = {
            let guard = self.buckets[index].resident.read().await;
            match guard.as_ref() {
                Some(map) => encode_bucket(map),
                None => return Ok(()), // nothing resident, nothing to flush
            }
        };
        if let Err(e) = self.store.put(&Self::object_name(index), &bytes).await {
            // Persist failed: restore the dirty flag so a later flush retries.
            self.buckets[index].dirty.store(true, Ordering::Release);
            return Err(e);
        }
        Ok(())
    }

    /// Persist every *dirty* bucket — the background flusher's coalescing
    /// path. A bucket a concurrent flush is mid-writing (dirty already
    /// cleared) is skipped here but re-dirtied by the write that raced it, so
    /// a later tick catches it.
    pub async fn flush_all(&self) -> Result<(), StorageError> {
        for i in 0..self.buckets.len() {
            if self.buckets[i].dirty.load(Ordering::Acquire) {
                self.flush_bucket(i).await?;
            }
        }
        Ok(())
    }

    /// Persist *every* bucket unconditionally, ignoring the dirty flag. Use
    /// this as the authoritative final flush before dropping the store (e.g.
    /// shutdown): because `flush_bucket` snapshots under the per-bucket io
    /// lock, this waits out any in-flight background flush and writes the
    /// newest snapshot last, so no stale in-flight write can win the race.
    pub async fn flush_all_force(&self) -> Result<(), StorageError> {
        for i in 0..self.buckets.len() {
            self.flush_bucket(i).await?;
        }
        Ok(())
    }

    fn spawn_flusher(self: &Arc<Self>, interval_ms: u64) {
        // The task holds a WEAK ref, not a strong one: otherwise it would keep
        // the store alive forever and `Drop` (hence shutdown) would never fire.
        // When every external `Arc` is dropped, `upgrade()` returns `None` and
        // the task exits. Callers that need a guaranteed final persist call
        // `flush_all().await` before dropping their handle.
        let weak = Arc::downgrade(self);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_millis(interval_ms));
            loop {
                tick.tick().await;
                let Some(this) = weak.upgrade() else { break };
                if this.shutdown.load(Ordering::Acquire) {
                    break;
                }
                if let Err(e) = this.flush_all().await {
                    tracing::warn!("sharded store background flush failed: {e}");
                }
            }
        });
    }

    /// After a write to `index`, either flush now (Sync) or mark dirty
    /// (Coalesce) for the background flusher.
    async fn on_write(&self, index: usize) -> Result<(), StorageError> {
        self.buckets[index].dirty.store(true, Ordering::Release);
        if self.policy == FlushPolicy::Sync {
            self.flush_bucket(index).await?;
        }
        Ok(())
    }
}

impl Drop for ShardedObjectStore {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
    }
}

#[async_trait]
impl StorageEngine for ShardedObjectStore {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        let index = self.bucket_index(key);
        self.ensure_resident(index).await?;
        let guard = self.buckets[index].resident.read().await;
        Ok(guard
            .as_ref()
            .and_then(|m| m.get(key).cloned()))
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> Result<Sequence, StorageError> {
        let index = self.bucket_index(key);
        self.ensure_resident(index).await?;
        let seq = self.next_sequence();
        {
            let mut guard = self.buckets[index].resident.write().await;
            guard
                .get_or_insert_with(HashMap::new)
                .insert(key.to_vec(), value.to_vec());
        }
        self.on_write(index).await?;
        Ok(seq)
    }

    async fn delete(&self, key: &[u8]) -> Result<Sequence, StorageError> {
        let index = self.bucket_index(key);
        self.ensure_resident(index).await?;
        let seq = self.next_sequence();
        let removed = {
            let mut guard = self.buckets[index].resident.write().await;
            guard
                .as_mut()
                .map(|m| m.remove(key).is_some())
                .unwrap_or(false)
        };
        if removed {
            self.on_write(index).await?;
        }
        Ok(seq)
    }

    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError> {
        // A prefix can span every bucket (hashing destroys key locality), so
        // a prefix scan must load and sweep all buckets. This is the tradeoff
        // for cheap point-access: sharding optimizes GET/PUT/DEL, not range.
        let mut out = Vec::new();
        for i in 0..self.buckets.len() {
            self.ensure_resident(i).await?;
            let guard = self.buckets[i].resident.read().await;
            if let Some(map) = guard.as_ref() {
                for (k, v) in map {
                    if k.starts_with(prefix) {
                        out.push((k.clone(), v.clone()));
                    }
                }
            }
        }
        Ok(out)
    }

    async fn apply_replicated(&self, event: &ChangeEvent) -> Result<(), StorageError> {
        match &event.value {
            ChangeValue::Put(value) => {
                self.put(&event.key, value).await?;
            }
            ChangeValue::Delete => {
                self.delete(&event.key).await?;
            }
        }
        self.sequence.fetch_max(event.sequence, Ordering::SeqCst);
        Ok(())
    }

    fn last_applied_sequence(&self) -> Sequence {
        self.sequence.load(Ordering::SeqCst)
    }

    fn tier(&self) -> StorageTier {
        StorageTier::Sharded
    }

    fn durable_bytes(&self) -> u64 {
        self.store.approx_size_bytes()
    }

    async fn flush(&self) -> Result<(), StorageError> {
        // Authoritative final flush: persist every bucket, ignoring the dirty
        // flag, so a coalesce-mode store loses nothing on graceful shutdown.
        self.flush_all_force().await
    }

    fn as_any_arc(self: std::sync::Arc<Self>) -> std::sync::Arc<dyn std::any::Any + Send + Sync> {
        self
    }
}

/// FNV-1a 64-bit — a fast, allocation-free, deterministic hash. Chosen over
/// `DefaultHasher` because its output must be *stable across processes* so a
/// key maps to the same bucket after a restart.
fn fnv1a(bytes: &[u8]) -> u64 {
    const OFFSET: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x100000001b3;
    let mut hash = OFFSET;
    for &b in bytes {
        hash ^= b as u64;
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

/// Bucket object encoding: `[count:u32]` then, per entry,
/// `[klen:u32][key][vlen:u32][value]`, all big-endian. Compact, self-framing,
/// and independent of any external serde format.
fn encode_bucket(map: &HashMap<Vec<u8>, Vec<u8>>) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + map.len() * 16);
    out.extend_from_slice(&(map.len() as u32).to_be_bytes());
    for (k, v) in map {
        out.extend_from_slice(&(k.len() as u32).to_be_bytes());
        out.extend_from_slice(k);
        out.extend_from_slice(&(v.len() as u32).to_be_bytes());
        out.extend_from_slice(v);
    }
    out
}

fn decode_bucket(bytes: &[u8]) -> Result<HashMap<Vec<u8>, Vec<u8>>, StorageError> {
    let mut map = HashMap::new();
    let mut i = 0usize;
    let read_u32 = |b: &[u8], i: usize| -> Option<usize> {
        let arr: [u8; 4] = b.get(i..i + 4)?.try_into().ok()?;
        Some(u32::from_be_bytes(arr) as usize)
    };
    let count = read_u32(bytes, i).ok_or(StorageError::CorruptWal(0))?;
    i += 4;
    for _ in 0..count {
        let klen = read_u32(bytes, i).ok_or(StorageError::CorruptWal(i as u64))?;
        i += 4;
        let key = bytes.get(i..i + klen).ok_or(StorageError::CorruptWal(i as u64))?.to_vec();
        i += klen;
        let vlen = read_u32(bytes, i).ok_or(StorageError::CorruptWal(i as u64))?;
        i += 4;
        let val = bytes.get(i..i + vlen).ok_or(StorageError::CorruptWal(i as u64))?.to_vec();
        i += vlen;
        map.insert(key, val);
    }
    Ok(map)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(map: HashMap<Vec<u8>, Vec<u8>>) {
        let enc = encode_bucket(&map);
        let dec = decode_bucket(&enc).expect("decode");
        assert_eq!(map, dec);
    }

    #[test]
    fn bucket_codec_round_trips() {
        let mut m = HashMap::new();
        m.insert(b"user:1".to_vec(), b"alice".to_vec());
        m.insert(Vec::new(), b"empty-key".to_vec());
        m.insert(b"k".to_vec(), Vec::new());
        m.insert(vec![0, 255, 1, 254], vec![9, 8, 7]);
        roundtrip(m);
        roundtrip(HashMap::new());
    }

    #[test]
    fn hash_is_stable_and_spreads() {
        // Deterministic across calls (the property restarts rely on).
        assert_eq!(fnv1a(b"hello"), fnv1a(b"hello"));
        // Reasonable spread across 256 buckets for 10k keys.
        let mask = 255u64;
        let mut counts = [0u32; 256];
        for i in 0..10_000u32 {
            let k = format!("key:{i}");
            counts[(fnv1a(k.as_bytes()) & mask) as usize] += 1;
        }
        let max = *counts.iter().max().unwrap();
        let min = *counts.iter().min().unwrap();
        // ~39 per bucket expected; assert no pathological skew.
        assert!(max < 120, "bucket skew too high: max={max}");
        assert!(min > 5, "bucket underfill: min={min}");
    }
}
