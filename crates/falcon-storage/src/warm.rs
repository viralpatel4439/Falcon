use crate::engine::{StorageEngine, StorageError, StorageTier};
use crate::lock_table::KeyLockTable;
use crate::wal::{frame_record, frame_record_hlc, SparseIndex, Wal, WalOp};
use crate::wal_writer::WalWriter;
use async_trait::async_trait;
use dashmap::DashMap;
use falcon_events::{ChangeEvent, ChangeValue, Hlc, Sequence, Timestamp};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

/// In-memory index backed by an append-only, group-committed WAL file for
/// durability. The WAL doubles as the replication log for this tier.
///
/// Writes to different keys run fully concurrently; writes to the *same*
/// key are serialized via `locks` so sequence allocation, the WAL append,
/// and the in-memory map update for that key always happen as one atomic
/// step relative to other writers of the same key. Durability is provided
/// by `WalWriter`, a background task that owns the WAL file exclusively
/// and batches concurrently-submitted writes into a single fsync (group
/// commit) instead of one fsync per write — see `wal_writer.rs`.
pub struct WarmEngine {
    map: DashMap<Vec<u8>, Vec<u8>>,
    /// The WAL writer and its sparse index live behind an `RwLock` so
    /// compaction can atomically swap in a freshly-rewritten WAL. Normal
    /// writes take a *read* guard (they still batch/commit concurrently with
    /// each other via group commit); compaction takes the *write* guard,
    /// briefly excluding writers while it rewrites the log. Reads (`get`)
    /// never touch this lock.
    wal: tokio::sync::RwLock<WalSide>,
    /// Shared sparse index. Lives outside `WalSide` so the sync
    /// `read_replog_from` (replication catch-up) can consult it without the
    /// async WAL lock. Compaction replaces its *contents* in place under the
    /// mutex rather than swapping the Arc.
    sparse_index: std::sync::Arc<Mutex<SparseIndex>>,
    wal_path: PathBuf,
    policy: crate::wal_writer::FsyncPolicy,
    sequence: AtomicU64,
    /// Serializes `next_sequence()` + WAL enqueue so the WAL file order always
    /// matches sequence order. Without this, two writes to *different* keys can
    /// allocate seq N and N+1 but enqueue them out of order, leaving the
    /// durable replication log unordered — which broke a follower's sparse-
    /// index catch-up under concurrent writes. Held only across enqueue (a
    /// non-blocking channel send), never across the fsync await, so group
    /// commit still batches fully.
    seq_order: tokio::sync::Mutex<()>,
    locks: KeyLockTable,
    /// Per-key HLC of the current value, for multi-region last-write-wins.
    /// Rebuilt from the WAL on open (durable), so LWW ordering survives
    /// restarts. Only consulted on the multi-leader write path.
    hlc_index: DashMap<Vec<u8>, Hlc>,
}

/// The mutable WAL writer, swapped wholesale by compaction.
struct WalSide {
    writer: WalWriter,
}

impl WarmEngine {
    pub fn open(path: &Path) -> Result<Self, StorageError> {
        Self::open_with_policy(path, crate::wal_writer::FsyncPolicy::Always)
    }

    pub fn open_with_policy(
        path: &Path,
        policy: crate::wal_writer::FsyncPolicy,
    ) -> Result<Self, StorageError> {
        let (records, index, end_offset) = Wal::replay_with_index(path)?;
        let map = DashMap::new();
        let hlc_index = DashMap::new();
        let mut max_seq = 0;
        for record in &records {
            max_seq = max_seq.max(record.sequence);
            match &record.op {
                crate::wal::WalOp::Put(value) => {
                    map.insert(record.key.clone(), value.clone());
                }
                crate::wal::WalOp::Delete => {
                    map.remove(&record.key);
                }
            }
            // Rebuild the per-key HLC index from the durable log so
            // last-write-wins ordering survives restarts.
            if record.hlc != Hlc::zero() {
                hlc_index.insert(record.key.clone(), record.hlc.clone());
            }
        }
        let file = Wal::open_file(path)?;
        let (wal_writer, sparse_index) =
            WalWriter::spawn_with_policy(file, end_offset, index, policy);
        Ok(Self {
            map,
            wal: tokio::sync::RwLock::new(WalSide { writer: wal_writer }),
            sparse_index,
            wal_path: path.to_path_buf(),
            policy,
            sequence: AtomicU64::new(max_seq),
            seq_order: tokio::sync::Mutex::new(()),
            locks: KeyLockTable::new(),
            hlc_index,
        })
    }

    fn next_sequence(&self) -> Sequence {
        self.sequence.fetch_add(1, Ordering::SeqCst) + 1
    }

    fn wal_writer_gone() -> StorageError {
        StorageError::Io(std::io::Error::new(
            std::io::ErrorKind::BrokenPipe,
            "wal writer task is no longer running",
        ))
    }

    /// Allocate the next sequence, frame the record, and enqueue it to the WAL
    /// **atomically** (under `seq_order`) so file order == sequence order, then
    /// await durability *outside* the order lock (so group commit still
    /// batches). `frame` is given the freshly-allocated sequence and returns
    /// the bytes to append. Returns the assigned sequence once durable. Used by
    /// the single-leader write path, which feeds the ordered replication log.
    async fn seq_and_submit(
        &self,
        frame: impl FnOnce(Sequence) -> (Vec<u8>, Timestamp),
    ) -> Result<Sequence, StorageError> {
        // Read guard: a concurrent compaction (write guard) can't swap the WAL
        // mid-append. Order lock: seq allocation + enqueue happen together.
        let wal = self.wal.read().await;
        let (seq, pending) = {
            let _order = self.seq_order.lock().await;
            let seq = self.next_sequence();
            let (framed, _ts) = frame(seq);
            let pending = wal.writer.enqueue(framed)?;
            (seq, pending)
        };
        drop(wal);
        pending.await.map_err(|_| Self::wal_writer_gone())??;
        Ok(seq)
    }

    /// Submit a pre-framed record whose sequence was already allocated (the
    /// multi-leader LWW paths and replicated-apply, which carry an externally
    /// assigned sequence/HLC). Ordered against the single-leader path via the
    /// same `seq_order` lock so file order stays consistent.
    async fn submit(&self, ts: Timestamp, framed: Vec<u8>) -> Result<Timestamp, StorageError> {
        let wal = self.wal.read().await;
        let pending = {
            let _order = self.seq_order.lock().await;
            wal.writer.enqueue(framed)?
        };
        drop(wal);
        pending.await.map_err(|_| Self::wal_writer_gone())??;
        Ok(ts)
    }

    pub fn wal_path(&self) -> &Path {
        &self.wal_path
    }

    /// Multi-leader local write: stamp the value with `hlc`, persist it
    /// (HLC in the WAL), and record it as the current per-key HLC. Since a
    /// local write always uses a freshly-minted HLC (greater than anything
    /// this region has produced or observed), it always wins locally.
    pub async fn put_lww(&self, key: &[u8], value: &[u8], hlc: &Hlc) -> Result<Sequence, StorageError> {
        let _guard = self.locks.lock(key).await;
        let seq = self.next_sequence();
        let (framed, _ts) = frame_record_hlc(seq, key, &WalOp::Put(value.to_vec()), hlc);
        self.submit(0, framed).await?;
        self.map.insert(key.to_vec(), value.to_vec());
        self.hlc_index.insert(key.to_vec(), hlc.clone());
        Ok(seq)
    }

    pub async fn delete_lww(&self, key: &[u8], hlc: &Hlc) -> Result<Sequence, StorageError> {
        let _guard = self.locks.lock(key).await;
        let seq = self.next_sequence();
        let (framed, _ts) = frame_record_hlc(seq, key, &WalOp::Delete, hlc);
        self.submit(0, framed).await?;
        self.map.remove(key);
        // Keep the tombstone's HLC so a late, older put loses.
        self.hlc_index.insert(key.to_vec(), hlc.clone());
        Ok(seq)
    }

    /// Apply a replicated change under last-write-wins: apply it only if its
    /// HLC is strictly greater than the HLC currently stored for the key.
    /// Idempotent and commutative — replaying or reordering converges to the
    /// same state. Returns whether it was applied (false = older/duplicate).
    pub async fn apply_lww(&self, event: &ChangeEvent) -> Result<bool, StorageError> {
        let _guard = self.locks.lock(&event.key).await;
        if let Some(stored) = self.hlc_index.get(&event.key) {
            if event.hlc <= *stored {
                return Ok(false); // we already hold an equal-or-newer write
            }
        }
        let seq = self.next_sequence();
        match &event.value {
            ChangeValue::Put(value) => {
                let (framed, _ts) =
                    frame_record_hlc(seq, &event.key, &WalOp::Put(value.clone()), &event.hlc);
                self.submit(0, framed).await?;
                self.map.insert(event.key.clone(), value.clone());
            }
            ChangeValue::Delete => {
                let (framed, _ts) =
                    frame_record_hlc(seq, &event.key, &WalOp::Delete, &event.hlc);
                self.submit(0, framed).await?;
                self.map.remove(&event.key);
            }
        }
        self.hlc_index.insert(event.key.clone(), event.hlc.clone());
        Ok(true)
    }

    /// Current stored HLC for a key (multi-leader introspection/testing).
    pub fn stored_hlc(&self, key: &[u8]) -> Option<Hlc> {
        self.hlc_index.get(key).map(|h| h.clone())
    }

    /// Read WAL entries with sequence > `from`, in order. Used by
    /// replication to serve a follower's catch-up request. Uses the
    /// sparse offset index to seek near `from` instead of re-scanning the
    /// WAL from byte 0; falls back to a full scan if the index isn't
    /// trusted (e.g. after an unexpected inconsistency) or doesn't cover
    /// `from` yet.
    pub fn read_replog_from(&self, from: Sequence) -> Result<Vec<ChangeEvent>, StorageError> {
        let seek_offset = self
            .sparse_index
            .lock()
            .expect("sparse index mutex poisoned")
            .floor(from);

        let records = match seek_offset {
            Some(offset) => match Wal::replay_from_offset(&self.wal_path, offset) {
                Ok(records) => records,
                Err(_) => {
                    self.sparse_index
                        .lock()
                        .expect("sparse index mutex poisoned")
                        .invalidate();
                    Wal::replay(&self.wal_path)?
                }
            },
            None => Wal::replay(&self.wal_path)?,
        };

        Ok(records
            .into_iter()
            .filter(|r| r.sequence > from)
            .map(|r| ChangeEvent {
                keyspace: String::new(), // filled in by caller, which knows the keyspace name
                key: r.key,
                value: match r.op {
                    WalOp::Put(v) => ChangeValue::Put(v),
                    WalOp::Delete => ChangeValue::Delete,
                },
                sequence: r.sequence,
                timestamp: r.timestamp,
                origin_region: r.hlc.region.clone(),
                hlc: r.hlc,
            })
            .collect())
    }

    /// Rewrite the WAL as a compact snapshot of only the live keys, dropping
    /// every superseded value and tombstone. Bounds on-disk size and restart
    /// replay time. Takes the exclusive WAL guard so no write interleaves.
    ///
    /// **Renumbers sequences** `1..=N` over the live keys, so it must only be
    /// run when this engine is NOT a replication source (a leader's followers
    /// track sequence watermarks). The caller (Node) enforces that via config.
    async fn compact_inner(&self) -> Result<bool, StorageError> {
        // Exclusive: block all writers for the duration of the swap.
        let mut wal_guard = self.wal.write().await;

        // Snapshot live keys in a deterministic order for reproducibility.
        let mut live: Vec<(Vec<u8>, Vec<u8>)> = self
            .map
            .iter()
            .map(|e| (e.key().clone(), e.value().clone()))
            .collect();
        live.sort_by(|a, b| a.0.cmp(&b.0));

        // Build a fresh WAL at a temp path: one PUT per live key, seq 1..=N,
        // carrying each key's current HLC so LWW ordering survives compaction.
        let tmp_path = self.wal_path.with_extension("wal.compact");
        let mut fresh_index = SparseIndex::new();
        {
            use std::io::Write;
            let mut tmp = std::fs::File::create(&tmp_path)?;
            let mut offset = 0u64;
            let mut seq = 0u64;
            for (key, value) in &live {
                seq += 1;
                let hlc = self.hlc_index.get(key).map(|h| h.clone()).unwrap_or_else(Hlc::zero);
                let (framed, _ts) = frame_record_hlc(seq, key, &WalOp::Put(value.clone()), &hlc);
                tmp.write_all(&framed)?;
                fresh_index.record(seq, offset);
                offset += framed.len() as u64;
            }
            tmp.sync_all()?;
            // Atomic replace: a crash before this leaves the old WAL intact;
            // after it, the new one is fully durable.
            std::fs::rename(&tmp_path, &self.wal_path)?;
            let new_seq = seq;

            // Spawn a fresh writer over the compacted file and swap it in.
            let file = Wal::open_file(&self.wal_path)?;
            let (writer, new_sparse) =
                WalWriter::spawn_with_policy(file, offset, fresh_index, self.policy);
            // Replace the shared sparse index's contents in place so
            // `read_replog_from` (which holds the same Arc) sees the new one.
            {
                let mut si = self.sparse_index.lock().expect("sparse index mutex poisoned");
                *si = std::mem::take(&mut *new_sparse.lock().expect("sparse index mutex poisoned"));
            }
            wal_guard.writer = writer;
            self.sequence.store(new_seq, Ordering::SeqCst);
        }
        Ok(true)
    }
}

#[async_trait]
impl StorageEngine for WarmEngine {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        Ok(self.map.get(key).map(|v| v.value().clone()))
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> Result<Sequence, StorageError> {
        let _guard = self.locks.lock(key).await;
        // Sequence allocation + WAL enqueue happen atomically (file order ==
        // sequence order) so the replication log a follower reads is ordered.
        let seq = self
            .seq_and_submit(|seq| frame_record(seq, key, &WalOp::Put(value.to_vec())))
            .await?;
        self.map.insert(key.to_vec(), value.to_vec());
        Ok(seq)
    }

    async fn delete(&self, key: &[u8]) -> Result<Sequence, StorageError> {
        let _guard = self.locks.lock(key).await;
        let seq = self
            .seq_and_submit(|seq| frame_record(seq, key, &WalOp::Delete))
            .await?;
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

    async fn apply_replicated(&self, event: &ChangeEvent) -> Result<(), StorageError> {
        let _guard = self.locks.lock(&event.key).await;
        let last = self.sequence.load(Ordering::SeqCst);
        if event.sequence <= last {
            return Ok(()); // already applied, idempotent no-op
        }
        match &event.value {
            ChangeValue::Put(value) => {
                let (framed, ts) = frame_record(event.sequence, &event.key, &WalOp::Put(value.clone()));
                self.submit(ts, framed).await?;
                self.map.insert(event.key.clone(), value.clone());
            }
            ChangeValue::Delete => {
                let (framed, ts) = frame_record(event.sequence, &event.key, &WalOp::Delete);
                self.submit(ts, framed).await?;
                self.map.remove(&event.key);
            }
        }
        self.sequence.store(event.sequence, Ordering::SeqCst);
        Ok(())
    }

    fn last_applied_sequence(&self) -> Sequence {
        self.sequence.load(Ordering::SeqCst)
    }

    fn tier(&self) -> StorageTier {
        StorageTier::Warm
    }

    fn durable_bytes(&self) -> u64 {
        std::fs::metadata(&self.wal_path).map(|m| m.len()).unwrap_or(0)
    }

    async fn compact(&self) -> Result<bool, StorageError> {
        self.compact_inner().await
    }

    fn as_any_arc(self: std::sync::Arc<Self>) -> std::sync::Arc<dyn std::any::Any + Send + Sync> {
        self
    }
}
