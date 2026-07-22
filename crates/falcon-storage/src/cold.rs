use crate::engine::{StorageEngine, StorageError, StorageTier};
use crate::lock_table::KeyLockTable;
use async_trait::async_trait;
use falcon_events::{now_millis, ChangeEvent, ChangeValue, Hlc, Sequence};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

const META_LAST_SEQ_KEY: &[u8] = b"last_seq";

/// Disk-backed tier using sled. Durable, handles its own compaction.
/// An auxiliary `__replog` tree stores serialized change events by
/// sequence number so followers can catch up; `__meta` tracks the
/// last-assigned sequence.
///
/// Writes to different keys run fully concurrently; writes to the *same*
/// key are serialized via `locks`, so sequence allocation and the sled
/// mutation it protects can never be reordered relative to another writer
/// of the same key.
pub struct ColdEngine {
    db: sled::Db,
    data: sled::Tree,
    replog: sled::Tree,
    meta: sled::Tree,
    sequence: AtomicU64,
    locks: KeyLockTable,
}

impl ColdEngine {
    pub fn open(path: &Path) -> Result<Self, StorageError> {
        let db = sled::open(path)?;
        let data = db.open_tree("data")?;
        let replog = db.open_tree("__replog")?;
        let meta = db.open_tree("__meta")?;

        let last_seq = meta
            .get(META_LAST_SEQ_KEY)?
            .map(|v| u64::from_be_bytes(v.as_ref().try_into().unwrap_or([0; 8])))
            .unwrap_or(0);

        Ok(Self {
            db,
            data,
            replog,
            meta,
            sequence: AtomicU64::new(last_seq),
            locks: KeyLockTable::new(),
        })
    }

    fn next_sequence(&self) -> Sequence {
        self.sequence.fetch_add(1, Ordering::SeqCst) + 1
    }

    fn persist_watermark(&self, seq: Sequence) -> Result<(), StorageError> {
        self.meta.insert(META_LAST_SEQ_KEY, &seq.to_be_bytes())?;
        Ok(())
    }

    fn record_replog(&self, event: &ChangeEvent) -> Result<(), StorageError> {
        let encoded = encode_event(event);
        self.replog.insert(event.sequence.to_be_bytes(), encoded)?;
        Ok(())
    }

    /// Read replicated-log entries with sequence > `from`, in order.
    pub fn read_replog_from(&self, from: Sequence) -> Result<Vec<ChangeEvent>, StorageError> {
        let start = (from + 1).to_be_bytes();
        let mut out = Vec::new();
        for item in self.replog.range(start..) {
            let (seq_bytes, value) = item?;
            let seq = Sequence::from_be_bytes(seq_bytes.as_ref().try_into().unwrap_or([0; 8]));
            if let Some(event) = decode_event_value(seq, &value) {
                out.push(event);
            }
        }
        Ok(out)
    }
}

/// Encodes key + timestamp + value/tombstone (sequence is the sled key, not repeated here).
fn encode_event(event: &ChangeEvent) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&event.timestamp.to_be_bytes());
    buf.extend_from_slice(&(event.key.len() as u32).to_be_bytes());
    buf.extend_from_slice(&event.key);
    match &event.value {
        ChangeValue::Put(v) => {
            buf.push(1);
            buf.extend_from_slice(&(v.len() as u32).to_be_bytes());
            buf.extend_from_slice(v);
        }
        ChangeValue::Delete => buf.push(0),
    }
    buf
}

fn decode_event_value(seq: Sequence, bytes: &[u8]) -> Option<ChangeEvent> {
    if bytes.len() < 16 + 4 {
        return None;
    }
    let mut pos = 0;
    let ts = u128::from_be_bytes(bytes[pos..pos + 16].try_into().ok()?);
    pos += 16;
    let key_len = u32::from_be_bytes(bytes[pos..pos + 4].try_into().ok()?) as usize;
    pos += 4;
    if bytes.len() < pos + key_len + 1 {
        return None;
    }
    let key = bytes[pos..pos + key_len].to_vec();
    pos += key_len;
    let tag = bytes[pos];
    pos += 1;
    let value = if tag == 1 {
        if bytes.len() < pos + 4 {
            return None;
        }
        let len = u32::from_be_bytes(bytes[pos..pos + 4].try_into().ok()?) as usize;
        pos += 4;
        if bytes.len() < pos + len {
            return None;
        }
        ChangeValue::Put(bytes[pos..pos + len].to_vec())
    } else {
        ChangeValue::Delete
    };
    Some(ChangeEvent {
        keyspace: String::new(), // filled in by caller (kv-core), which knows the keyspace name
        key,
        value,
        sequence: seq,
        timestamp: ts,
        origin_region: String::new(),
        hlc: Hlc::zero(),
    })
}

#[async_trait]
impl StorageEngine for ColdEngine {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        let data = self.data.clone();
        let key = key.to_vec();
        let result = tokio::task::spawn_blocking(move || data.get(key))
            .await
            .expect("blocking task panicked")?;
        Ok(result.map(|v| v.to_vec()))
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> Result<Sequence, StorageError> {
        let _guard = self.locks.lock(key).await;
        let seq = self.next_sequence();
        let event = ChangeEvent {
            keyspace: String::new(),
            key: key.to_vec(),
            value: ChangeValue::Put(value.to_vec()),
            sequence: seq,
            timestamp: now_millis(),
            origin_region: String::new(),
            hlc: Hlc::zero(),
        };
        let data = self.data.clone();
        let k = key.to_vec();
        let v = value.to_vec();
        tokio::task::spawn_blocking(move || data.insert(k, v))
            .await
            .expect("blocking task panicked")?;
        self.record_replog(&event)?;
        self.persist_watermark(seq)?;
        Ok(seq)
    }

    async fn delete(&self, key: &[u8]) -> Result<Sequence, StorageError> {
        let _guard = self.locks.lock(key).await;
        let seq = self.next_sequence();
        let event = ChangeEvent {
            keyspace: String::new(),
            key: key.to_vec(),
            value: ChangeValue::Delete,
            sequence: seq,
            timestamp: now_millis(),
            origin_region: String::new(),
            hlc: Hlc::zero(),
        };
        let data = self.data.clone();
        let k = key.to_vec();
        tokio::task::spawn_blocking(move || data.remove(k))
            .await
            .expect("blocking task panicked")?;
        self.record_replog(&event)?;
        self.persist_watermark(seq)?;
        Ok(seq)
    }

    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError> {
        let data = self.data.clone();
        let prefix = prefix.to_vec();
        let result = tokio::task::spawn_blocking(move || {
            data.scan_prefix(prefix)
                .map(|item| item.map(|(k, v)| (k.to_vec(), v.to_vec())))
                .collect::<Result<Vec<_>, sled::Error>>()
        })
        .await
        .expect("blocking task panicked")?;
        Ok(result)
    }

    async fn apply_replicated(&self, event: &ChangeEvent) -> Result<(), StorageError> {
        let _guard = self.locks.lock(&event.key).await;
        let last = self.sequence.load(Ordering::SeqCst);
        if event.sequence <= last {
            return Ok(());
        }
        let data = self.data.clone();
        let key = event.key.clone();
        match &event.value {
            ChangeValue::Put(value) => {
                let value = value.clone();
                tokio::task::spawn_blocking(move || data.insert(key, value))
                    .await
                    .expect("blocking task panicked")?;
            }
            ChangeValue::Delete => {
                tokio::task::spawn_blocking(move || data.remove(key))
                    .await
                    .expect("blocking task panicked")?;
            }
        }
        self.record_replog(event)?;
        self.sequence.store(event.sequence, Ordering::SeqCst);
        self.persist_watermark(event.sequence)?;
        Ok(())
    }

    fn last_applied_sequence(&self) -> Sequence {
        self.sequence.load(Ordering::SeqCst)
    }

    fn tier(&self) -> StorageTier {
        StorageTier::Cold
    }

    fn as_any_arc(self: std::sync::Arc<Self>) -> std::sync::Arc<dyn std::any::Any + Send + Sync> {
        self
    }
}

// Re-implement read_replog_from using decode_event_value (the single real decoder).
impl ColdEngine {
    pub fn flush(&self) -> Result<(), StorageError> {
        self.db.flush()?;
        Ok(())
    }
}
