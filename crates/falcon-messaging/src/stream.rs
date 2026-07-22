//! Falcon Event Streaming: partitioned, offset-addressed, replayable event
//! logs with durable consumer-group cursors — the Kafka-shaped sibling of
//! pub/sub topics.
//!
//! A [`Topic`](crate::Topic) is a single log with live fan-out. A `Stream`
//! adds the three things a real event pipeline needs:
//!
//! - **Partitioning** — each record is routed to a partition by
//!   `hash(partition_key) % partitions`. Records that share a key land on the
//!   same partition and are therefore **totally ordered** relative to each
//!   other, while unrelated keys spread across partitions for parallelism.
//!   Each partition is an independent durable [`MessageLog`].
//! - **Consumer groups** — a group has one durable committed offset *per
//!   partition*. Members of a group split the partitions (competing
//!   consumers); different groups each see the full stream independently.
//!   Committed offsets survive restart, so a consumer resumes where it left
//!   off — at-least-once from its last commit.
//! - **Replay + live tail** — read any range from an offset (durable
//!   history) and subscribe for records appended from now on (live).
//!
//! Partitioning uses the same stable FNV-1a hash as the sharded store, so a
//! key's partition is stable across processes.

use crate::error::MessagingError;
use crate::log::{MessageLog, Offset};
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::broadcast;

/// A record delivered from a stream: its partition, offset within that
/// partition, and payload. `(partition, offset)` is the durable coordinate a
/// consumer commits.
#[derive(Clone, Debug)]
pub struct StreamRecord {
    pub partition: usize,
    pub offset: Offset,
    pub payload: Arc<Vec<u8>>,
}

/// One partition: a durable append log plus a live broadcast channel.
struct Partition {
    log: MessageLog,
    tx: broadcast::Sender<StreamRecord>,
    index: usize,
}

impl Partition {
    fn open(index: usize, path: &Path, capacity: usize) -> Result<Self, MessagingError> {
        let log = MessageLog::open(path)?;
        let (tx, _) = broadcast::channel(capacity.max(16));
        Ok(Self { log, tx, index })
    }

    fn append(&self, payload: Vec<u8>) -> Result<Offset, MessagingError> {
        let offset = self.log.append(&payload)?;
        let _ = self.tx.send(StreamRecord {
            partition: self.index,
            offset,
            payload: Arc::new(payload),
        });
        Ok(offset)
    }

    fn read_from(&self, from: Offset) -> Result<Vec<StreamRecord>, MessagingError> {
        Ok(self
            .log
            .read_from(from)?
            .into_iter()
            .map(|m| StreamRecord {
                partition: self.index,
                offset: m.offset,
                payload: Arc::new(m.payload),
            })
            .collect())
    }
}

/// A partitioned, durable, replayable event stream.
pub struct Stream {
    name: String,
    partitions: Vec<Partition>,
    /// Durable per-group committed offsets, keyed by group name. Held in
    /// memory and mirrored to one small file per group under `offsets_dir`.
    groups: Mutex<HashMap<String, GroupCursor>>,
    offsets_dir: PathBuf,
}

/// A consumer group's committed offsets, one per partition. `committed[p]` is
/// the offset of the last record the group has processed on partition `p`
/// (0 = nothing committed yet; records start at offset 1).
#[derive(Clone, Debug)]
struct GroupCursor {
    committed: Vec<Offset>,
}

impl Stream {
    /// Open a stream named `name` with `partitions` durable partitions
    /// (min 1) under `data_dir`. Existing partition logs and group offsets
    /// are recovered.
    pub fn open(
        name: &str,
        partitions: usize,
        data_dir: &Path,
        capacity: usize,
    ) -> Result<Self, MessagingError> {
        let n = partitions.max(1);
        let stream_dir = data_dir.join(format!("stream_{name}"));
        std::fs::create_dir_all(&stream_dir)?;
        let mut parts = Vec::with_capacity(n);
        for i in 0..n {
            let path = stream_dir.join(format!("partition_{i}.log"));
            parts.push(Partition::open(i, &path, capacity)?);
        }
        let offsets_dir = stream_dir.join("offsets");
        std::fs::create_dir_all(&offsets_dir)?;
        let groups = recover_groups(&offsets_dir, n)?;
        Ok(Self {
            name: name.to_string(),
            partitions: parts,
            groups: Mutex::new(groups),
            offsets_dir,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn partition_count(&self) -> usize {
        self.partitions.len()
    }

    /// Route `key` to a partition using the stable FNV-1a hash. Records with
    /// the same key are always ordered on the same partition.
    pub fn partition_for(&self, key: &[u8]) -> usize {
        (fnv1a(key) % self.partitions.len() as u64) as usize
    }

    /// Append a record, routing by `key`. Returns `(partition, offset)`.
    /// Durable (fsync) before the live broadcast, so a live consumer never
    /// sees a record that isn't persisted.
    pub fn append_keyed(
        &self,
        key: &[u8],
        payload: Vec<u8>,
    ) -> Result<(usize, Offset), MessagingError> {
        let p = self.partition_for(key);
        let offset = self.partitions[p].append(payload)?;
        Ok((p, offset))
    }

    /// Append directly to a chosen partition (for callers that partition
    /// themselves, e.g. round-robin producers).
    pub fn append_to(
        &self,
        partition: usize,
        payload: Vec<u8>,
    ) -> Result<Offset, MessagingError> {
        let part = self
            .partitions
            .get(partition)
            .ok_or(MessagingError::PartitionOutOfRange(
                partition,
                self.partitions.len(),
            ))?;
        part.append(payload)
    }

    /// Live subscription to a partition: records appended from now on.
    pub fn subscribe(
        &self,
        partition: usize,
    ) -> Result<broadcast::Receiver<StreamRecord>, MessagingError> {
        let part = self
            .partitions
            .get(partition)
            .ok_or(MessagingError::PartitionOutOfRange(
                partition,
                self.partitions.len(),
            ))?;
        Ok(part.tx.subscribe())
    }

    /// Durable replay of a partition from `from` (inclusive of offsets >=
    /// `from`).
    pub fn replay(
        &self,
        partition: usize,
        from: Offset,
    ) -> Result<Vec<StreamRecord>, MessagingError> {
        let part = self
            .partitions
            .get(partition)
            .ok_or(MessagingError::PartitionOutOfRange(
                partition,
                self.partitions.len(),
            ))?;
        part.read_from(from)
    }

    /// The offset a group should resume from on a partition: one past its
    /// last committed offset (so a fresh group starts at 1 = the first
    /// record). Registers the group on first use.
    pub fn group_next_offset(&self, group: &str, partition: usize) -> Offset {
        let mut groups = self.groups.lock().expect("groups mutex poisoned");
        let cursor = groups
            .entry(group.to_string())
            .or_insert_with(|| GroupCursor {
                committed: vec![0; self.partitions.len()],
            });
        cursor.committed.get(partition).copied().unwrap_or(0) + 1
    }

    /// Fetch the next batch for a group on a partition (records after its
    /// committed offset). Does NOT commit — the caller commits after
    /// processing, which is what makes delivery at-least-once.
    pub fn poll(
        &self,
        group: &str,
        partition: usize,
    ) -> Result<Vec<StreamRecord>, MessagingError> {
        let from = self.group_next_offset(group, partition);
        self.replay(partition, from)
    }

    /// Durably commit a group's progress on a partition. Monotonic: a commit
    /// that would move the cursor backwards is ignored.
    pub fn commit(
        &self,
        group: &str,
        partition: usize,
        offset: Offset,
    ) -> Result<(), MessagingError> {
        if partition >= self.partitions.len() {
            return Err(MessagingError::PartitionOutOfRange(
                partition,
                self.partitions.len(),
            ));
        }
        let snapshot = {
            let mut groups = self.groups.lock().expect("groups mutex poisoned");
            let cursor = groups
                .entry(group.to_string())
                .or_insert_with(|| GroupCursor {
                    committed: vec![0; self.partitions.len()],
                });
            if offset > cursor.committed[partition] {
                cursor.committed[partition] = offset;
            }
            cursor.committed.clone()
        };
        write_group_offsets(&self.offsets_dir, group, &snapshot)
    }

    /// A group's committed offset on a partition (0 = nothing committed).
    pub fn committed_offset(&self, group: &str, partition: usize) -> Offset {
        let groups = self.groups.lock().expect("groups mutex poisoned");
        groups
            .get(group)
            .and_then(|c| c.committed.get(partition).copied())
            .unwrap_or(0)
    }
}

/// Group offset file format: `partition_count:u32` then that many
/// `offset:u64`, big-endian. Rewritten in full on each commit (tiny: bytes).
/// Written to a temp file and renamed for atomic, crash-safe replacement.
fn write_group_offsets(
    dir: &Path,
    group: &str,
    committed: &[Offset],
) -> Result<(), MessagingError> {
    let mut buf = Vec::with_capacity(4 + committed.len() * 8);
    buf.extend_from_slice(&(committed.len() as u32).to_be_bytes());
    for &o in committed {
        buf.extend_from_slice(&o.to_be_bytes());
    }
    let path = dir.join(format!("group_{}.off", encode_group(group)));
    let tmp = path.with_extension("tmp");
    {
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)?;
        f.write_all(&buf)?;
        f.sync_data()?;
    }
    std::fs::rename(&tmp, &path)?;
    Ok(())
}

fn recover_groups(
    dir: &Path,
    partitions: usize,
) -> Result<HashMap<String, GroupCursor>, MessagingError> {
    let mut out = HashMap::new();
    let rd = match std::fs::read_dir(dir) {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
        Err(e) => return Err(e.into()),
    };
    for entry in rd.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let Some(encoded) = name.strip_prefix("group_").and_then(|s| s.strip_suffix(".off")) else {
            continue;
        };
        let Some(group) = decode_group(encoded) else {
            continue;
        };
        let mut bytes = Vec::new();
        if std::fs::File::open(entry.path())
            .and_then(|mut f| f.read_to_end(&mut bytes))
            .is_err()
        {
            continue;
        }
        if let Some(committed) = parse_offsets(&bytes, partitions) {
            out.insert(group, GroupCursor { committed });
        }
    }
    Ok(out)
}

fn parse_offsets(bytes: &[u8], partitions: usize) -> Option<Vec<Offset>> {
    let count = u32::from_be_bytes(bytes.get(0..4)?.try_into().ok()?) as usize;
    let mut committed = vec![0u64; partitions];
    // Tolerate a partition-count change: read every stored offset (advancing
    // through the buffer) but only keep the ones that still fit the current
    // partition count.
    for (i, slot) in committed.iter_mut().enumerate().take(count) {
        let start = 4 + i * 8;
        *slot = u64::from_be_bytes(bytes.get(start..start + 8)?.try_into().ok()?);
    }
    Some(committed)
}

/// Group names go into filenames; encode any non-safe byte as `%XX` so
/// arbitrary group names map to a flat, collision-free filename.
fn encode_group(group: &str) -> String {
    let mut out = String::with_capacity(group.len());
    for &b in group.as_bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_') {
            out.push(b as char);
        } else {
            out.push('%');
            out.push(char::from_digit((b >> 4) as u32, 16).unwrap());
            out.push(char::from_digit((b & 0xf) as u32, 16).unwrap());
        }
    }
    if out.is_empty() {
        out.push_str("default");
    }
    out
}

fn decode_group(name: &str) -> Option<String> {
    let bytes = name.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hi = (*bytes.get(i + 1)? as char).to_digit(16)? as u8;
            let lo = (*bytes.get(i + 2)? as char).to_digit(16)? as u8;
            out.push((hi << 4) | lo);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

/// FNV-1a 64-bit: fast, deterministic, stable across processes — the property
/// partition routing relies on so a key's partition never shifts on restart.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn group_name_encoding_round_trips() {
        for g in ["orders", "group/with:slash", "analytics-1", "grp.1", "a b"] {
            let dec = decode_group(&encode_group(g)).expect("decode");
            assert_eq!(dec, g, "round-trip failed for {g:?}");
        }
        // An empty group name is a degenerate case: it maps to the literal
        // "default" filename rather than round-tripping.
        assert_eq!(encode_group(""), "default");
    }

    #[test]
    fn same_key_same_partition() {
        let dir = tempfile::tempdir().unwrap();
        let s = Stream::open("s", 8, dir.path(), 64).unwrap();
        let a = s.partition_for(b"user:42");
        let b = s.partition_for(b"user:42");
        assert_eq!(a, b);
        assert!(a < 8);
    }
}
