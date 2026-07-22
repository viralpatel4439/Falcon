use crate::engine::StorageError;
use falcon_events::{now_millis, Hlc, Sequence, Timestamp};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::Path;

const OP_PUT: u8 = 1;
const OP_DELETE: u8 = 2;

/// How many records apart the sparse offset index samples. Bounds the
/// post-seek linear scan in `replay_from_offset` to at most this many
/// record parses, while keeping index memory at roughly total/STRIDE.
pub const SPARSE_INDEX_STRIDE: u64 = 64;

pub enum WalOp {
    Put(Vec<u8>),
    Delete,
}

pub struct WalRecord {
    pub sequence: Sequence,
    pub timestamp: Timestamp,
    pub key: Vec<u8>,
    pub op: WalOp,
    /// HLC stamp for multi-region last-write-wins. `Hlc::zero()` for
    /// single-leader writes (and for records written before HLC support).
    pub hlc: Hlc,
}

/// Sequence -> file byte offset, sampled every `SPARSE_INDEX_STRIDE`
/// records, so replication catch-up can seek near a target sequence
/// instead of re-scanning the WAL from byte 0. Purely a speed
/// optimization: `consistent` gates whether it's trusted at all, and
/// every consumer falls back to a full scan when it isn't.
pub struct SparseIndex {
    entries: BTreeMap<Sequence, u64>,
    records_since_sample: u64,
    consistent: bool,
}

impl SparseIndex {
    pub fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
            records_since_sample: 0,
            consistent: true,
        }
    }

    /// Called once per record as it's written/replayed, in sequence order.
    pub fn record(&mut self, sequence: Sequence, offset: u64) {
        if self.records_since_sample == 0 {
            self.entries.insert(sequence, offset);
        }
        self.records_since_sample = (self.records_since_sample + 1) % SPARSE_INDEX_STRIDE;
    }

    /// Nearest sampled offset at or before `sequence`, if the index is
    /// still trusted and has any entry that old.
    pub fn floor(&self, sequence: Sequence) -> Option<u64> {
        if !self.consistent {
            return None;
        }
        self.entries.range(..=sequence).next_back().map(|(_, &offset)| offset)
    }

    pub fn invalidate(&mut self) {
        self.consistent = false;
    }

    pub fn is_consistent(&self) -> bool {
        self.consistent
    }
}

impl Default for SparseIndex {
    fn default() -> Self {
        Self::new()
    }
}

/// Builds the length-prefixed on-disk bytes for one record. Pure, no I/O —
/// callers (via `WalWriter`) do the actual write + fsync so writes to
/// different keys can be batched into one fsync (group commit).
///
/// Format: [total_len: u32][seq: u64][ts: u128][op: u8][key_len: u32][key]
///         [value_len: u32][value?][hlc_wall: u64][hlc_logical: u32][region_len: u32][region]
/// `value_len`/`value` are omitted for deletes. The HLC trailer is always
/// present (zero HLC for single-leader). Records without the trailer (from
/// before HLC support) parse with `Hlc::zero()`.
pub fn frame_record(sequence: Sequence, key: &[u8], op: &WalOp) -> (Vec<u8>, Timestamp) {
    frame_record_hlc(sequence, key, op, &Hlc::zero())
}

/// Like `frame_record` but stamps the record with an explicit HLC (used by
/// multi-leader writes).
pub fn frame_record_hlc(sequence: Sequence, key: &[u8], op: &WalOp, hlc: &Hlc) -> (Vec<u8>, Timestamp) {
    let ts = now_millis();
    let mut buf = Vec::new();
    buf.extend_from_slice(&sequence.to_be_bytes());
    buf.extend_from_slice(&ts.to_be_bytes());
    match op {
        WalOp::Put(value) => {
            buf.push(OP_PUT);
            buf.extend_from_slice(&(key.len() as u32).to_be_bytes());
            buf.extend_from_slice(key);
            buf.extend_from_slice(&(value.len() as u32).to_be_bytes());
            buf.extend_from_slice(value);
        }
        WalOp::Delete => {
            buf.push(OP_DELETE);
            buf.extend_from_slice(&(key.len() as u32).to_be_bytes());
            buf.extend_from_slice(key);
        }
    }
    // HLC trailer.
    buf.extend_from_slice(&hlc.wall.to_be_bytes());
    buf.extend_from_slice(&hlc.logical.to_be_bytes());
    buf.extend_from_slice(&(hlc.region.len() as u32).to_be_bytes());
    buf.extend_from_slice(hlc.region.as_bytes());

    let mut framed = Vec::with_capacity(buf.len() + 4);
    framed.extend_from_slice(&(buf.len() as u32).to_be_bytes());
    framed.extend_from_slice(&buf);
    (framed, ts)
}

pub struct Wal;

impl Wal {
    /// Opens (creating if needed) the WAL file for appending, returning
    /// the raw `File` for a `WalWriter` to take exclusive ownership of.
    pub fn open_file(path: &Path) -> Result<File, StorageError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path)?;
        Ok(file)
    }

    /// Replay all records currently in the WAL, in order. Truncates a
    /// trailing partial record (from a crash mid-write, including a crash
    /// mid-batch under group commit — indistinguishable on disk from a
    /// crash mid-single-write) rather than erroring.
    pub fn replay(path: &Path) -> Result<Vec<WalRecord>, StorageError> {
        Self::replay_inner(path, 0).map(|(records, _index, _end_offset)| records)
    }

    /// Like `replay`, but also builds a `SparseIndex` over the records
    /// found, using the same offsets replay already computes.
    pub fn replay_with_index(path: &Path) -> Result<(Vec<WalRecord>, SparseIndex, u64), StorageError> {
        Self::replay_inner(path, 0)
    }

    /// Replay starting at a given byte offset (used by replication
    /// catch-up once the sparse index has located a nearby offset).
    pub fn replay_from_offset(path: &Path, offset: u64) -> Result<Vec<WalRecord>, StorageError> {
        Self::replay_inner(path, offset).map(|(records, _index, _end_offset)| records)
    }

    fn replay_inner(
        path: &Path,
        start_offset: u64,
    ) -> Result<(Vec<WalRecord>, SparseIndex, u64), StorageError> {
        let mut index = SparseIndex::new();
        if !path.exists() {
            return Ok((Vec::new(), index, 0));
        }
        let mut file = File::open(path)?;
        if start_offset > 0 {
            file.seek(SeekFrom::Start(start_offset))?;
        }
        let mut reader = BufReader::new(file);
        let mut records = Vec::new();
        let mut offset: u64 = start_offset;

        loop {
            let record_start = offset;
            let mut len_buf = [0u8; 4];
            match reader.read_exact(&mut len_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }
            let len = u32::from_be_bytes(len_buf) as usize;

            let mut body = vec![0u8; len];
            if reader.read_exact(&mut body).is_err() {
                // Partial trailing record from a crash mid-write (or
                // mid-batch); stop here.
                break;
            }

            match parse_record(&body) {
                Some(record) => {
                    index.record(record.sequence, record_start);
                    records.push(record);
                }
                None => break,
            }
            offset += 4 + len as u64;
        }
        Ok((records, index, offset))
    }
}

fn parse_record(body: &[u8]) -> Option<WalRecord> {
    if body.len() < 8 + 16 + 1 + 4 {
        return None;
    }
    let mut pos = 0;
    let sequence = Sequence::from_be_bytes(body[pos..pos + 8].try_into().ok()?);
    pos += 8;
    let timestamp = Timestamp::from_be_bytes(body[pos..pos + 16].try_into().ok()?);
    pos += 16;
    let op = body[pos];
    pos += 1;

    let key_len = u32::from_be_bytes(body[pos..pos + 4].try_into().ok()?) as usize;
    pos += 4;
    if body.len() < pos + key_len {
        return None;
    }
    let key = body[pos..pos + key_len].to_vec();
    pos += key_len;

    let parsed_op = match op {
        OP_PUT => {
            if body.len() < pos + 4 {
                return None;
            }
            let value_len = u32::from_be_bytes(body[pos..pos + 4].try_into().ok()?) as usize;
            pos += 4;
            if body.len() < pos + value_len {
                return None;
            }
            let value = body[pos..pos + value_len].to_vec();
            pos += value_len;
            WalOp::Put(value)
        }
        OP_DELETE => WalOp::Delete,
        _ => return None,
    };

    // HLC trailer (may be absent in pre-HLC records -> zero HLC).
    let hlc = parse_hlc_trailer(&body[pos..]).unwrap_or_else(Hlc::zero);

    Some(WalRecord {
        sequence,
        timestamp,
        key,
        op: parsed_op,
        hlc,
    })
}

fn parse_hlc_trailer(trailer: &[u8]) -> Option<Hlc> {
    if trailer.len() < 8 + 4 + 4 {
        return None;
    }
    let wall = u64::from_be_bytes(trailer[0..8].try_into().ok()?);
    let logical = u32::from_be_bytes(trailer[8..12].try_into().ok()?);
    let region_len = u32::from_be_bytes(trailer[12..16].try_into().ok()?) as usize;
    if trailer.len() < 16 + region_len {
        return None;
    }
    let region = String::from_utf8(trailer[16..16 + region_len].to_vec()).ok()?;
    Some(Hlc { wall, logical, region })
}
