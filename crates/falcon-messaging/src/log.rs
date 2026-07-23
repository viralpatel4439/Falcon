//! A durable append-only message log, offset-addressed. Backs durable topics,
//! queues, and event streams. Each appended message gets a monotonic offset
//! (starting at 1). Framing mirrors the KV WAL:
//! `[len:u32][offset:u64][ts:u128][payload]`, so a crash mid-append truncates
//! cleanly to the last whole record on replay.
//!
//! ## Group commit
//!
//! A dedicated background writer thread owns the file. `append` assigns an
//! offset, hands the framed bytes to the writer over a channel, and blocks
//! until the writer confirms durability. The writer drains *every* request
//! currently queued into one batch, writes them all, and does a **single**
//! fsync for the whole batch. So a burst of N concurrent appends costs one
//! fsync, not N — durable messaging throughput scales with concurrency
//! instead of pinning at `1 / fsync_latency`. Each `append` still returns
//! only once its own bytes are fsynced, so per-message durability is
//! unchanged. This is the same pattern the KV WAL uses (`wal_writer.rs`).

use crate::error::MessagingError;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{Receiver, Sender};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

pub type Offset = u64;

#[derive(Debug, Clone)]
pub struct LogMessage {
    pub offset: Offset,
    /// Append time (unix millis). Used for retention/age in a later phase.
    #[allow(dead_code)]
    pub timestamp: u128,
    pub payload: Vec<u8>,
}

pub struct MessageLog {
    /// Assigns offsets and submits framed bytes to the writer thread. Guarded
    /// so offset assignment + channel submission is atomic per append (records
    /// enter the log in offset order).
    submit: Mutex<Submitter>,
    path: PathBuf,
}

/// The offset counter + channel to the background writer, behind one lock.
struct Submitter {
    next_offset: Offset,
    tx: Sender<WriteRequest>,
    file_id: usize,
}

/// One queued append: which file it targets, the framed bytes, and a one-shot
/// channel the writer signals (with the fsync result) once the batch is durable.
struct WriteRequest {
    file_id: usize,
    framed: Vec<u8>,
    done: Sender<Result<(), String>>,
}

/// A shared group-commit writer that owns one or more append files behind a
/// single thread. Multiple `MessageLog`s (e.g. every partition of one event
/// stream) can register with the same writer so a burst across all of them is
/// drained together and each touched file is fsynced once — partitioning then
/// buys ordering/parallelism WITHOUT multiplying fsyncs on a single disk.
pub struct SharedWriter {
    tx: Sender<WriteRequest>,
    next_file_id: Mutex<usize>,
    // Files are registered by sending a Register request out-of-band; we keep
    // the registration channel here.
    register: Sender<(usize, File)>,
}

impl SharedWriter {
    /// Create a shared writer that fsyncs every batch (full durability: an
    /// append returns only once its bytes are on disk).
    pub fn new() -> Self {
        Self::with_fsync_interval(0)
    }

    /// Create a shared writer with an optional interval-fsync policy.
    /// `interval_ms == 0` means fsync-every-batch (fully durable). A value
    /// `> 0` switches to interval fsync: bytes are written immediately but the
    /// fsync (and the append acks that depend on it) are coalesced onto a
    /// timer, so appends across all partitions share one fsync per interval —
    /// far higher throughput at the cost of a bounded crash-loss window (up to
    /// one interval of acked-but-unsynced writes).
    pub fn with_fsync_interval(interval_ms: u64) -> Self {
        let (tx, rx) = std::sync::mpsc::channel::<WriteRequest>();
        let (register, reg_rx) = std::sync::mpsc::channel::<(usize, File)>();
        std::thread::Builder::new()
            .name("falcon-msglog-shared-writer".into())
            .spawn(move || {
                if interval_ms == 0 {
                    shared_writer_loop(rx, reg_rx)
                } else {
                    shared_writer_loop_interval(rx, reg_rx, interval_ms)
                }
            })
            .expect("failed to spawn shared message-log writer thread");
        Self {
            tx,
            next_file_id: Mutex::new(0),
            register,
        }
    }

    fn register_file(&self, file: File) -> Result<(usize, Sender<WriteRequest>), MessagingError> {
        let mut id = self.next_file_id.lock().expect("file id mutex poisoned");
        let file_id = *id;
        *id += 1;
        self.register
            .send((file_id, file))
            .map_err(|_| writer_gone())?;
        Ok((file_id, self.tx.clone()))
    }
}

impl Default for SharedWriter {
    fn default() -> Self {
        Self::new()
    }
}

fn now_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch")
        .as_millis()
}

impl MessageLog {
    /// Open a standalone log with its own dedicated writer thread. Best for a
    /// single durable topic or queue.
    pub fn open(path: &Path) -> Result<Self, MessagingError> {
        let (next, file) = Self::open_file(path)?;

        // Spawn a dedicated background group-commit writer owning this file.
        let (tx, rx) = std::sync::mpsc::channel::<WriteRequest>();
        std::thread::Builder::new()
            .name("falcon-msglog-writer".into())
            .spawn(move || writer_loop(file, rx))
            .expect("failed to spawn message-log writer thread");

        Ok(Self {
            submit: Mutex::new(Submitter {
                next_offset: next,
                tx,
                file_id: 0,
            }),
            path: path.to_path_buf(),
        })
    }

    /// Open a log that shares a group-commit writer with other logs (e.g. all
    /// partitions of one event stream), so a burst across them coalesces into
    /// one fsync per touched file on a single thread.
    pub fn open_shared(path: &Path, writer: &SharedWriter) -> Result<Self, MessagingError> {
        let (next, file) = Self::open_file(path)?;
        let (file_id, tx) = writer.register_file(file)?;
        Ok(Self {
            submit: Mutex::new(Submitter {
                next_offset: next,
                tx,
                file_id,
            }),
            path: path.to_path_buf(),
        })
    }

    /// Replay to find the next offset and open the append handle.
    fn open_file(path: &Path) -> Result<(Offset, File), MessagingError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let existing = Self::replay(path)?;
        let next = existing.last().map(|m| m.offset + 1).unwrap_or(1);
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path)?;
        Ok((next, file))
    }

    /// Append a message and return its assigned offset, durable (fsync'd)
    /// before returning. See the module docs: the background writer batches
    /// concurrent appends into a single fsync (group commit), so this returns
    /// as soon as *its* batch is on disk.
    pub fn append(&self, payload: &[u8]) -> Result<Offset, MessagingError> {
        let ts = now_millis();
        let (done_tx, done_rx) = std::sync::mpsc::channel();

        // Assign an offset and submit — under the lock so records are queued
        // in strict offset order.
        let offset = {
            let mut s = self.submit.lock().expect("log submit mutex poisoned");
            let offset = s.next_offset;

            let mut framed = Vec::with_capacity(4 + 8 + 16 + payload.len());
            framed.extend_from_slice(&((8 + 16 + payload.len()) as u32).to_be_bytes());
            framed.extend_from_slice(&offset.to_be_bytes());
            framed.extend_from_slice(&ts.to_be_bytes());
            framed.extend_from_slice(payload);

            s.tx.send(WriteRequest {
                file_id: s.file_id,
                framed,
                done: done_tx,
            })
            .map_err(|_| writer_gone())?;
            s.next_offset = offset + 1;
            offset
        };

        // Block until the writer has fsynced the batch containing this record.
        match done_rx.recv() {
            Ok(Ok(())) => Ok(offset),
            Ok(Err(e)) => Err(MessagingError::Io(std::io::Error::other(e))),
            Err(_) => Err(writer_gone()),
        }
    }

    /// Read all messages with offset >= `from`, in order.
    pub fn read_from(&self, from: Offset) -> Result<Vec<LogMessage>, MessagingError> {
        Ok(Self::replay(&self.path)?
            .into_iter()
            .filter(|m| m.offset >= from)
            .collect())
    }

    pub fn next_offset(&self) -> Offset {
        self.submit.lock().expect("log submit mutex poisoned").next_offset
    }

    fn replay(path: &Path) -> Result<Vec<LogMessage>, MessagingError> {
        if !path.exists() {
            return Ok(Vec::new());
        }
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        let mut out = Vec::new();
        loop {
            let mut len_buf = [0u8; 4];
            match reader.read_exact(&mut len_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }
            let len = u32::from_be_bytes(len_buf) as usize;
            let mut body = vec![0u8; len];
            if reader.read_exact(&mut body).is_err() {
                break; // partial trailing record from a crash; stop
            }
            let (Ok(off_bytes), Ok(ts_bytes)) = (
                <[u8; 8]>::try_from(body.get(0..8).unwrap_or(&[])),
                <[u8; 16]>::try_from(body.get(8..24).unwrap_or(&[])),
            ) else {
                break; // truncated/garbage header; stop cleanly
            };
            let offset = u64::from_be_bytes(off_bytes);
            let timestamp = u128::from_be_bytes(ts_bytes);
            let payload = body[24..].to_vec();
            out.push(LogMessage {
                offset,
                timestamp,
                payload,
            });
        }
        Ok(out)
    }
}

fn writer_gone() -> MessagingError {
    MessagingError::Io(std::io::Error::new(
        std::io::ErrorKind::BrokenPipe,
        "message-log writer thread is no longer running",
    ))
}

/// The background group-commit writer: owns the file, drains every queued
/// request into one batch, writes them all, does a SINGLE fsync, then signals
/// every waiter in the batch. Exits when all senders drop (log closed).
fn writer_loop(mut file: File, rx: Receiver<WriteRequest>) {
    // Block for the first request; if the channel is closed, stop.
    while let Ok(first) = rx.recv() {
        let mut batch = vec![first];
        // Drain everything else already queued — this is the group commit:
        // all of it becomes durable with one fsync.
        while let Ok(next) = rx.try_recv() {
            batch.push(next);
        }

        // Write all framed records, then fsync once for the whole batch.
        let mut write_err: Option<String> = None;
        for req in &batch {
            if let Err(e) = file.write_all(&req.framed) {
                write_err = Some(e.to_string());
                break;
            }
        }
        let result = match write_err {
            Some(e) => Err(e),
            None => file.sync_data().map_err(|e| e.to_string()),
        };

        // Signal every waiter with the shared batch result.
        for req in batch {
            let _ = req.done.send(result.clone());
        }
    }
}

/// The shared group-commit writer: owns MANY files (one per registered log,
/// e.g. every partition of a stream) and drains all pending appends across
/// every file in one batch. It writes each request to its target file, then
/// fsyncs **each touched file exactly once** for the whole batch. So a burst
/// spread over P partitions costs at most P fsyncs total per drain cycle —
/// independent of how many records — instead of one fsync per record. On a
/// single disk this is the difference between partitioning helping and
/// hurting throughput.
fn shared_writer_loop(rx: Receiver<WriteRequest>, reg_rx: Receiver<(usize, File)>) {
    let mut files: Vec<Option<File>> = Vec::new();

    // Absorb any files registered before we start looping.
    while let Ok((id, file)) = reg_rx.try_recv() {
        ensure_slot(&mut files, id);
        files[id] = Some(file);
    }

    while let Ok(first) = rx.recv() {
        // Pick up any newly-registered files first.
        while let Ok((id, file)) = reg_rx.try_recv() {
            ensure_slot(&mut files, id);
            files[id] = Some(file);
        }

        let mut batch = vec![first];
        while let Ok(next) = rx.try_recv() {
            batch.push(next);
        }

        // Write every request to its target file, tracking which files were
        // touched so we fsync each of them exactly once.
        let mut touched: Vec<bool> = vec![false; files.len()];
        let mut write_err: Option<String> = None;
        for req in &batch {
            if req.file_id >= files.len() {
                // A file registered after this request was queued but before
                // we picked it up; refresh registrations and retry the slot.
                while let Ok((id, file)) = reg_rx.try_recv() {
                    ensure_slot(&mut files, id);
                    files[id] = Some(file);
                }
                if touched.len() < files.len() {
                    touched.resize(files.len(), false);
                }
            }
            match files.get_mut(req.file_id).and_then(|f| f.as_mut()) {
                Some(f) => {
                    if let Err(e) = f.write_all(&req.framed) {
                        write_err = Some(e.to_string());
                        break;
                    }
                    touched[req.file_id] = true;
                }
                None => {
                    write_err = Some(format!("unknown log file id {}", req.file_id));
                    break;
                }
            }
        }

        // One fsync per touched file for the whole batch.
        let result = match write_err {
            Some(e) => Err(e),
            None => {
                let mut r = Ok(());
                for (id, was_touched) in touched.iter().enumerate() {
                    if *was_touched {
                        if let Some(f) = files[id].as_ref() {
                            if let Err(e) = f.sync_data() {
                                r = Err(e.to_string());
                                break;
                            }
                        }
                    }
                }
                r
            }
        };

        for req in batch {
            let _ = req.done.send(result.clone());
        }
    }
}

fn ensure_slot(files: &mut Vec<Option<File>>, id: usize) {
    if id >= files.len() {
        files.resize_with(id + 1, || None);
    }
}

/// Interval-fsync variant of the shared writer. Bytes are written to their
/// files as requests arrive, but the fsync — and the acks that wait on it — are
/// deferred and coalesced onto a fixed timer. So a whole interval's worth of
/// appends across every partition costs one fsync per touched file per tick.
/// Trades a bounded crash-loss window (≤ one interval of acked-but-unsynced
/// writes) for much higher durable throughput. Same knob as the KV warm tier's
/// `interval_fsync_ms`.
fn shared_writer_loop_interval(
    rx: Receiver<WriteRequest>,
    reg_rx: Receiver<(usize, File)>,
    interval_ms: u64,
) {
    use std::time::{Duration, Instant};
    let interval = Duration::from_millis(interval_ms);
    let mut files: Vec<Option<File>> = Vec::new();
    // Waiters whose bytes are written but not yet fsynced, and which files are
    // dirty since the last fsync.
    let mut pending: Vec<Sender<Result<(), String>>> = Vec::new();
    let mut dirty: Vec<bool> = Vec::new();
    let mut next_sync = Instant::now() + interval;

    let absorb_regs = |files: &mut Vec<Option<File>>, reg_rx: &Receiver<(usize, File)>| {
        while let Ok((id, file)) = reg_rx.try_recv() {
            ensure_slot(files, id);
            files[id] = Some(file);
        }
    };

    loop {
        absorb_regs(&mut files, &reg_rx);
        let now = Instant::now();
        let timeout = next_sync.saturating_duration_since(now);

        match rx.recv_timeout(timeout) {
            Ok(req) => {
                absorb_regs(&mut files, &reg_rx);
                if dirty.len() < files.len() {
                    dirty.resize(files.len(), false);
                }
                match files.get_mut(req.file_id).and_then(|f| f.as_mut()) {
                    Some(f) => match f.write_all(&req.framed) {
                        Ok(()) => {
                            dirty[req.file_id] = true;
                            pending.push(req.done);
                        }
                        Err(e) => {
                            let _ = req.done.send(Err(e.to_string()));
                        }
                    },
                    None => {
                        let _ = req.done.send(Err(format!("unknown log file id {}", req.file_id)));
                    }
                }
                // Keep draining until the timer is due, batching aggressively.
                if Instant::now() < next_sync {
                    continue;
                }
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                // Final fsync + ack, then exit.
                flush_and_ack(&mut files, &mut dirty, &mut pending);
                return;
            }
        }

        // Timer due: one fsync per dirty file, then ack everyone waiting.
        flush_and_ack(&mut files, &mut dirty, &mut pending);
        next_sync = Instant::now() + interval;
    }
}

/// Fsync every dirty file once, then resolve all pending append waiters with
/// the shared result. Clears the dirty flags and the pending list.
fn flush_and_ack(
    files: &mut [Option<File>],
    dirty: &mut [bool],
    pending: &mut Vec<Sender<Result<(), String>>>,
) {
    if pending.is_empty() {
        return;
    }
    let mut result = Ok(());
    for (id, is_dirty) in dirty.iter_mut().enumerate() {
        if *is_dirty {
            if let Some(f) = files[id].as_ref() {
                if let Err(e) = f.sync_data() {
                    result = Err(e.to_string());
                }
            }
            *is_dirty = false;
        }
    }
    for done in pending.drain(..) {
        let _ = done.send(result.clone());
    }
}
