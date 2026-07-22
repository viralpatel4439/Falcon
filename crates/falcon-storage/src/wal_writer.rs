use crate::engine::StorageError;
use crate::wal::SparseIndex;
use falcon_events::{Sequence, Timestamp};
use std::fs::File;
use std::io::Write;
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, oneshot};

/// Fsync durability policy. `Always` (the default) fsyncs every batch
/// before acknowledging any write in it — every acknowledged write is
/// fully durable. `IntervalMs` is a documented, opt-in relaxation: writes
/// are acked (and become visible) before their bytes are guaranteed on
/// disk, trading a bounded crash-loss window for lower latency under
/// light load. Only `Always` is wired up end-to-end today.
#[derive(Clone, Copy, Debug)]
pub enum FsyncPolicy {
    Always,
    IntervalMs(u64),
}

struct CommitRequest {
    framed: Vec<u8>,
    ack: oneshot::Sender<Result<(), StorageError>>,
}

/// Owns the WAL file exclusively (no lock needed — only this task ever
/// touches it) and batches concurrently-submitted writes into a single
/// fsync: group commit. Under light load a batch is just one request (the
/// same latency as fsync-per-write); under concurrent load, requests that
/// arrive while a previous fsync is in flight naturally pile up in the
/// channel and get flushed together, so throughput scales up with
/// concurrency instead of staying flat at `1 / fsync_latency`.
#[derive(Clone)]
pub struct WalWriter {
    tx: mpsc::UnboundedSender<CommitRequest>,
}

impl WalWriter {
    /// Spawns the background writer task with the default `Always` policy.
    pub fn spawn(file: File, initial_offset: u64, initial_index: SparseIndex) -> (Self, Arc<Mutex<SparseIndex>>) {
        Self::spawn_with_policy(file, initial_offset, initial_index, FsyncPolicy::Always)
    }

    /// Spawns the background writer with an explicit durability policy.
    pub fn spawn_with_policy(
        file: File,
        initial_offset: u64,
        initial_index: SparseIndex,
        policy: FsyncPolicy,
    ) -> (Self, Arc<Mutex<SparseIndex>>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let index = Arc::new(Mutex::new(initial_index));
        let index_for_task = index.clone();
        match policy {
            FsyncPolicy::Always => {
                tokio::spawn(run_always(file, rx, index_for_task, initial_offset));
            }
            FsyncPolicy::IntervalMs(ms) => {
                tokio::spawn(run_interval(file, rx, index_for_task, initial_offset, ms));
            }
        }
        (Self { tx }, index)
    }

    /// Submits a pre-framed record and awaits durability (per the
    /// `Always` policy): this does not return until the batch containing
    /// this record has been fsynced.
    pub async fn submit(&self, ts_hint: Timestamp, framed: Vec<u8>) -> Result<Timestamp, StorageError> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.tx
            .send(CommitRequest { framed, ack: ack_tx })
            .map_err(|_| wal_writer_gone())?;
        ack_rx.await.map_err(|_| wal_writer_gone())??;
        Ok(ts_hint)
    }
}

fn wal_writer_gone() -> StorageError {
    StorageError::Io(std::io::Error::new(
        std::io::ErrorKind::BrokenPipe,
        "wal writer task is no longer running",
    ))
}

/// Write a batch of requests to the file (no fsync), updating the sparse
/// index. Returns the first write error, if any.
fn write_batch(
    file: &mut File,
    index: &Arc<Mutex<SparseIndex>>,
    offset: &mut u64,
    batch: &[CommitRequest],
) -> Option<std::io::Error> {
    let mut idx = index.lock().expect("sparse index mutex poisoned");
    for req in batch {
        if let Err(e) = file.write_all(&req.framed) {
            return Some(e);
        }
        // Sequence is embedded right after the length prefix (bytes 4..12).
        if let Ok(seq_bytes) = <[u8; 8]>::try_from(req.framed.get(4..12).unwrap_or(&[])) {
            idx.record(Sequence::from_be_bytes(seq_bytes), *offset);
        }
        *offset += req.framed.len() as u64;
    }
    None
}

fn ack_batch(batch: Vec<CommitRequest>, result: &std::io::Result<()>) {
    for req in batch {
        let r = match result {
            Ok(()) => Ok(()),
            Err(e) => Err(StorageError::Io(std::io::Error::new(e.kind(), e.to_string()))),
        };
        let _ = req.ack.send(r);
    }
}

/// `Always` policy: fsync every batch before acking — full durability.
async fn run_always(
    mut file: File,
    mut rx: mpsc::UnboundedReceiver<CommitRequest>,
    index: Arc<Mutex<SparseIndex>>,
    mut offset: u64,
) {
    while let Some(first) = rx.recv().await {
        // Drain everything else currently queued — this forms the batch.
        let mut batch = vec![first];
        while let Ok(next) = rx.try_recv() {
            batch.push(next);
        }

        let write_err = write_batch(&mut file, &index, &mut offset, &batch);
        let fsync_result = match write_err {
            None => file.sync_data(),
            Some(e) => Err(e),
        };
        ack_batch(batch, &fsync_result);
    }
}

/// `IntervalMs` policy: write bytes and ack immediately (before fsync), and
/// fsync on a fixed interval. Trades a bounded crash-loss window (up to one
/// interval of already-acked writes) for lower write latency. Explicitly
/// opt-in — documented as NOT fully durable per-write.
async fn run_interval(
    mut file: File,
    mut rx: mpsc::UnboundedReceiver<CommitRequest>,
    index: Arc<Mutex<SparseIndex>>,
    mut offset: u64,
    interval_ms: u64,
) {
    let mut ticker = tokio::time::interval(std::time::Duration::from_millis(interval_ms.max(1)));
    ticker.tick().await; // consume the immediate first tick
    let mut unsynced = false;

    loop {
        tokio::select! {
            maybe = rx.recv() => {
                let Some(first) = maybe else { break };
                let mut batch = vec![first];
                while let Ok(next) = rx.try_recv() {
                    batch.push(next);
                }
                let write_err = write_batch(&mut file, &index, &mut offset, &batch);
                // Ack as soon as bytes are written (not yet fsynced).
                let result = match write_err {
                    None => { unsynced = true; Ok(()) }
                    Some(e) => Err(e),
                };
                ack_batch(batch, &result);
            }
            _ = ticker.tick() => {
                if unsynced {
                    let _ = file.sync_data();
                    unsynced = false;
                }
            }
        }
    }
    // Final flush on shutdown.
    if unsynced {
        let _ = file.sync_data();
    }
}
