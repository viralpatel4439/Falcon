//! Regression test: under CONCURRENT writes to different keys, the warm
//! engine's replication log (`read_replog_from`) must return every write with
//! no gaps, and sequences must be contiguous. This guards the bug where
//! sequence allocation and WAL enqueue were not atomic, so the on-disk file
//! order didn't match sequence order — which stranded a replication follower's
//! sparse-index catch-up and silently dropped writes.

use falcon_storage::{StorageEngine, WarmEngine};
use std::sync::Arc;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replog_has_no_gaps_under_concurrent_writes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(WarmEngine::open(&dir.path().join("ks.wal")).unwrap());

    // Many concurrent writers, each on its own keys (different keys => the
    // per-key lock does NOT serialize them, so sequence/enqueue ordering is
    // exercised).
    const WRITERS: usize = 16;
    const PER: usize = 250;
    let mut tasks = Vec::new();
    for w in 0..WRITERS {
        let e = engine.clone();
        tasks.push(tokio::spawn(async move {
            for i in 0..PER {
                e.put(format!("w{w}:{i}").as_bytes(), b"v").await.unwrap();
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }

    let total = (WRITERS * PER) as u64;

    // The replication log from 0 must contain EVERY sequence 1..=total exactly
    // once, contiguous — this is what a follower streams and applies.
    let events = engine.read_replog_from(0).unwrap();
    assert_eq!(
        events.len() as u64,
        total,
        "replog returned {} events, expected {total}",
        events.len()
    );

    let mut seqs: Vec<u64> = events.iter().map(|e| e.sequence).collect();
    seqs.sort_unstable();
    for (i, s) in seqs.iter().enumerate() {
        assert_eq!(*s, (i as u64) + 1, "sequence gap/duplicate at index {i}: got {s}");
    }

    // And a mid-stream resume (what a caught-up follower does) returns exactly
    // the tail, in order, with no gaps.
    let from = total / 2;
    let tail = engine.read_replog_from(from).unwrap();
    assert_eq!(tail.len() as u64, total - from);
    for e in &tail {
        assert!(e.sequence > from);
    }
}
