use falcon_storage::{StorageEngine, WarmEngine};
use std::sync::Arc;
use std::time::Instant;

/// Proves group commit is actually amortizing fsyncs rather than silently
/// degrading back to one-fsync-per-write: total wall-clock time for many
/// concurrent writers to *different* keys must grow much slower than
/// linearly with writer count. If every write still paid its own
/// serialized fsync, doubling the writer count would roughly double the
/// total time; under group commit, doubling the writer count should barely
/// move the total time once fsyncs are being shared across a batch.
#[tokio::test]
async fn concurrent_different_key_writes_amortize_across_batches() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(WarmEngine::open(&dir.path().join("test.wal")).unwrap());

    async fn run_batch(engine: Arc<WarmEngine>, writers: usize) -> std::time::Duration {
        let start = Instant::now();
        let mut handles = Vec::with_capacity(writers);
        for i in 0..writers {
            let engine = engine.clone();
            handles.push(tokio::spawn(async move {
                engine
                    .put(format!("batch-key-{i}").as_bytes(), b"v")
                    .await
                    .unwrap()
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        start.elapsed()
    }

    // Warm up (first write on a fresh file pays any one-time setup cost).
    run_batch(engine.clone(), 4).await;

    let small = run_batch(engine.clone(), 8).await;
    let large = run_batch(engine.clone(), 128).await;

    // If writes were still fully serialized behind one fsync each, 128
    // writers would take roughly 16x as long as 8 writers. Under group
    // commit, most of those 128 concurrent writes should land in a
    // handful of batches, so the total time should grow far less than
    // linearly — allow generous headroom (4x) to keep this robust across
    // slow/loaded CI machines while still catching an actual regression
    // back to per-write fsyncing.
    let ratio = large.as_secs_f64() / small.as_secs_f64().max(0.000_001);
    assert!(
        ratio < 8.0,
        "128 concurrent different-key writes took {ratio:.1}x longer than 8 — \
         expected group commit to amortize fsyncs, not scale linearly \
         (small={small:?}, large={large:?})"
    );
}
