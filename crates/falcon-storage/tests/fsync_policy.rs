use falcon_storage::{FsyncPolicy, StorageEngine, WarmEngine};

#[tokio::test]
async fn interval_fsync_still_serves_reads_and_recovers() {
    let dir = tempfile::tempdir().unwrap();
    let wal = dir.path().join("test.wal");

    {
        let engine = WarmEngine::open_with_policy(&wal, FsyncPolicy::IntervalMs(10)).unwrap();
        for i in 0..100 {
            engine
                .put(format!("k{i}").as_bytes(), format!("v{i}").as_bytes())
                .await
                .unwrap();
        }
        // Reads reflect writes immediately (map updated regardless of fsync).
        assert_eq!(engine.get(b"k42").await.unwrap(), Some(b"v42".to_vec()));
        // Give the interval fsync a chance to flush before dropping.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    // Reopen: the flushed data is recovered from the WAL.
    let engine = WarmEngine::open(&wal).unwrap();
    assert_eq!(engine.get(b"k0").await.unwrap(), Some(b"v0".to_vec()));
    assert_eq!(engine.get(b"k99").await.unwrap(), Some(b"v99".to_vec()));
}

#[tokio::test]
async fn always_policy_is_durable_immediately() {
    let dir = tempfile::tempdir().unwrap();
    let wal = dir.path().join("test.wal");

    {
        let engine = WarmEngine::open_with_policy(&wal, FsyncPolicy::Always).unwrap();
        engine.put(b"durable", b"yes").await.unwrap();
        // No sleep: Always fsyncs before the put returns, so it's on disk.
    }

    let engine = WarmEngine::open(&wal).unwrap();
    assert_eq!(engine.get(b"durable").await.unwrap(), Some(b"yes".to_vec()));
}
