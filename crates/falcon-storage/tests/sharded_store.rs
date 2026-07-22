//! Behavioral tests for the sharded (bucket-per-hash) storage engine: the
//! object-count guarantee (N buckets, not N-keys objects), durability across
//! reopen, coalesced flushing, and correct point/prefix semantics.

use falcon_storage::{FlushPolicy, ShardedObjectStore, StorageEngine};
use std::path::Path;

fn object_count(dir: &Path) -> usize {
    std::fs::read_dir(dir)
        .map(|rd| {
            rd.flatten()
                .filter(|e| {
                    e.file_name()
                        .to_string_lossy()
                        .starts_with("bucket_")
                })
                .count()
        })
        .unwrap_or(0)
}

#[tokio::test]
async fn many_keys_map_to_bounded_object_count() {
    let dir = tempfile::tempdir().unwrap();
    // 16 buckets, sync flush: 1000 keys must never create more than 16 objects.
    let s = ShardedObjectStore::open_local(dir.path(), 16, FlushPolicy::Sync).unwrap();
    for i in 0..1000u32 {
        let k = format!("user:{i}");
        s.put(k.as_bytes(), format!("v{i}").as_bytes()).await.unwrap();
    }
    let objects = object_count(dir.path());
    assert!(
        objects <= 16,
        "expected <= 16 bucket objects for 1000 keys, got {objects}"
    );
    assert!(objects > 0, "some buckets should have been written");

    // Every key still reads back correctly (in-memory index + bucket decode).
    for i in 0..1000u32 {
        let k = format!("user:{i}");
        let v = s.get(k.as_bytes()).await.unwrap();
        assert_eq!(v, Some(format!("v{i}").into_bytes()));
    }
}

#[tokio::test]
async fn survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let s = ShardedObjectStore::open_local(dir.path(), 8, FlushPolicy::Sync).unwrap();
        s.put(b"alpha", b"1").await.unwrap();
        s.put(b"beta", b"2").await.unwrap();
        s.delete(b"alpha").await.unwrap();
    }
    // Reopen: deleted key gone, surviving key present, all from disk objects.
    let s = ShardedObjectStore::open_local(dir.path(), 8, FlushPolicy::Sync).unwrap();
    assert_eq!(s.get(b"alpha").await.unwrap(), None);
    assert_eq!(s.get(b"beta").await.unwrap(), Some(b"2".to_vec()));
}

#[tokio::test]
async fn coalesced_flush_persists_and_batches() {
    let dir = tempfile::tempdir().unwrap();
    {
        let s = ShardedObjectStore::open_local(
            dir.path(),
            4,
            FlushPolicy::Coalesce { interval_ms: 20 },
        )
        .unwrap();
        for i in 0..200u32 {
            s.put(format!("k{i}").as_bytes(), b"x").await.unwrap();
        }
        // Force a deterministic, authoritative flush rather than waiting on
        // the timer (or racing an in-flight background flush).
        s.flush_all_force().await.unwrap();
    }
    let s = ShardedObjectStore::open_local(dir.path(), 4, FlushPolicy::Sync).unwrap();
    for i in 0..200u32 {
        assert_eq!(s.get(format!("k{i}").as_bytes()).await.unwrap(), Some(b"x".to_vec()));
    }
    assert!(object_count(dir.path()) <= 4);
}

#[tokio::test]
async fn engine_flush_persists_coalesced_writes_for_shutdown() {
    let dir = tempfile::tempdir().unwrap();
    {
        let s = ShardedObjectStore::open_local(
            dir.path(),
            8,
            FlushPolicy::Coalesce { interval_ms: 100_000 }, // effectively never auto-flush
        )
        .unwrap();
        for i in 0..300u32 {
            s.put(format!("k{i}").as_bytes(), b"v").await.unwrap();
        }
        // Simulate graceful shutdown: the StorageEngine::flush path must
        // persist everything even though the timer never fired.
        StorageEngine::flush(&*s).await.unwrap();
    }
    let s = ShardedObjectStore::open_local(dir.path(), 8, FlushPolicy::Sync).unwrap();
    for i in 0..300u32 {
        assert_eq!(
            s.get(format!("k{i}").as_bytes()).await.unwrap(),
            Some(b"v".to_vec()),
            "lost k{i} across shutdown flush"
        );
    }
}

#[tokio::test]
async fn prefix_scan_spans_buckets() {
    let dir = tempfile::tempdir().unwrap();
    let s = ShardedObjectStore::open_local(dir.path(), 32, FlushPolicy::Sync).unwrap();
    s.put(b"user:1", b"a").await.unwrap();
    s.put(b"user:2", b"b").await.unwrap();
    s.put(b"order:1", b"c").await.unwrap();
    let mut users = s.scan_prefix(b"user:").await.unwrap();
    users.sort();
    assert_eq!(
        users,
        vec![
            (b"user:1".to_vec(), b"a".to_vec()),
            (b"user:2".to_vec(), b"b".to_vec())
        ]
    );
}
