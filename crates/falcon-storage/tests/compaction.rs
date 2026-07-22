//! WAL compaction: after compaction the on-disk WAL shrinks to a live-key
//! snapshot, all live data survives (including across a reopen), and deleted
//! keys stay gone.

use falcon_storage::{StorageEngine, WarmEngine};

#[tokio::test]
async fn compaction_shrinks_wal_and_preserves_live_data() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ks.wal");

    let engine = WarmEngine::open(&path).unwrap();

    // Churn: write the same keys many times (lots of superseded records) and
    // delete some (tombstones) — all dead weight compaction should drop.
    for round in 0..50u32 {
        for k in 0..20u32 {
            let key = format!("key{k}");
            engine
                .put(key.as_bytes(), format!("v{round}").as_bytes())
                .await
                .unwrap();
        }
    }
    for k in 0..10u32 {
        engine.delete(format!("key{k}").as_bytes()).await.unwrap();
    }

    let before = engine.durable_bytes();
    let ran = engine.compact().await.unwrap();
    assert!(ran, "compaction should have run");
    let after = engine.durable_bytes();
    assert!(
        after < before,
        "WAL should shrink after compaction: before={before} after={after}"
    );

    // Live keys (10..20) survive with their latest value; deleted keys gone.
    for k in 0..10u32 {
        assert_eq!(engine.get(format!("key{k}").as_bytes()).await.unwrap(), None);
    }
    for k in 10..20u32 {
        assert_eq!(
            engine.get(format!("key{k}").as_bytes()).await.unwrap(),
            Some(b"v49".to_vec())
        );
    }

    // Writes still work after the WAL swap.
    engine.put(b"post", b"compaction").await.unwrap();
    assert_eq!(engine.get(b"post").await.unwrap(), Some(b"compaction".to_vec()));
}

#[tokio::test]
async fn compacted_wal_reopens_correctly() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ks.wal");
    {
        let engine = WarmEngine::open(&path).unwrap();
        for k in 0..30u32 {
            engine
                .put(format!("k{k}").as_bytes(), b"old")
                .await
                .unwrap();
            engine
                .put(format!("k{k}").as_bytes(), b"new")
                .await
                .unwrap();
        }
        engine.delete(b"k0").await.unwrap();
        engine.compact().await.unwrap();
    }
    // Reopen from the compacted WAL: state is exactly the live snapshot.
    let engine = WarmEngine::open(&path).unwrap();
    assert_eq!(engine.get(b"k0").await.unwrap(), None);
    for k in 1..30u32 {
        assert_eq!(
            engine.get(format!("k{k}").as_bytes()).await.unwrap(),
            Some(b"new".to_vec())
        );
    }
}

#[tokio::test]
async fn compaction_is_safe_under_concurrent_writes() {
    use std::sync::Arc;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ks.wal");
    let engine = Arc::new(WarmEngine::open(&path).unwrap());

    // Spawn writers hammering the engine while we compact underneath them.
    let mut tasks = Vec::new();
    for w in 0..4u32 {
        let e = engine.clone();
        tasks.push(tokio::spawn(async move {
            for i in 0..200u32 {
                let key = format!("w{w}:{i}");
                e.put(key.as_bytes(), b"x").await.unwrap();
            }
        }));
    }
    // Interleave a couple of compactions.
    let _ = engine.compact().await.unwrap();
    for t in tasks {
        t.await.unwrap();
    }
    let _ = engine.compact().await.unwrap();

    // Every written key is present and readable — no lost write, no corruption.
    for w in 0..4u32 {
        for i in 0..200u32 {
            let key = format!("w{w}:{i}");
            assert_eq!(
                engine.get(key.as_bytes()).await.unwrap(),
                Some(b"x".to_vec()),
                "missing {key}"
            );
        }
    }
}
