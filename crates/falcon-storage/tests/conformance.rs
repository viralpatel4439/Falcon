use falcon_events::{ChangeEvent, ChangeValue};
use falcon_storage::{ColdEngine, HotEngine, StorageEngine, StorageTier, WarmEngine};
use std::sync::Arc;

async fn conformance_suite(engine: Arc<dyn StorageEngine>) {
    assert_eq!(engine.get(b"foo").await.unwrap(), None);

    let seq1 = engine.put(b"foo", b"bar").await.unwrap();
    assert_eq!(engine.get(b"foo").await.unwrap(), Some(b"bar".to_vec()));

    let seq2 = engine.put(b"foo", b"baz").await.unwrap();
    assert!(seq2 > seq1);
    assert_eq!(engine.get(b"foo").await.unwrap(), Some(b"baz".to_vec()));

    engine.put(b"user:1", b"alice").await.unwrap();
    engine.put(b"user:2", b"bob").await.unwrap();
    engine.put(b"other", b"x").await.unwrap();

    let mut scanned = engine.scan_prefix(b"user:").await.unwrap();
    scanned.sort();
    assert_eq!(
        scanned,
        vec![
            (b"user:1".to_vec(), b"alice".to_vec()),
            (b"user:2".to_vec(), b"bob".to_vec()),
        ]
    );

    let seq3 = engine.delete(b"foo").await.unwrap();
    assert!(seq3 > seq2);
    assert_eq!(engine.get(b"foo").await.unwrap(), None);

    assert_eq!(engine.last_applied_sequence(), seq3);
}

#[tokio::test]
async fn hot_engine_conformance() {
    let engine: Arc<dyn StorageEngine> = Arc::new(HotEngine::new());
    assert_eq!(engine.tier(), StorageTier::Hot);
    conformance_suite(engine).await;
}

#[tokio::test]
async fn warm_engine_conformance() {
    let dir = tempfile::tempdir().unwrap();
    let engine: Arc<dyn StorageEngine> =
        Arc::new(WarmEngine::open(&dir.path().join("test.wal")).unwrap());
    assert_eq!(engine.tier(), StorageTier::Warm);
    conformance_suite(engine).await;
}

#[tokio::test]
async fn cold_engine_conformance() {
    let dir = tempfile::tempdir().unwrap();
    let engine: Arc<dyn StorageEngine> = Arc::new(ColdEngine::open(&dir.path().join("db")).unwrap());
    assert_eq!(engine.tier(), StorageTier::Cold);
    conformance_suite(engine).await;
}

#[tokio::test]
async fn hot_engine_rejects_replication() {
    let engine = HotEngine::new();
    let event = ChangeEvent {
        keyspace: "default".into(),
        key: b"k".to_vec(),
        value: ChangeValue::Put(b"v".to_vec()),
        sequence: 1,
        timestamp: 0,
        origin_region: "local".into(),
        hlc: falcon_events::Hlc::zero(),
    };
    assert!(engine.apply_replicated(&event).await.is_err());
}

#[tokio::test]
async fn warm_engine_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("test.wal");

    {
        let engine = WarmEngine::open(&wal_path).unwrap();
        engine.put(b"a", b"1").await.unwrap();
        engine.put(b"b", b"2").await.unwrap();
        engine.delete(b"a").await.unwrap();
    } // engine dropped, simulating restart

    let engine = WarmEngine::open(&wal_path).unwrap();
    assert_eq!(engine.get(b"a").await.unwrap(), None);
    assert_eq!(engine.get(b"b").await.unwrap(), Some(b"2".to_vec()));
    assert_eq!(engine.last_applied_sequence(), 3);
}

#[tokio::test]
async fn hot_engine_does_not_survive_restart() {
    // Not a literal restart test (hot has no persistence to reload from),
    // this documents the guarantee: a fresh HotEngine never sees prior data.
    let engine1 = HotEngine::new();
    engine1.put(b"a", b"1").await.unwrap();
    drop(engine1);

    let engine2 = HotEngine::new();
    assert_eq!(engine2.get(b"a").await.unwrap(), None);
}

#[tokio::test]
async fn cold_engine_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("db");

    {
        let engine = ColdEngine::open(&db_path).unwrap();
        engine.put(b"a", b"1").await.unwrap();
        engine.put(b"b", b"2").await.unwrap();
        engine.flush().unwrap();
    }

    let engine = ColdEngine::open(&db_path).unwrap();
    assert_eq!(engine.get(b"a").await.unwrap(), Some(b"1".to_vec()));
    assert_eq!(engine.last_applied_sequence(), 2);
}

#[tokio::test]
async fn warm_engine_apply_replicated_is_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let engine = WarmEngine::open(&dir.path().join("test.wal")).unwrap();

    let event = ChangeEvent {
        keyspace: "default".into(),
        key: b"k".to_vec(),
        value: ChangeValue::Put(b"v".to_vec()),
        sequence: 5,
        timestamp: 0,
        origin_region: "region-a".into(),
        hlc: falcon_events::Hlc::zero(),
    };
    engine.apply_replicated(&event).await.unwrap();
    assert_eq!(engine.last_applied_sequence(), 5);
    assert_eq!(engine.get(b"k").await.unwrap(), Some(b"v".to_vec()));

    // Re-applying the same (or older) sequence must be a no-op.
    engine.apply_replicated(&event).await.unwrap();
    assert_eq!(engine.last_applied_sequence(), 5);
}
