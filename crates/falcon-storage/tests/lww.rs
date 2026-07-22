use falcon_events::{ChangeEvent, ChangeValue, Hlc};
use falcon_storage::{StorageEngine, WarmEngine};

fn put_event(key: &[u8], value: &[u8], wall: u64, logical: u32, region: &str) -> ChangeEvent {
    ChangeEvent {
        keyspace: "default".into(),
        key: key.to_vec(),
        value: ChangeValue::Put(value.to_vec()),
        sequence: 0,
        timestamp: 0,
        origin_region: region.into(),
        hlc: Hlc { wall, logical, region: region.into() },
    }
}

fn delete_event(key: &[u8], wall: u64, logical: u32, region: &str) -> ChangeEvent {
    ChangeEvent {
        keyspace: "default".into(),
        key: key.to_vec(),
        value: ChangeValue::Delete,
        sequence: 0,
        timestamp: 0,
        origin_region: region.into(),
        hlc: Hlc { wall, logical, region: region.into() },
    }
}

#[tokio::test]
async fn higher_hlc_wins_regardless_of_apply_order() {
    // Two regions write the same key with different HLCs. Whichever order
    // they arrive, the higher HLC must win — this is convergence.
    let dir = tempfile::tempdir().unwrap();
    let a = WarmEngine::open(&dir.path().join("a.wal")).unwrap();
    let dir2 = tempfile::tempdir().unwrap();
    let b = WarmEngine::open(&dir2.path().join("b.wal")).unwrap();

    let low = put_event(b"k", b"from-region-1", 100, 0, "region-1");
    let high = put_event(b"k", b"from-region-2", 200, 0, "region-2");

    // Engine A applies low then high.
    assert!(a.apply_lww(&low).await.unwrap());
    assert!(a.apply_lww(&high).await.unwrap());

    // Engine B applies high then low (reverse order).
    assert!(b.apply_lww(&high).await.unwrap());
    assert!(!b.apply_lww(&low).await.unwrap()); // low loses, not applied

    // Both converge to the higher-HLC value.
    assert_eq!(a.get(b"k").await.unwrap(), Some(b"from-region-2".to_vec()));
    assert_eq!(b.get(b"k").await.unwrap(), Some(b"from-region-2".to_vec()));
}

#[tokio::test]
async fn concurrent_same_wall_resolves_by_region_deterministically() {
    // Same wall+logical, different regions -> region breaks the tie the
    // same way everywhere, so all replicas agree.
    let dir = tempfile::tempdir().unwrap();
    let eng = WarmEngine::open(&dir.path().join("x.wal")).unwrap();

    let ra = put_event(b"k", b"A", 50, 0, "aaa");
    let rb = put_event(b"k", b"B", 50, 0, "bbb"); // "bbb" > "aaa"

    eng.apply_lww(&ra).await.unwrap();
    eng.apply_lww(&rb).await.unwrap();
    // bbb wins the tiebreak.
    assert_eq!(eng.get(b"k").await.unwrap(), Some(b"B".to_vec()));

    // Reverse order on a fresh engine -> same winner.
    let dir2 = tempfile::tempdir().unwrap();
    let eng2 = WarmEngine::open(&dir2.path().join("y.wal")).unwrap();
    eng2.apply_lww(&rb).await.unwrap();
    assert!(!eng2.apply_lww(&ra).await.unwrap());
    assert_eq!(eng2.get(b"k").await.unwrap(), Some(b"B".to_vec()));
}

#[tokio::test]
async fn delete_and_put_resolve_by_hlc() {
    let dir = tempfile::tempdir().unwrap();
    let eng = WarmEngine::open(&dir.path().join("z.wal")).unwrap();

    // A put at wall=100, then a delete at wall=200 -> key is gone.
    eng.apply_lww(&put_event(b"k", b"v", 100, 0, "r1")).await.unwrap();
    eng.apply_lww(&delete_event(b"k", 200, 0, "r1")).await.unwrap();
    assert_eq!(eng.get(b"k").await.unwrap(), None);

    // A LATE put at wall=150 (older than the delete) must lose -> still gone.
    assert!(!eng.apply_lww(&put_event(b"k", b"resurrected", 150, 0, "r2")).await.unwrap());
    assert_eq!(eng.get(b"k").await.unwrap(), None);

    // A NEWER put at wall=300 wins over the delete.
    assert!(eng.apply_lww(&put_event(b"k", b"alive-again", 300, 0, "r1")).await.unwrap());
    assert_eq!(eng.get(b"k").await.unwrap(), Some(b"alive-again".to_vec()));
}

#[tokio::test]
async fn lww_is_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let eng = WarmEngine::open(&dir.path().join("i.wal")).unwrap();

    let e = put_event(b"k", b"v", 100, 0, "r1");
    assert!(eng.apply_lww(&e).await.unwrap());
    // Re-applying the exact same event is a no-op (not strictly greater).
    assert!(!eng.apply_lww(&e).await.unwrap());
    assert!(!eng.apply_lww(&e).await.unwrap());
    assert_eq!(eng.get(b"k").await.unwrap(), Some(b"v".to_vec()));
}

#[tokio::test]
async fn hlc_and_lww_survive_restart() {
    let dir = tempfile::tempdir().unwrap();
    let wal = dir.path().join("r.wal");

    {
        let eng = WarmEngine::open(&wal).unwrap();
        eng.apply_lww(&put_event(b"k", b"v-at-200", 200, 0, "r1")).await.unwrap();
    }

    // Reopen: the stored HLC is rebuilt from the WAL, so an older write
    // still correctly loses after restart (durable LWW ordering).
    let eng = WarmEngine::open(&wal).unwrap();
    assert_eq!(eng.stored_hlc(b"k").unwrap().wall, 200);
    assert!(!eng.apply_lww(&put_event(b"k", b"older", 150, 0, "r2")).await.unwrap());
    assert_eq!(eng.get(b"k").await.unwrap(), Some(b"v-at-200".to_vec()));
}
