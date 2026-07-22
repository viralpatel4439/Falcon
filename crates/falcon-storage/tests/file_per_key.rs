use falcon_storage::{FilePerKeyEngine, StorageEngine, StorageTier};

#[tokio::test]
async fn file_per_key_basic_crud() {
    let dir = tempfile::tempdir().unwrap();
    let engine = FilePerKeyEngine::open_local(dir.path()).unwrap();
    assert_eq!(engine.tier(), StorageTier::FilePerKey);

    assert_eq!(engine.get(b"foo").await.unwrap(), None);
    engine.put(b"foo", b"bar").await.unwrap();
    assert_eq!(engine.get(b"foo").await.unwrap(), Some(b"bar".to_vec()));
    engine.put(b"foo", b"baz").await.unwrap();
    assert_eq!(engine.get(b"foo").await.unwrap(), Some(b"baz".to_vec()));
    engine.delete(b"foo").await.unwrap();
    assert_eq!(engine.get(b"foo").await.unwrap(), None);
}

#[tokio::test]
async fn file_per_key_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = FilePerKeyEngine::open_local(dir.path()).unwrap();
        engine.put(b"a", b"1").await.unwrap();
        engine.put(b"b", b"2").await.unwrap();
    }
    // Reopen the same directory: each value is an independent durable file.
    let engine = FilePerKeyEngine::open_local(dir.path()).unwrap();
    assert_eq!(engine.get(b"a").await.unwrap(), Some(b"1".to_vec()));
    assert_eq!(engine.get(b"b").await.unwrap(), Some(b"2".to_vec()));
}

#[tokio::test]
async fn file_per_key_prefix_scan() {
    let dir = tempfile::tempdir().unwrap();
    let engine = FilePerKeyEngine::open_local(dir.path()).unwrap();
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
}

#[tokio::test]
async fn file_per_key_handles_arbitrary_key_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = FilePerKeyEngine::open_local(dir.path()).unwrap();
    // Keys with slashes, binary, unicode — each must map to one flat file.
    for (k, v) in [
        (&b"with/slash"[..], &b"v1"[..]),
        (b"bin\x00\xff", b"v2"),
        ("emoji-\u{1F600}".as_bytes(), b"v3"),
    ] {
        engine.put(k, v).await.unwrap();
        assert_eq!(engine.get(k).await.unwrap(), Some(v.to_vec()));
    }
    // A slash in the key must not create a subdirectory / escape the root.
    let entries: Vec<_> = std::fs::read_dir(dir.path()).unwrap().flatten().collect();
    for e in &entries {
        assert!(e.file_type().unwrap().is_file(), "keys must be flat files, no dirs");
    }
}

#[tokio::test]
async fn file_per_key_can_be_replication_target() {
    use falcon_events::{ChangeEvent, ChangeValue, Hlc};
    let dir = tempfile::tempdir().unwrap();
    let engine = FilePerKeyEngine::open_local(dir.path()).unwrap();

    // apply_replicated works (it's a valid replication *target*).
    let event = ChangeEvent {
        keyspace: "default".into(),
        key: b"k".to_vec(),
        value: ChangeValue::Put(b"v".to_vec()),
        sequence: 7,
        timestamp: 0,
        origin_region: "r1".into(),
        hlc: Hlc::zero(),
    };
    engine.apply_replicated(&event).await.unwrap();
    assert_eq!(engine.get(b"k").await.unwrap(), Some(b"v".to_vec()));
    assert_eq!(engine.last_applied_sequence(), 7);
}
