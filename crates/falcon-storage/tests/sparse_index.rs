use falcon_storage::{StorageEngine, WarmEngine};

/// Populates a WAL with many records, then checks `read_replog_from`
/// (index-assisted) returns exactly what a byte-0 full replay would, for a
/// range of `from` values: before the first sample, exactly at a sample
/// boundary, mid-way between samples, and past the last write.
#[tokio::test]
async fn read_replog_from_matches_full_scan_across_boundaries() {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("test.wal");
    let engine = WarmEngine::open(&wal_path).unwrap();

    // More than a few multiples of the sparse index stride (64), so we
    // exercise several sample points, not just the first one.
    const N: u64 = 200;
    for i in 0..N {
        engine
            .put(format!("k{i}").as_bytes(), format!("v{i}").as_bytes())
            .await
            .unwrap();
    }

    for &from in &[0u64, 1, 63, 64, 65, 100, 128, 129, 199, 200, 500] {
        let index_assisted = engine.read_replog_from(from).unwrap();

        // Force a full scan independently via the raw WAL replay, filtered
        // the same way `read_replog_from` filters, as the ground truth.
        let full = falcon_storage::Wal::replay(&wal_path).unwrap();
        let expected: Vec<_> = full.into_iter().filter(|r| r.sequence > from).collect();

        assert_eq!(
            index_assisted.len(),
            expected.len(),
            "mismatch at from={from}: index-assisted returned {} entries, full scan returned {}",
            index_assisted.len(),
            expected.len()
        );
        for (got, want) in index_assisted.iter().zip(expected.iter()) {
            assert_eq!(got.sequence, want.sequence, "sequence mismatch at from={from}");
            assert_eq!(got.key, want.key, "key mismatch at from={from}");
        }
    }
}

/// `read_replog_from` must still return correct data even when the sparse
/// index cannot be trusted (e.g. after some inconsistency) — it should
/// fall back to a full scan rather than ever return wrong/misaligned data.
/// We exercise this indirectly: a follower reconnecting with a `from` far
/// beyond anything indexed (larger than the last real sequence) must
/// return an empty result, not garbage or a panic, on both the
/// index-assisted and fallback paths.
#[tokio::test]
async fn read_replog_from_beyond_last_sequence_is_empty_not_garbage() {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("test.wal");
    let engine = WarmEngine::open(&wal_path).unwrap();

    for i in 0..10u64 {
        engine.put(format!("k{i}").as_bytes(), b"v").await.unwrap();
    }

    let result = engine.read_replog_from(9_999).unwrap();
    assert!(result.is_empty(), "expected no entries beyond the last written sequence");
}

/// A brand-new (empty) WAL must handle catch-up requests cleanly — no
/// index entries yet, must not panic, must return empty.
#[tokio::test]
async fn read_replog_from_on_empty_wal_is_empty() {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("test.wal");
    let engine = WarmEngine::open(&wal_path).unwrap();

    let result = engine.read_replog_from(0).unwrap();
    assert!(result.is_empty());
}
