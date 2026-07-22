use falcon_storage::{StorageEngine, StorageTier, TieredEngine};
use std::sync::Arc;

const MB: usize = 1024 * 1024;

#[tokio::test]
async fn tiered_basic_crud() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TieredEngine::open(&dir.path().join("db"), 64 * MB, 8).unwrap();
    assert_eq!(engine.tier(), StorageTier::Tiered);

    assert_eq!(engine.get(b"foo").await.unwrap(), None);
    engine.put(b"foo", b"bar").await.unwrap();
    assert_eq!(engine.get(b"foo").await.unwrap(), Some(b"bar".to_vec()));
    engine.put(b"foo", b"baz").await.unwrap();
    assert_eq!(engine.get(b"foo").await.unwrap(), Some(b"baz".to_vec()));
    engine.delete(b"foo").await.unwrap();
    assert_eq!(engine.get(b"foo").await.unwrap(), None);
}

#[tokio::test]
async fn tiered_data_survives_restart_via_cold() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("db");

    {
        let engine = TieredEngine::open(&db_path, 64 * MB, 8).unwrap();
        engine.put(b"a", b"1").await.unwrap();
        engine.put(b"b", b"2").await.unwrap();
        // stats sanity: both writes cached hot
        let stats = engine.stats();
        assert_eq!(stats.hot_keys, 2);
    }

    // Reopen: hot cache is empty, but data is durable in the cold store and
    // is promoted back on read.
    let engine = TieredEngine::open(&db_path, 64 * MB, 8).unwrap();
    assert_eq!(engine.get(b"a").await.unwrap(), Some(b"1".to_vec()));
    assert_eq!(engine.get(b"b").await.unwrap(), Some(b"2".to_vec()));
}

#[tokio::test]
async fn tiered_holds_more_than_hot_budget_with_no_data_loss() {
    let dir = tempfile::tempdir().unwrap();
    // Tiny hot budget (64 KB) but write far more than that — proves the
    // dataset can exceed the RAM budget and every key stays readable
    // (promoted back from the durable cold store on access).
    let engine = TieredEngine::open(&dir.path().join("db"), 64 * 1024, 8).unwrap();

    const N: usize = 2000;
    let value = vec![b'x'; 256]; // ~256B each -> ~512KB total, 8x the budget
    for i in 0..N {
        engine
            .put(format!("k{i}").as_bytes(), &value)
            .await
            .unwrap();
    }

    // Hot footprint must stay near the budget, not grow to the full dataset.
    let stats = engine.stats();
    assert!(
        stats.hot_bytes <= 64 * 1024 * 3,
        "hot_bytes {} should stay near the 64KB budget, not hold the whole dataset",
        stats.hot_bytes
    );
    assert!(stats.evictions > 0, "eviction should have run when over budget");

    // Every single key must still be readable (from cold, promoting back).
    for i in 0..N {
        assert_eq!(
            engine.get(format!("k{i}").as_bytes()).await.unwrap(),
            Some(value.clone()),
            "key k{i} lost"
        );
    }
}

#[tokio::test]
async fn tiered_reports_hit_rate() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TieredEngine::open(&dir.path().join("db"), 64 * MB, 8).unwrap();

    engine.put(b"hot", b"v").await.unwrap();
    // Repeated reads of a hot key -> mostly hot hits.
    for _ in 0..100 {
        engine.get(b"hot").await.unwrap();
    }
    let stats = engine.stats();
    assert!(stats.hot_hits >= 100);
    assert!(stats.hit_rate() > 0.9, "hit rate was {}", stats.hit_rate());
}

#[tokio::test]
async fn tiered_same_key_concurrent_writes_lossless() {
    let dir = tempfile::tempdir().unwrap();
    let engine: Arc<dyn StorageEngine> =
        Arc::new(TieredEngine::open(&dir.path().join("db"), 64 * MB, 8).unwrap());

    const WRITERS: usize = 50;
    let mut handles = Vec::new();
    for i in 0..WRITERS {
        let engine = engine.clone();
        handles.push(tokio::spawn(async move {
            engine
                .put(b"shared", format!("value-{i}").as_bytes())
                .await
                .unwrap()
        }));
    }
    let mut sequences = Vec::new();
    for h in handles {
        sequences.push(h.await.unwrap());
    }

    // Unique sequence numbers => writes were serialized, none lost.
    let mut sorted = sequences.clone();
    sorted.sort_unstable();
    sorted.dedup();
    assert_eq!(sorted.len(), WRITERS, "sequences must be unique: {sequences:?}");

    // The value with the highest sequence must be what's stored.
    let max_seq = *sorted.last().unwrap();
    let winner = sequences.iter().position(|&s| s == max_seq).unwrap();
    assert_eq!(
        engine.get(b"shared").await.unwrap(),
        Some(format!("value-{winner}").into_bytes())
    );
}
