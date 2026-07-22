use falcon_storage::{ColdEngine, HotEngine, StorageEngine, WarmEngine};
use std::sync::Arc;
use std::time::Duration;

/// Many concurrent writers hammer the *same* key. The engine must never
/// lose a write or apply them out of the order they actually completed:
/// the last write to actually land must be exactly what `get` returns, and
/// the winning value's sequence must be the highest one that was written.
async fn same_key_writes_are_serialized_and_lossless(engine: Arc<dyn StorageEngine>) {
    const WRITERS: usize = 50;
    let mut handles = Vec::new();
    for i in 0..WRITERS {
        let engine = engine.clone();
        handles.push(tokio::spawn(async move {
            engine
                .put(b"shared-key", format!("value-{i}").as_bytes())
                .await
                .unwrap()
        }));
    }

    let mut sequences: Vec<u64> = Vec::new();
    for h in handles {
        sequences.push(h.await.unwrap());
    }

    // No two writers should ever have been assigned the same sequence
    // number for the same key (that would mean they weren't serialized).
    let mut sorted = sequences.clone();
    sorted.sort_unstable();
    sorted.dedup();
    assert_eq!(sorted.len(), WRITERS, "sequence numbers must be unique: {sequences:?}");

    // Whichever write actually has the highest sequence must be the value
    // left in the store — no lost update from reordered map mutations.
    let max_seq = *sorted.last().unwrap();
    let winner_index = sequences.iter().position(|&s| s == max_seq).unwrap();
    let expected_value = format!("value-{winner_index}");

    let stored = engine.get(b"shared-key").await.unwrap().unwrap();
    assert_eq!(
        String::from_utf8(stored).unwrap(),
        expected_value,
        "value left in store must match the write with the highest sequence"
    );
}

#[tokio::test]
async fn hot_engine_same_key_concurrent_writes() {
    same_key_writes_are_serialized_and_lossless(Arc::new(HotEngine::new())).await;
}

#[tokio::test]
async fn warm_engine_same_key_concurrent_writes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = WarmEngine::open(&dir.path().join("test.wal")).unwrap();
    same_key_writes_are_serialized_and_lossless(Arc::new(engine)).await;
}

#[tokio::test]
async fn cold_engine_same_key_concurrent_writes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = ColdEngine::open(&dir.path().join("db")).unwrap();
    same_key_writes_are_serialized_and_lossless(Arc::new(engine)).await;
}

/// Proves the actual concurrency property at the source: holding a
/// per-key lock for one key must NOT block acquiring the lock for a
/// different key. This is what makes "different keys write concurrently"
/// true for every engine built on `KeyLockTable`, independent of how fast
/// or slow any particular engine's own critical section happens to be.
#[tokio::test]
async fn different_keys_do_not_block_each_other() {
    let locks = Arc::new(falcon_storage::KeyLockTable::new());

    // Hold key "a" for a while...
    let locks_a = locks.clone();
    let holder = tokio::spawn(async move {
        let _guard = locks_a.lock(b"a").await;
        tokio::time::sleep(Duration::from_millis(300)).await;
    });

    // ...meanwhile, key "b" must be acquirable quickly, not after "a" is released.
    tokio::time::sleep(Duration::from_millis(20)).await; // let the holder grab its lock first
    let start = tokio::time::Instant::now();
    let _guard_b = locks.lock(b"b").await;
    let elapsed = start.elapsed();

    assert!(
        elapsed < Duration::from_millis(100),
        "acquiring a different key's lock should not wait on an unrelated key's lock, took {elapsed:?}"
    );

    holder.await.unwrap();
}

/// The complementary property: repeated writes to the *same* key really do
/// queue — a second acquisition of the same key's lock must wait for the
/// first to release, not proceed concurrently.
#[tokio::test]
async fn same_key_lock_is_exclusive() {
    let locks = Arc::new(falcon_storage::KeyLockTable::new());

    let locks2 = locks.clone();
    let holder = tokio::spawn(async move {
        let _guard = locks2.lock(b"same").await;
        tokio::time::sleep(Duration::from_millis(200)).await;
    });

    tokio::time::sleep(Duration::from_millis(20)).await;
    let start = tokio::time::Instant::now();
    let _guard = locks.lock(b"same").await;
    let elapsed = start.elapsed();

    assert!(
        elapsed >= Duration::from_millis(150),
        "acquiring the same key's lock should wait for the first holder to release, took {elapsed:?}"
    );

    holder.await.unwrap();
}

/// The exact scenario: writes fire concurrently as key1, key1, key2, key1.
/// Key "2" must finish quickly regardless of how backed up key "1"'s
/// three-deep queue is — it has its own independent lock and zero queue
/// depth from key "1"'s perspective. The three key "1" writes must
/// complete strictly one at a time (their completion timestamps must be
/// spaced apart by roughly the artificial per-write delay), while key "2"
/// finishes near-instantly, sometime in the middle of that sequence.
#[tokio::test]
async fn key1_key1_key2_key1_key2_is_independent_of_key1_queue() {
    let locks = Arc::new(falcon_storage::KeyLockTable::new());
    const WRITE_DELAY: Duration = Duration::from_millis(100);

    async fn timed_write(locks: Arc<falcon_storage::KeyLockTable>, key: &'static [u8], label: &'static str, start: tokio::time::Instant) -> (&'static str, Duration) {
        let _guard = locks.lock(key).await;
        tokio::time::sleep(WRITE_DELAY).await;
        (label, start.elapsed())
    }

    let start = tokio::time::Instant::now();
    let (r1a, r1b, r2, r1c) = tokio::join!(
        timed_write(locks.clone(), b"1", "1a", start),
        timed_write(locks.clone(), b"1", "1b", start),
        timed_write(locks.clone(), b"2", "2", start),
        timed_write(locks.clone(), b"1", "1c", start),
    );

    // Key "2" is fully independent: it must finish in roughly one
    // write's time, not after waiting behind any of key "1"'s writes.
    assert!(
        r2.1 < WRITE_DELAY * 2,
        "key 2 should not queue behind key 1's writes at all, took {:?}",
        r2.1
    );

    // Key "1"'s three writes share one queue: the total time for all
    // three to complete must be roughly 3x one write's delay (serialized),
    // not ~1x (which would mean they ran concurrently with each other).
    let key1_finish_times = [r1a.1, r1b.1, r1c.1];
    let last_key1_finish = key1_finish_times.iter().max().unwrap();
    assert!(
        *last_key1_finish >= WRITE_DELAY * 3 - Duration::from_millis(30),
        "key 1's three writes should be serialized (~3x one write's delay), \
         last one finished at {last_key1_finish:?}"
    );

    println!("key 1 writes finished at: {r1a:?} {r1b:?} {r1c:?}; key 2 finished at: {r2:?}");
}
