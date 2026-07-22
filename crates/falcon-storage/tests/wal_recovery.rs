use falcon_storage::{StorageEngine, WarmEngine};
use std::io::Write;

#[tokio::test]
async fn wal_replay_truncates_partial_trailing_record() {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("test.wal");

    {
        let engine = WarmEngine::open(&wal_path).unwrap();
        engine.put(b"a", b"1").await.unwrap();
        engine.put(b"b", b"2").await.unwrap();
    }

    // Simulate a crash mid-write: append a truncated/garbage record.
    {
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&wal_path)
            .unwrap();
        // Claims a large length but body is short — a partial write from a crash.
        file.write_all(&[0, 0, 0, 100]).unwrap();
        file.write_all(b"not enough bytes").unwrap();
    }

    // Recovery must succeed, ignoring the partial trailing record, keeping
    // both prior valid records intact.
    let engine = WarmEngine::open(&wal_path).unwrap();
    assert_eq!(engine.get(b"a").await.unwrap(), Some(b"1".to_vec()));
    assert_eq!(engine.get(b"b").await.unwrap(), Some(b"2".to_vec()));
    assert_eq!(engine.last_applied_sequence(), 2);
}

/// Under group commit, several unrelated writers' records can be
/// `write_all`'d in the same batch before the batch's single fsync
/// completes. A crash in that window (some whole records physically
/// written, then nothing more, or a partial next one) must be recovered
/// identically to a crash mid single-write: replay everything parseable,
/// stop cleanly at the first thing that isn't. This directly exercises
/// that "crash mid-batch" and "crash mid-single-write" are the same code
/// path on disk.
#[tokio::test]
async fn wal_replay_recovers_up_to_last_fully_written_record_in_a_batch() {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("test.wal");

    {
        let engine = WarmEngine::open(&wal_path).unwrap();
        engine.put(b"x", b"1").await.unwrap();
        engine.put(b"y", b"2").await.unwrap();
        engine.put(b"z", b"3").await.unwrap();
    }

    // Simulate a crash mid-batch: two more whole, well-formed records
    // land (as if write_all'd for keys in the same batch) followed by a
    // partial one (the batch's fsync never completed, and the process
    // died mid-write of a later record).
    {
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&wal_path)
            .unwrap();
        for (seq, key, value) in [(4u64, b"p".as_slice(), b"4".as_slice()), (5u64, b"q", b"5")] {
            let mut buf = Vec::new();
            buf.extend_from_slice(&seq.to_be_bytes());
            buf.extend_from_slice(&0u128.to_be_bytes());
            buf.push(1); // OP_PUT
            buf.extend_from_slice(&(key.len() as u32).to_be_bytes());
            buf.extend_from_slice(key);
            buf.extend_from_slice(&(value.len() as u32).to_be_bytes());
            buf.extend_from_slice(value);
            file.write_all(&(buf.len() as u32).to_be_bytes()).unwrap();
            file.write_all(&buf).unwrap();
        }
        // Partial trailing record: claims more bytes than actually follow.
        file.write_all(&[0, 0, 0, 200]).unwrap();
        file.write_all(b"truncated").unwrap();
    }

    let engine = WarmEngine::open(&wal_path).unwrap();
    // Original three records, plus both fully-written batch records.
    assert_eq!(engine.get(b"x").await.unwrap(), Some(b"1".to_vec()));
    assert_eq!(engine.get(b"y").await.unwrap(), Some(b"2".to_vec()));
    assert_eq!(engine.get(b"z").await.unwrap(), Some(b"3".to_vec()));
    assert_eq!(engine.get(b"p").await.unwrap(), Some(b"4".to_vec()));
    assert_eq!(engine.get(b"q").await.unwrap(), Some(b"5".to_vec()));
    assert_eq!(engine.last_applied_sequence(), 5);
}
