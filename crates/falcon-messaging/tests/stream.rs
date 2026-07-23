//! Falcon Event Streaming tests: key-ordered partitioning, durable replay,
//! consumer-group commit/resume across reopen, and live tailing.

use falcon_messaging::{Messaging, StreamSpec};
use std::time::Duration;

fn spec(name: &str, partitions: usize) -> StreamSpec {
    StreamSpec {
        name: name.to_string(),
        partitions,
        capacity: 256,
        interval_fsync_ms: 0,
    }
}

#[tokio::test]
async fn same_key_is_ordered_on_one_partition() {
    let dir = tempfile::tempdir().unwrap();
    let m = Messaging::build(dir.path().to_path_buf(), &[], &[], &[spec("orders", 8)]).unwrap();
    let s = m.stream("orders").unwrap();

    // All records for one key go to the same partition, in append order.
    let mut partitions = std::collections::HashSet::new();
    for i in 0..5 {
        let (p, off) = s.append_keyed(b"cust:1", format!("evt{i}").into_bytes()).unwrap();
        partitions.insert(p);
        assert_eq!(off, i + 1); // offsets are 1-based and monotonic
    }
    assert_eq!(partitions.len(), 1, "one key must map to one partition");

    let p = s.partition_for(b"cust:1");
    let records = s.replay(p, 1).unwrap();
    let payloads: Vec<_> = records.iter().map(|r| r.payload.to_vec()).collect();
    assert_eq!(
        payloads,
        (0..5).map(|i| format!("evt{i}").into_bytes()).collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn consumer_group_commits_and_resumes_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let p;
    {
        let m = Messaging::build(dir.path().to_path_buf(), &[], &[], &[spec("clicks", 4)]).unwrap();
        let s = m.stream("clicks").unwrap();
        p = s.partition_for(b"session:9");
        for i in 0..4 {
            s.append_keyed(b"session:9", format!("c{i}").into_bytes()).unwrap();
        }
        // Group "analytics" processes the first two records and commits.
        let batch = s.poll("analytics", p).unwrap();
        assert_eq!(batch.len(), 4);
        s.commit("analytics", p, batch[1].offset).unwrap(); // commit through record 2
    }

    // Reopen: the group resumes AFTER its committed offset — sees only 2 left.
    let m = Messaging::build(dir.path().to_path_buf(), &[], &[], &[spec("clicks", 4)]).unwrap();
    let s = m.stream("clicks").unwrap();
    let remaining = s.poll("analytics", p).unwrap();
    let payloads: Vec<_> = remaining.iter().map(|r| r.payload.to_vec()).collect();
    assert_eq!(payloads, vec![b"c2".to_vec(), b"c3".to_vec()]);

    // A different group sees the full stream (independent cursor).
    let all = s.poll("audit", p).unwrap();
    assert_eq!(all.len(), 4);
}

#[tokio::test]
async fn live_tail_receives_new_records() {
    let dir = tempfile::tempdir().unwrap();
    let m = Messaging::build(dir.path().to_path_buf(), &[], &[], &[spec("live", 1)]).unwrap();
    let s = m.stream("live").unwrap();
    let mut rx = s.subscribe(0).unwrap();

    s.append_to(0, b"hello".to_vec()).unwrap();
    let got = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("timely delivery")
        .expect("record");
    assert_eq!(got.payload.to_vec(), b"hello".to_vec());
    assert_eq!(got.offset, 1);
}
