use falcon_messaging::{Messaging, QueueSpec, TopicMode, TopicSpec};
use std::time::Duration;

fn topic_spec(name: &str, mode: TopicMode) -> TopicSpec {
    TopicSpec {
        name: name.to_string(),
        mode,
        capacity: 1024,
    }
}

fn queue_spec(name: &str, ack_ms: u64) -> QueueSpec {
    QueueSpec {
        name: name.to_string(),
        ack_timeout: Duration::from_millis(ack_ms),
    }
}

#[tokio::test]
async fn ephemeral_topic_fans_out_to_live_subscribers() {
    let dir = tempfile::tempdir().unwrap();
    let m = Messaging::build(
        dir.path().to_path_buf(),
        &[topic_spec("live", TopicMode::Ephemeral)],
        &[],
        &[],
    )
    .unwrap();
    let topic = m.topic("live").unwrap();

    let mut sub1 = topic.subscribe();
    let mut sub2 = topic.subscribe();

    topic.publish(b"hello".to_vec()).unwrap();

    let d1 = sub1.recv().await.unwrap();
    let d2 = sub2.recv().await.unwrap();
    assert_eq!(&d1.payload[..], b"hello");
    assert_eq!(&d2.payload[..], b"hello"); // both subscribers get it
}

#[tokio::test]
async fn durable_topic_survives_restart_and_replays() {
    let dir = tempfile::tempdir().unwrap();
    let specs = [topic_spec("events", TopicMode::Durable)];

    {
        let m = Messaging::build(dir.path().to_path_buf(), &specs, &[], &[]).unwrap();
        let t = m.topic("events").unwrap();
        t.publish(b"a".to_vec()).unwrap();
        t.publish(b"b".to_vec()).unwrap();
        t.publish(b"c".to_vec()).unwrap();
    }

    // Reopen: a subscriber that was offline can replay from the start.
    let m = Messaging::build(dir.path().to_path_buf(), &specs, &[], &[]).unwrap();
    let t = m.topic("events").unwrap();
    let replayed = t.replay_from(1).unwrap();
    let payloads: Vec<_> = replayed.iter().map(|d| d.payload.to_vec()).collect();
    assert_eq!(payloads, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
}

#[tokio::test]
async fn ephemeral_topic_has_no_durable_replay() {
    let dir = tempfile::tempdir().unwrap();
    let m = Messaging::build(
        dir.path().to_path_buf(),
        &[topic_spec("live", TopicMode::Ephemeral)],
        &[],
        &[],
    )
    .unwrap();
    let t = m.topic("live").unwrap();
    t.publish(b"x".to_vec()).unwrap();
    assert!(t.replay_from(1).unwrap().is_empty());
}

#[tokio::test]
async fn queue_delivers_each_message_once_when_acked() {
    let dir = tempfile::tempdir().unwrap();
    let m = Messaging::build(dir.path().to_path_buf(), &[], &[queue_spec("jobs", 5000)], &[]).unwrap();
    let q = m.queue("jobs").unwrap();

    for i in 0..5 {
        q.push(format!("job-{i}").as_bytes()).unwrap();
    }

    let mut received = Vec::new();
    for _ in 0..5 {
        let msg = q.pop("workers").unwrap().expect("expected a message");
        received.push(String::from_utf8(msg.payload).unwrap());
        q.ack("workers", msg.offset);
    }
    // Queue drained; nothing left.
    assert!(q.pop("workers").unwrap().is_none());

    received.sort();
    assert_eq!(
        received,
        vec!["job-0", "job-1", "job-2", "job-3", "job-4"]
    );
}

#[tokio::test]
async fn queue_redelivers_unacked_after_timeout() {
    let dir = tempfile::tempdir().unwrap();
    let m = Messaging::build(dir.path().to_path_buf(), &[], &[queue_spec("jobs", 100)], &[]).unwrap();
    let q = m.queue("jobs").unwrap();

    q.push(b"task").unwrap();

    // Deliver but do NOT ack.
    let first = q.pop("g").unwrap().expect("first delivery");
    assert_eq!(&first.payload, b"task");
    assert_eq!(q.in_flight_count("g"), 1);

    // Before the ack timeout, the queue has nothing else to hand out.
    assert!(q.pop("g").unwrap().is_none());

    // After the timeout, the unacked message is redelivered.
    tokio::time::sleep(Duration::from_millis(150)).await;
    let redelivered = q.pop("g").unwrap().expect("redelivery after timeout");
    assert_eq!(&redelivered.payload, b"task");
    assert_eq!(redelivered.offset, first.offset);

    // Ack it; now truly drained.
    q.ack("g", redelivered.offset);
    assert_eq!(q.in_flight_count("g"), 0);
}

#[tokio::test]
async fn queue_distributes_work_across_competing_consumers_in_a_group() {
    let dir = tempfile::tempdir().unwrap();
    let m = Messaging::build(dir.path().to_path_buf(), &[], &[queue_spec("jobs", 5000)], &[]).unwrap();
    let q = m.queue("jobs").unwrap();

    for i in 0..10 {
        q.push(format!("j{i}").as_bytes()).unwrap();
    }

    // Two workers in the same group each pop; together they cover all 10
    // exactly once (no message goes to two workers in the same group).
    let mut seen = std::collections::HashSet::new();
    for _ in 0..10 {
        let msg = q.pop("workers").unwrap().expect("message");
        assert!(seen.insert(msg.offset), "offset {} delivered twice", msg.offset);
        q.ack("workers", msg.offset);
    }
    assert_eq!(seen.len(), 10);
}

#[tokio::test]
async fn queue_different_groups_each_get_full_stream() {
    let dir = tempfile::tempdir().unwrap();
    let m = Messaging::build(dir.path().to_path_buf(), &[], &[queue_spec("jobs", 5000)], &[]).unwrap();
    let q = m.queue("jobs").unwrap();

    q.push(b"only").unwrap();

    // Group A and group B each independently receive the message.
    let a = q.pop("A").unwrap().expect("A gets it");
    let b = q.pop("B").unwrap().expect("B also gets it");
    assert_eq!(&a.payload, b"only");
    assert_eq!(&b.payload, b"only");
}
