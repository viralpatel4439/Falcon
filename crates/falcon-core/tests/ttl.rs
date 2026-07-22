use falcon_core::{Config, Node};
use std::sync::Arc;
use std::time::Duration;

fn test_config() -> Config {
    let dir = std::env::temp_dir().join(format!("kvttl-{}-{}", std::process::id(), rand_suffix()));
    let _ = std::fs::create_dir_all(&dir);
    let mut config = Config::default();
    config.storage.data_dir = dir.to_string_lossy().to_string();
    config
}

fn rand_suffix() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64
}

#[tokio::test]
async fn per_write_ttl_expires_lazily_on_get() {
    let node = Node::build(test_config()).unwrap();
    let ks = node.keyspace("default").unwrap();

    ks.put_with_ttl(b"temp", b"v", Some(1)).await.unwrap();
    assert_eq!(ks.get(b"temp").await.unwrap(), Some(b"v".to_vec()));

    // After the TTL passes, a get returns None (lazy expiry deletes it).
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert_eq!(ks.get(b"temp").await.unwrap(), None);
}

#[tokio::test]
async fn put_without_ttl_never_expires() {
    let node = Node::build(test_config()).unwrap();
    let ks = node.keyspace("default").unwrap();

    ks.put(b"permanent", b"v").await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(ks.get(b"permanent").await.unwrap(), Some(b"v".to_vec()));
    assert_eq!(ks.tracked_ttl_keys(), 0, "no TTL tracked for a plain put");
}

#[tokio::test]
async fn background_reaper_deletes_expired_keys() {
    let node = Arc::new(Node::build(test_config()).unwrap());
    let ks = node.keyspace("default").unwrap();

    ks.put_with_ttl(b"a", b"1", Some(1)).await.unwrap();
    ks.put_with_ttl(b"b", b"2", Some(1)).await.unwrap();
    assert_eq!(ks.tracked_ttl_keys(), 2);

    tokio::time::sleep(Duration::from_millis(1100)).await;

    // Reap directly (what the background task does on its interval).
    let reaped = node.reap_expired().await;
    assert_eq!(reaped, 2);
    assert_eq!(ks.tracked_ttl_keys(), 0);
    assert_eq!(ks.get(b"a").await.unwrap(), None);
    assert_eq!(ks.get(b"b").await.unwrap(), None);
}

#[tokio::test]
async fn ttl_expiry_emits_delete_event_for_subscribers() {
    // A keyspace with subscriptions on gets an event bus; a TTL expiry must
    // publish a Delete event, exactly like an explicit delete, so
    // subscribers and replication stay consistent.
    let mut config = test_config();
    config.keyspaces[0].subscriptions = true;
    let node = Node::build(config).unwrap();
    let ks = node.keyspace("default").unwrap();

    let mut sub = ks.events().unwrap().subscribe();

    ks.put_with_ttl(b"k", b"v", Some(1)).await.unwrap();
    // Drain the Put event.
    let put_evt = sub.recv().await.unwrap();
    assert_eq!(put_evt.key, b"k");

    tokio::time::sleep(Duration::from_millis(1100)).await;
    let _ = ks.get(b"k").await.unwrap(); // triggers lazy expiry -> delete

    let del_evt = sub.recv().await.unwrap();
    assert_eq!(del_evt.key, b"k");
    assert!(del_evt.is_tombstone(), "TTL expiry must emit a Delete/tombstone event");
}

#[tokio::test]
async fn ttl_zero_clears_existing_expiry() {
    let node = Node::build(test_config()).unwrap();
    let ks = node.keyspace("default").unwrap();

    ks.put_with_ttl(b"k", b"v1", Some(1)).await.unwrap();
    assert_eq!(ks.tracked_ttl_keys(), 1);
    // Rewrite with ttl=0 -> no expiry.
    ks.put_with_ttl(b"k", b"v2", Some(0)).await.unwrap();
    assert_eq!(ks.tracked_ttl_keys(), 0);

    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert_eq!(ks.get(b"k").await.unwrap(), Some(b"v2".to_vec()));
}
