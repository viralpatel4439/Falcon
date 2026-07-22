//! Two-region active-active convergence tests. Each "region" is a full
//! `Node` with a multi-leader keyspace; cross-region delivery is simulated
//! by draining one region's event bus and applying it to the other via
//! `apply_replicated` (the exact code path the gRPC peer-follower uses).

use falcon_core::{Config, Node, TierName, WriteMode};
use falcon_events::ChangeEvent;
use std::sync::Arc;

fn region_config(node_id: &str, region: &str, dir: &std::path::Path) -> Config {
    let mut config = Config::default();
    config.node.id = node_id.to_string();
    config.node.region = region.to_string();
    config.storage.data_dir = dir.to_string_lossy().to_string();
    config.replication.enabled = true;
    // Mark the default keyspace multi-leader + warm + replicated.
    config.keyspaces[0].tier = TierName::Warm;
    config.keyspaces[0].replication = true;
    config.keyspaces[0].subscriptions = true; // gives it an event bus to drain
    config.keyspaces[0].write_mode = WriteMode::MultiLeader;
    config.validate().unwrap();
    config
}

/// Deliver every buffered change from `src` region's default keyspace into
/// `dst` region via apply_replicated (LWW). Returns how many were delivered.
async fn deliver(src: &Arc<Node>, dst: &Arc<Node>, buffer: &mut Vec<ChangeEvent>) {
    for event in buffer.drain(..) {
        let _ = dst.keyspace("default").unwrap().apply_replicated(&event).await;
    }
    let _ = src; // symmetry
}

#[tokio::test]
async fn two_regions_both_write_and_converge() {
    let d1 = tempfile::tempdir().unwrap();
    let d2 = tempfile::tempdir().unwrap();
    let r1 = Arc::new(Node::build(region_config("node-1", "us-east", d1.path())).unwrap());
    let r2 = Arc::new(Node::build(region_config("node-2", "eu-west", d2.path())).unwrap());

    assert!(r1.keyspace("default").unwrap().is_multi_leader());

    // Subscribe to each region's bus to capture the events it produces.
    let mut r1_out = r1.keyspace("default").unwrap().events().unwrap().subscribe();
    let mut r2_out = r2.keyspace("default").unwrap().events().unwrap().subscribe();

    // Both regions write DIFFERENT keys concurrently.
    r1.keyspace("default").unwrap().put(b"key-from-1", b"v1").await.unwrap();
    r2.keyspace("default").unwrap().put(b"key-from-2", b"v2").await.unwrap();

    // Collect what each produced, then cross-deliver.
    let mut from_r1 = vec![r1_out.recv().await.unwrap()];
    let mut from_r2 = vec![r2_out.recv().await.unwrap()];
    deliver(&r1, &r2, &mut from_r1).await; // r1's write -> r2
    deliver(&r2, &r1, &mut from_r2).await; // r2's write -> r1

    // Both regions now hold both keys — converged.
    for r in [&r1, &r2] {
        let ks = r.keyspace("default").unwrap();
        assert_eq!(ks.get(b"key-from-1").await.unwrap(), Some(b"v1".to_vec()));
        assert_eq!(ks.get(b"key-from-2").await.unwrap(), Some(b"v2".to_vec()));
    }
}

#[tokio::test]
async fn concurrent_same_key_writes_converge_to_one_winner() {
    let d1 = tempfile::tempdir().unwrap();
    let d2 = tempfile::tempdir().unwrap();
    let r1 = Arc::new(Node::build(region_config("node-1", "us-east", d1.path())).unwrap());
    let r2 = Arc::new(Node::build(region_config("node-2", "eu-west", d2.path())).unwrap());

    let mut r1_out = r1.keyspace("default").unwrap().events().unwrap().subscribe();
    let mut r2_out = r2.keyspace("default").unwrap().events().unwrap().subscribe();

    // Both regions write the SAME key at ~the same time.
    r1.keyspace("default").unwrap().put(b"contested", b"value-r1").await.unwrap();
    r2.keyspace("default").unwrap().put(b"contested", b"value-r2").await.unwrap();

    let mut from_r1 = vec![r1_out.recv().await.unwrap()];
    let mut from_r2 = vec![r2_out.recv().await.unwrap()];

    // Cross-deliver (each applies the other's write via LWW).
    deliver(&r1, &r2, &mut from_r1).await;
    deliver(&r2, &r1, &mut from_r2).await;

    // Both regions MUST agree on the same winner (deterministic by HLC).
    let v1 = r1.keyspace("default").unwrap().get(b"contested").await.unwrap();
    let v2 = r2.keyspace("default").unwrap().get(b"contested").await.unwrap();
    assert_eq!(v1, v2, "regions diverged on a contested key: {v1:?} vs {v2:?}");
    assert!(v1.is_some());
}

#[tokio::test]
async fn partition_then_heal_converges() {
    let d1 = tempfile::tempdir().unwrap();
    let d2 = tempfile::tempdir().unwrap();
    let r1 = Arc::new(Node::build(region_config("node-1", "us-east", d1.path())).unwrap());
    let r2 = Arc::new(Node::build(region_config("node-2", "eu-west", d2.path())).unwrap());

    let mut r1_out = r1.keyspace("default").unwrap().events().unwrap().subscribe();
    let mut r2_out = r2.keyspace("default").unwrap().events().unwrap().subscribe();

    // PARTITION: both regions take independent writes with NO delivery.
    for i in 0..5 {
        r1.keyspace("default").unwrap().put(format!("r1-{i}").as_bytes(), b"a").await.unwrap();
        r2.keyspace("default").unwrap().put(format!("r2-{i}").as_bytes(), b"b").await.unwrap();
    }
    // Also a contested key on both sides during the partition.
    r1.keyspace("default").unwrap().put(b"shared", b"r1-wrote").await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    r2.keyspace("default").unwrap().put(b"shared", b"r2-wrote-later").await.unwrap();

    // Buffer everything produced during the partition.
    let mut from_r1 = Vec::new();
    while let Ok(e) = r1_out.try_recv() { from_r1.push(e); }
    let mut from_r2 = Vec::new();
    while let Ok(e) = r2_out.try_recv() { from_r2.push(e); }

    // HEAL: deliver both directions.
    deliver(&r1, &r2, &mut from_r1).await;
    deliver(&r2, &r1, &mut from_r2).await;

    // Every key from both partitions is present in both regions, and the
    // contested key agrees.
    for r in [&r1, &r2] {
        let ks = r.keyspace("default").unwrap();
        for i in 0..5 {
            assert_eq!(ks.get(format!("r1-{i}").as_bytes()).await.unwrap(), Some(b"a".to_vec()));
            assert_eq!(ks.get(format!("r2-{i}").as_bytes()).await.unwrap(), Some(b"b".to_vec()));
        }
    }
    let s1 = r1.keyspace("default").unwrap().get(b"shared").await.unwrap();
    let s2 = r2.keyspace("default").unwrap().get(b"shared").await.unwrap();
    assert_eq!(s1, s2, "contested key diverged after heal");
}
