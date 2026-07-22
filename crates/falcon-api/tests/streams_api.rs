//! End-to-end tests for the Falcon Event Streaming REST API: append routes by
//! key to a stable partition, poll returns records after a group's committed
//! offset, and commit advances that offset (at-least-once resume).

use falcon_core::{Config, Node};
use serde_json::Value;
use std::sync::Arc;

fn config_with_stream(dir: &std::path::Path) -> Config {
    let mut config = Config::default();
    config.storage.data_dir = dir.to_string_lossy().to_string();
    config.streams.push(falcon_core::config::StreamConfig {
        name: "events".to_string(),
        partitions: 4,
        capacity: 256,
    });
    config
}

async fn start(config: Config) -> std::net::SocketAddr {
    let node = Arc::new(Node::build(config).unwrap());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = falcon_api::router(node);
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(80)).await;
    addr
}

#[tokio::test]
async fn append_poll_commit_lifecycle() {
    let dir = tempfile::tempdir().unwrap();
    let addr = start(config_with_stream(dir.path())).await;
    let client = reqwest::Client::new();

    // Append 3 records under the same key -> same partition, ordered.
    let mut partition = None;
    for i in 0..3 {
        let resp: Value = client
            .post(format!("http://{addr}/streams/events/records?key=user:1"))
            .body(format!("evt{i}"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let p = resp["partition"].as_u64().unwrap();
        assert_eq!(resp["offset"].as_u64().unwrap(), i + 1);
        partition = Some(partition.unwrap_or(p));
        assert_eq!(Some(p), partition, "same key must map to one partition");
    }
    let p = partition.unwrap();

    // Poll (group "g1") -> all 3 records, uncommitted.
    let poll: Value = client
        .get(format!("http://{addr}/streams/events/poll?group=g1&partition={p}"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let records = poll["records"].as_array().unwrap();
    assert_eq!(records.len(), 3);
    assert_eq!(records[0]["payload"], "evt0");
    assert_eq!(records[2]["payload"], "evt2");

    // Commit through offset 2.
    let commit: Value = client
        .post(format!(
            "http://{addr}/streams/events/commit?group=g1&partition={p}&offset=2"
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(commit["committed"].as_u64().unwrap(), 2);

    // Re-poll g1 -> only the record after the commit remains.
    let poll2: Value = client
        .get(format!("http://{addr}/streams/events/poll?group=g1&partition={p}"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let rec2 = poll2["records"].as_array().unwrap();
    assert_eq!(rec2.len(), 1);
    assert_eq!(rec2[0]["payload"], "evt2");

    // A different group sees the full stream (independent cursor).
    let poll_g2: Value = client
        .get(format!("http://{addr}/streams/events/poll?group=g2&partition={p}"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(poll_g2["records"].as_array().unwrap().len(), 3);
}

#[tokio::test]
async fn stream_info_and_unknown_stream() {
    let dir = tempfile::tempdir().unwrap();
    let addr = start(config_with_stream(dir.path())).await;
    let client = reqwest::Client::new();

    let info: Value = client
        .get(format!("http://{addr}/streams/events"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(info["partitions"].as_u64().unwrap(), 4);

    // Unknown stream -> 404.
    let resp = client
        .get(format!("http://{addr}/streams/nope"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn poll_requires_group_and_partition() {
    let dir = tempfile::tempdir().unwrap();
    let addr = start(config_with_stream(dir.path())).await;
    let client = reqwest::Client::new();

    // Missing ?group= and ?partition= -> 400.
    let resp = client
        .get(format!("http://{addr}/streams/events/poll"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
}
