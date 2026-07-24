//! End-to-end tests for the simplified Falcon Event Stream REST API:
//! `POST /stream {key,value}` appends, `GET /stream` returns the next batch of
//! records and advances the consumer position (auto-commit), so a second read
//! returns nothing new. Partitions, groups, and offsets are internal.

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
        interval_fsync_ms: 0,
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
async fn append_then_read_next_batch_and_it_commits() {
    let dir = tempfile::tempdir().unwrap();
    let addr = start(config_with_stream(dir.path())).await;
    let client = reqwest::Client::new();

    // Append 3 records under the same key (kept in order on one partition).
    for i in 0..3 {
        let resp = client
            .post(format!("http://{addr}/stream"))
            .json(&serde_json::json!({"key":"user:1","value":format!("evt{i}")}))
            .send()
            .await
            .unwrap();
        assert!(resp.status().is_success());
    }

    // First read returns all 3, in order.
    let batch: Value = client
        .get(format!("http://{addr}/stream"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let items = batch["items"].as_array().unwrap();
    assert_eq!(items.len(), 3, "first read should return all appended records");
    assert_eq!(items[0]["value"], "evt0");
    assert_eq!(items[2]["value"], "evt2");

    // Second read returns nothing new (the first read committed progress).
    let batch2: Value = client
        .get(format!("http://{addr}/stream"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        batch2["items"].as_array().unwrap().len(),
        0,
        "records should not be redelivered after a read commits them"
    );
}

#[tokio::test]
async fn stream_route_absent_without_the_product() {
    // A node with no stream configured must not expose /stream.
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = dir.path().to_string_lossy().to_string();
    let node = Arc::new(Node::build(config).unwrap());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    // Only KV active — no Stream feature.
    let mut features = falcon_core::FeatureSet::new();
    features.insert(falcon_core::Feature::Kv);
    let app = falcon_api::router_for(node, features, "/tmp/falcon-test-profile.toml".into());
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(80)).await;

    let resp = client_get(&format!("http://{addr}/stream")).await;
    assert_eq!(resp, 404);
}

async fn client_get(url: &str) -> u16 {
    reqwest::Client::new()
        .get(url)
        .send()
        .await
        .unwrap()
        .status()
        .as_u16()
}
