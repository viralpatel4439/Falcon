//! Route gating by installed product. A node built from a single-product
//! profile must expose only that product's HTTP surface: a cache node serves
//! KV routes but 404s `/topics`, `/queues`, `/streams`; a pubsub node does the
//! reverse. The `/health` payload reports the active products either way.

use falcon_core::{Config, Feature, FeatureSet, Node, Profile};
use std::sync::Arc;

async fn start(features: FeatureSet, profile: Profile) -> std::net::SocketAddr {
    let dir = tempfile::tempdir().unwrap();
    let mut config: Config = profile.to_config();
    config.storage.data_dir = dir.path().to_string_lossy().to_string();
    // Keep the temp dir alive for the server's lifetime.
    std::mem::forget(dir);

    let node = Arc::new(Node::build(config).unwrap());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = falcon_api::router_for(node, features, "/tmp/falcon-test-profile.toml".into());
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(80)).await;
    addr
}

fn profile_with(feature: Feature) -> (FeatureSet, Profile) {
    let mut p = Profile::default();
    p.features.insert(feature);
    (p.features.clone(), p)
}

#[tokio::test]
async fn cache_node_serves_kv_but_gates_messaging() {
    let (features, profile) = profile_with(Feature::Cache);
    let addr = start(features, profile).await;
    let client = reqwest::Client::new();

    // Cache route is present.
    let put = client
        .post(format!("http://{addr}/cache"))
        .json(&serde_json::json!({"key":"k","value":"v"}))
        .send()
        .await
        .unwrap();
    assert!(put.status().is_success(), "cache node should accept cache writes");

    // Other products' routes are not mounted -> 404.
    for path in ["/pubsub", "/queue", "/stream"] {
        let resp = client.post(format!("http://{addr}{path}")).json(&serde_json::json!({"value":"x"})).send().await.unwrap();
        assert_eq!(resp.status(), 404, "cache node must not expose {path}");
    }

    // Health reports the active product.
    let health: serde_json::Value =
        client.get(format!("http://{addr}/health")).send().await.unwrap().json().await.unwrap();
    assert_eq!(health["products"], serde_json::json!(["cache"]));
    assert_eq!(health["primary_product"], "cache");
}

#[tokio::test]
async fn pubsub_node_gates_kv_and_streams() {
    let (features, profile) = profile_with(Feature::Pubsub);
    let addr = start(features, profile).await;
    let client = reqwest::Client::new();

    // Pub/Sub route present.
    let pub_resp = client
        .post(format!("http://{addr}/pubsub"))
        .json(&serde_json::json!({"value":"hello"}))
        .send()
        .await
        .unwrap();
    assert!(pub_resp.status().is_success(), "pubsub node should accept publishes");

    // KV + stream routes are gated off.
    let kv = client.get(format!("http://{addr}/kv?key=k")).send().await.unwrap();
    assert_eq!(kv.status(), 404, "pubsub node must not expose KV");
    let stream = client
        .post(format!("http://{addr}/stream"))
        .json(&serde_json::json!({"value":"x"}))
        .send()
        .await
        .unwrap();
    assert_eq!(stream.status(), 404, "pubsub node must not expose streams");
}

#[tokio::test]
async fn config_endpoint_reports_products() {
    let (features, profile) = profile_with(Feature::Queue);
    let addr = start(features, profile).await;
    let client = reqwest::Client::new();
    let cfg: serde_json::Value =
        client.get(format!("http://{addr}/config")).send().await.unwrap().json().await.unwrap();
    assert_eq!(cfg["products"], serde_json::json!(["queue"]));
    // The settable keys are present for the UI.
    assert!(cfg["entries"].as_array().unwrap().iter().any(|e| e["key"] == "node.region"));
}
