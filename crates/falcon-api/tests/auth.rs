use falcon_core::{Config, Node};
use std::sync::Arc;

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

fn config_with_token(dir: &std::path::Path, token: &str) -> Config {
    let mut config = Config::default();
    config.storage.data_dir = dir.to_string_lossy().to_string();
    config.auth.api_key = token.to_string();
    config
}

#[tokio::test]
async fn auth_off_by_default_allows_everything() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = dir.path().to_string_lossy().to_string();
    // token empty -> auth off
    let addr = start(config).await;
    let client = reqwest::Client::new();
    let resp = client.put(format!("http://{addr}/kv/k")).body("v").send().await.unwrap();
    assert!(resp.status().is_success());
}

#[tokio::test]
async fn auth_on_rejects_missing_and_wrong_token() {
    let dir = tempfile::tempdir().unwrap();
    let addr = start(config_with_token(dir.path(), "s3cret")).await;
    let client = reqwest::Client::new();

    // No token -> 401.
    let resp = client.get(format!("http://{addr}/kv/k")).send().await.unwrap();
    assert_eq!(resp.status(), 401);

    // Wrong token -> 401.
    let resp = client
        .get(format!("http://{addr}/kv/k"))
        .bearer_auth("wrong")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn auth_on_allows_correct_token_and_healthz_is_exempt() {
    let dir = tempfile::tempdir().unwrap();
    let addr = start(config_with_token(dir.path(), "s3cret")).await;
    let client = reqwest::Client::new();

    // healthz works without a token (liveness probes).
    let resp = client.get(format!("http://{addr}/healthz")).send().await.unwrap();
    assert!(resp.status().is_success());

    // Correct token -> allowed.
    let resp = client
        .put(format!("http://{addr}/kv/k"))
        .bearer_auth("s3cret")
        .body("v")
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
}

#[tokio::test]
async fn api_key_via_query_param_works() {
    // The query-param fallback (for browser WebSocket clients that can't set
    // headers). Correct key in ?api_key= is accepted; wrong/missing is 401.
    let dir = tempfile::tempdir().unwrap();
    let addr = start(config_with_token(dir.path(), "s3cret")).await;
    let client = reqwest::Client::new();

    let ok = client
        .put(format!("http://{addr}/kv/k?api_key=s3cret"))
        .body("v")
        .send()
        .await
        .unwrap();
    assert!(ok.status().is_success(), "valid ?api_key should be accepted");

    let bad = client
        .get(format!("http://{addr}/kv/k?api_key=wrong"))
        .send()
        .await
        .unwrap();
    assert_eq!(bad.status(), 401, "wrong ?api_key must be rejected");

    let missing = client.get(format!("http://{addr}/kv/k")).send().await.unwrap();
    assert_eq!(missing.status(), 401);
}

#[tokio::test]
async fn websocket_subscribe_requires_api_key() {
    // The /subscribe WebSocket upgrade goes through the same auth layer, so
    // an unauthenticated upgrade is rejected and a keyed one is accepted.
    let dir = tempfile::tempdir().unwrap();
    let addr = start(config_with_token(dir.path(), "s3cret")).await;

    // No key -> the upgrade request is 401 (handshake refused).
    let no_key = tokio_tungstenite::connect_async(format!("ws://{addr}/subscribe")).await;
    assert!(no_key.is_err(), "WS subscribe without api_key must be refused");

    // With key in the query param -> upgrade succeeds.
    let with_key =
        tokio_tungstenite::connect_async(format!("ws://{addr}/subscribe?api_key=s3cret")).await;
    assert!(with_key.is_ok(), "WS subscribe with valid ?api_key must connect");
}
