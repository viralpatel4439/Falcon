use futures::{SinkExt, StreamExt};
use falcon_core::{Config, Node};
use std::sync::Arc;
use tokio_tungstenite::tungstenite::Message;

async fn start_test_server(subscriptions_enabled: bool) -> (std::net::SocketAddr, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = dir.path().to_string_lossy().to_string();
    config.keyspaces[0].subscriptions = subscriptions_enabled;
    config.http.bind = "127.0.0.1:0".to_string();

    let node = Arc::new(Node::build(config).unwrap());
    let app = falcon_api::router(node);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (addr, dir)
}

#[tokio::test]
async fn subscribe_receives_put_update() {
    let (addr, _dir) = start_test_server(true).await;

    let ws_url = format!("ws://{addr}/subscribe");
    let (mut ws, _) = tokio_tungstenite::connect_async(ws_url).await.unwrap();

    ws.send(Message::text(
        r#"{"type":"subscribe","id":"sub1","keyspace":"default","key":"foo"}"#,
    ))
    .await
    .unwrap();

    let ack = ws.next().await.unwrap().unwrap();
    assert!(ack.to_text().unwrap().contains("subscribed"));

    let client = reqwest::Client::new();
    client
        .put(format!("http://{addr}/kv/foo"))
        .body("bar")
        .send()
        .await
        .unwrap();

    let update = tokio::time::timeout(std::time::Duration::from_secs(2), ws.next())
        .await
        .expect("timed out waiting for update")
        .unwrap()
        .unwrap();
    let text = update.to_text().unwrap();
    assert!(text.contains("\"type\":\"update\""));
    assert!(text.contains("\"key\":\"foo\""));
    assert!(text.contains("\"value\":\"bar\""));
}

#[tokio::test]
async fn subscribe_disabled_by_default_returns_error() {
    let (addr, _dir) = start_test_server(false).await;

    let ws_url = format!("ws://{addr}/subscribe");
    let (mut ws, _) = tokio_tungstenite::connect_async(ws_url).await.unwrap();

    ws.send(Message::text(
        r#"{"type":"subscribe","id":"sub1","keyspace":"default","key":"foo"}"#,
    ))
    .await
    .unwrap();

    let msg = ws.next().await.unwrap().unwrap();
    let text = msg.to_text().unwrap();
    assert!(text.contains("\"type\":\"error\""));
    assert!(text.contains("disabled"));
}

#[tokio::test]
async fn crud_still_works_when_subscriptions_disabled() {
    let (addr, _dir) = start_test_server(false).await;
    let client = reqwest::Client::new();

    let resp = client
        .put(format!("http://{addr}/kv/foo"))
        .body("bar")
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let resp = client
        .get(format!("http://{addr}/kv/foo"))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["value"], "bar");
}
