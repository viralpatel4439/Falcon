use bytes::BytesMut;
use falcon_core::{Config, Node};
use falcon_wire::{encode_request, OP_DEL, OP_GET, OP_PING, OP_SET, STATUS_NOT_FOUND, STATUS_OK, STATUS_PONG};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

struct TestServer {
    wire_addr: std::net::SocketAddr,
    http_addr: std::net::SocketAddr,
    _dir: tempfile::TempDir,
}

async fn start_server() -> TestServer {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = dir.path().to_string_lossy().to_string();
    let node = Arc::new(Node::build(config).unwrap());

    // Wire server on an ephemeral port (bind first, then serve — no race).
    let wire_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let wire_addr = wire_listener.local_addr().unwrap();
    let wire_node = node.clone();
    tokio::spawn(async move {
        let _ = falcon_wire::serve_with_listener(wire_node, wire_listener).await;
    });

    // HTTP server on an ephemeral port, sharing the same Node.
    let http_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_addr = http_listener.local_addr().unwrap();
    let app = falcon_api::router(node);
    tokio::spawn(async move {
        axum::serve(http_listener, app).await.unwrap();
    });

    // Give the wire server a moment to bind.
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    TestServer {
        wire_addr,
        http_addr,
        _dir: dir,
    }
}

struct Resp {
    status: u8,
    value: Vec<u8>,
}

async fn read_response(stream: &mut TcpStream) -> Resp {
    let mut header = [0u8; 5];
    stream.read_exact(&mut header).await.unwrap();
    let status = header[0];
    let val_len = u32::from_le_bytes([header[1], header[2], header[3], header[4]]) as usize;
    let mut value = vec![0u8; val_len];
    if val_len > 0 {
        stream.read_exact(&mut value).await.unwrap();
    }
    Resp { status, value }
}

#[tokio::test]
async fn pipelined_set_get_del_over_wire() {
    let server = start_server().await;
    let mut stream = TcpStream::connect(server.wire_addr).await.unwrap();
    stream.set_nodelay(true).unwrap();

    // Pipeline: PING, SET foo=bar, GET foo, DEL foo, GET foo — all sent
    // before reading any reply.
    let mut out = BytesMut::new();
    encode_request(&mut out, OP_PING, b"", b"", b"");
    encode_request(&mut out, OP_SET, b"", b"foo", b"bar");
    encode_request(&mut out, OP_GET, b"", b"foo", b"");
    encode_request(&mut out, OP_DEL, b"", b"foo", b"");
    encode_request(&mut out, OP_GET, b"", b"foo", b"");
    stream.write_all(&out).await.unwrap();
    stream.flush().await.unwrap();

    // Responses come back in request order.
    let ping = read_response(&mut stream).await;
    assert_eq!(ping.status, STATUS_PONG);

    let set = read_response(&mut stream).await;
    assert_eq!(set.status, STATUS_OK);

    let get1 = read_response(&mut stream).await;
    assert_eq!(get1.status, STATUS_OK);
    assert_eq!(get1.value, b"bar");

    let del = read_response(&mut stream).await;
    assert_eq!(del.status, STATUS_OK);

    let get2 = read_response(&mut stream).await;
    assert_eq!(get2.status, STATUS_NOT_FOUND);
    assert!(get2.value.is_empty());
}

#[tokio::test]
async fn value_written_over_wire_is_visible_via_http() {
    let server = start_server().await;
    let mut stream = TcpStream::connect(server.wire_addr).await.unwrap();
    stream.set_nodelay(true).unwrap();

    let mut out = BytesMut::new();
    encode_request(&mut out, OP_SET, b"", b"shared", b"cross-protocol");
    stream.write_all(&out).await.unwrap();
    stream.flush().await.unwrap();
    let set = read_response(&mut stream).await;
    assert_eq!(set.status, STATUS_OK);

    // Same Node underneath, so the HTTP API must see it.
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("http://{}/kv?key=shared", server.http_addr))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["value"], "cross-protocol");
}

#[tokio::test]
async fn deep_pipeline_preserves_order() {
    let server = start_server().await;
    let mut stream = TcpStream::connect(server.wire_addr).await.unwrap();
    stream.set_nodelay(true).unwrap();

    // Pipeline 500 SETs to distinct keys, then 500 GETs, all before reading.
    const N: usize = 500;
    let mut out = BytesMut::new();
    for i in 0..N {
        encode_request(&mut out, OP_SET, b"", format!("k{i}").as_bytes(), format!("v{i}").as_bytes());
    }
    for i in 0..N {
        encode_request(&mut out, OP_GET, b"", format!("k{i}").as_bytes(), b"");
    }
    stream.write_all(&out).await.unwrap();
    stream.flush().await.unwrap();

    for _ in 0..N {
        assert_eq!(read_response(&mut stream).await.status, STATUS_OK);
    }
    for i in 0..N {
        let r = read_response(&mut stream).await;
        assert_eq!(r.status, STATUS_OK);
        assert_eq!(r.value, format!("v{i}").into_bytes(), "GET k{i} returned wrong value");
    }
}
