use bytes::BytesMut;
use falcon_core::config::{QueueConfig, TopicConfig, TopicModeConfig};
use falcon_core::{Config, Node};
use falcon_wire::{
    encode_request, OP_ACK, OP_POP, OP_PUBLISH, OP_PUSH, OP_SUBSCRIBE, STATUS_EMPTY, STATUS_MESSAGE,
    STATUS_OK,
};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

async fn start_server(config: Config) -> std::net::SocketAddr {
    let node = Arc::new(Node::build(config).unwrap());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = falcon_wire::serve_with_listener(node, listener).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    addr
}

struct Frame {
    status: u8,
    body: Vec<u8>,
}

async fn read_frame(stream: &mut TcpStream) -> Frame {
    let mut header = [0u8; 5];
    stream.read_exact(&mut header).await.unwrap();
    let status = header[0];
    let len = u32::from_le_bytes([header[1], header[2], header[3], header[4]]) as usize;
    let mut body = vec![0u8; len];
    if len > 0 {
        stream.read_exact(&mut body).await.unwrap();
    }
    Frame { status, body }
}

fn config_with_messaging() -> Config {
    let dir = std::env::temp_dir().join(format!("kvwire-msg-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&dir);
    let mut config = Config::default();
    config.storage.data_dir = dir.to_string_lossy().to_string();
    config.topics = vec![TopicConfig {
        name: "news".to_string(),
        mode: TopicModeConfig::Ephemeral,
        capacity: 1024,
    }];
    config.queues = vec![QueueConfig {
        name: "jobs".to_string(),
        ack_timeout_secs: 30,
    }];
    config
}

#[tokio::test]
async fn publish_subscribe_over_wire() {
    let addr = start_server(config_with_messaging()).await;

    // Subscriber connection: send SUBSCRIBE, read the OK ack, then stream.
    let mut sub = TcpStream::connect(addr).await.unwrap();
    sub.set_nodelay(true).unwrap();
    let mut out = BytesMut::new();
    encode_request(&mut out, OP_SUBSCRIBE, b"news", b"", b"");
    sub.write_all(&out).await.unwrap();
    sub.flush().await.unwrap();
    let ack = read_frame(&mut sub).await;
    assert_eq!(ack.status, STATUS_OK);

    // Give the subscriber a moment to register before publishing.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Publisher connection: PUBLISH a message.
    let mut pubc = TcpStream::connect(addr).await.unwrap();
    pubc.set_nodelay(true).unwrap();
    let mut out = BytesMut::new();
    encode_request(&mut out, OP_PUBLISH, b"news", b"", b"breaking");
    pubc.write_all(&out).await.unwrap();
    pubc.flush().await.unwrap();
    let pub_ack = read_frame(&mut pubc).await;
    assert_eq!(pub_ack.status, STATUS_OK);

    // Subscriber receives the pushed message frame (offset:8 + payload).
    let msg = read_frame(&mut sub).await;
    assert_eq!(msg.status, STATUS_MESSAGE);
    assert_eq!(&msg.body[8..], b"breaking");
}

#[tokio::test]
async fn queue_push_pop_ack_over_wire() {
    let addr = start_server(config_with_messaging()).await;
    let mut conn = TcpStream::connect(addr).await.unwrap();
    conn.set_nodelay(true).unwrap();

    // PUSH two jobs.
    let mut out = BytesMut::new();
    encode_request(&mut out, OP_PUSH, b"jobs", b"", b"job-1");
    encode_request(&mut out, OP_PUSH, b"jobs", b"", b"job-2");
    conn.write_all(&out).await.unwrap();
    conn.flush().await.unwrap();
    assert_eq!(read_frame(&mut conn).await.status, STATUS_OK);
    assert_eq!(read_frame(&mut conn).await.status, STATUS_OK);

    // POP (group "w"), expecting the first job with an 8-byte offset prefix.
    let mut out = BytesMut::new();
    encode_request(&mut out, OP_POP, b"jobs", b"w", b"");
    conn.write_all(&out).await.unwrap();
    conn.flush().await.unwrap();
    let popped = read_frame(&mut conn).await;
    assert_eq!(popped.status, STATUS_MESSAGE);
    let offset = u64::from_le_bytes(popped.body[..8].try_into().unwrap());
    assert_eq!(&popped.body[8..], b"job-1");

    // ACK it.
    let mut out = BytesMut::new();
    encode_request(&mut out, OP_ACK, b"jobs", b"w", &offset.to_le_bytes());
    conn.write_all(&out).await.unwrap();
    conn.flush().await.unwrap();
    assert_eq!(read_frame(&mut conn).await.status, STATUS_OK);

    // POP again -> second job.
    let mut out = BytesMut::new();
    encode_request(&mut out, OP_POP, b"jobs", b"w", b"");
    conn.write_all(&out).await.unwrap();
    conn.flush().await.unwrap();
    let second = read_frame(&mut conn).await;
    assert_eq!(second.status, STATUS_MESSAGE);
    assert_eq!(&second.body[8..], b"job-2");
    let off2 = u64::from_le_bytes(second.body[..8].try_into().unwrap());
    let mut out = BytesMut::new();
    encode_request(&mut out, OP_ACK, b"jobs", b"w", &off2.to_le_bytes());
    conn.write_all(&out).await.unwrap();
    conn.flush().await.unwrap();
    assert_eq!(read_frame(&mut conn).await.status, STATUS_OK);

    // POP once more -> empty.
    let mut out = BytesMut::new();
    encode_request(&mut out, OP_POP, b"jobs", b"w", b"");
    conn.write_all(&out).await.unwrap();
    conn.flush().await.unwrap();
    assert_eq!(read_frame(&mut conn).await.status, STATUS_EMPTY);
}
