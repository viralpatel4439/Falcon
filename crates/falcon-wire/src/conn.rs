//! Per-connection read/dispatch/write loop — the pipelining core.

use crate::codec::{decode_one, DecodeError};
use crate::protocol::{
    Request, Response, OP_ACK, OP_AUTH, OP_DEL, OP_GET, OP_PING, OP_POP, OP_PUBLISH, OP_PUSH,
    OP_SET, OP_STREAM_APPEND, OP_SUBSCRIBE,
};
use bytes::BytesMut;
use falcon_core::Node;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter};

const BUF_CAPACITY: usize = 64 * 1024;
const DEFAULT_KEYSPACE: &str = "default";

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Handle one connection over any byte stream — a plain `TcpStream` or a
/// `tokio_rustls` TLS stream. Generic so the wire protocol runs identically
/// with or without transport encryption.
pub async fn handle_conn<S>(node: Arc<Node>, stream: S) -> std::io::Result<()>
where
    S: AsyncRead + AsyncWrite + Send + 'static,
{
    let (read_half, write_half) = tokio::io::split(stream);
    let mut reader = BufReader::with_capacity(BUF_CAPACITY, read_half);
    let mut writer = BufWriter::with_capacity(BUF_CAPACITY, write_half);
    let mut in_buf = BytesMut::with_capacity(BUF_CAPACITY);
    let mut out_buf = BytesMut::with_capacity(BUF_CAPACITY);

    // Auth gate: when a token is configured, the connection must present a
    // matching AUTH frame before any other op is honored. Off by default
    // (`authed` starts true), so the fast path pays nothing.
    let auth = &node.config().auth;
    let mut authed = !auth.is_enabled();

    loop {
        // 1. Drain every fully-buffered request into a pipeline batch.
        let mut batch: Vec<Request> = Vec::new();
        loop {
            match decode_one(&mut in_buf) {
                Ok(Some(req)) => batch.push(req),
                Ok(None) => break, // need more bytes
                Err(DecodeError::Malformed) => {
                    out_buf.clear();
                    Response::BadRequest.encode(&mut out_buf);
                    let _ = writer.write_all(&out_buf).await;
                    let _ = writer.flush().await;
                    return Ok(());
                }
            }
        }

        // 2. Nothing decoded yet — read more (the only awaited blocking point).
        if batch.is_empty() {
            let n = reader.read_buf(&mut in_buf).await?;
            if n == 0 {
                return Ok(()); // client closed the connection
            }
            continue;
        }

        // 3. Dispatch the batch sequentially, encoding responses in request
        //    order. (Measured: concurrent fan-out via join_all was *slower*
        //    here — each GET is a sub-microsecond DashMap lookup with no
        //    real await to overlap, so the future-vec bookkeeping cost more
        //    than it saved. Sequential wins.) A SUBSCRIBE converts the
        //    connection into a push stream and never returns.
        out_buf.clear();
        let mut subscribe_at = None;
        for (i, req) in batch.iter().enumerate() {
            // Auth gate: before authentication, only an AUTH frame is honored.
            if req.op == OP_AUTH {
                if constant_time_eq(&req.value, auth.api_key.as_bytes()) {
                    authed = true;
                    Response::Ok.encode(&mut out_buf);
                } else {
                    Response::Unauthorized.encode(&mut out_buf);
                }
                continue;
            }
            if !authed {
                Response::Unauthorized.encode(&mut out_buf);
                continue;
            }
            if req.op == OP_SUBSCRIBE {
                subscribe_at = Some(i);
                break;
            }
            dispatch(&node, req).await.encode(&mut out_buf);
        }
        if let Some(i) = subscribe_at {
            Response::Ok.encode(&mut out_buf);
            writer.write_all(&out_buf).await?;
            writer.flush().await?;
            return run_subscription(&node, &batch[i], writer).await;
        }

        // 4. One buffered write + one flush per batch (few syscalls).
        writer.write_all(&out_buf).await?;
        writer.flush().await?;
    }
}

/// Borrow the keyspace/topic/queue name from the request bytes without
/// allocating; empty means the default keyspace.
fn name_str<'a>(bytes: &'a [u8], default: &'a str) -> Option<&'a str> {
    if bytes.is_empty() {
        Some(default)
    } else {
        std::str::from_utf8(bytes).ok()
    }
}

async fn dispatch(node: &Arc<Node>, req: &Request) -> Response {
    match req.op {
        OP_PING => Response::Pong,
        OP_GET | OP_SET | OP_DEL => dispatch_kv(node, req).await,
        // Messaging/stream appends fsync a durable log, which blocks. Run them
        // on the blocking pool so a slow fsync never stalls the async worker
        // (and other pipelined connections it's driving). The request bytes are
        // cheap `Bytes` clones.
        OP_PUBLISH | OP_PUSH | OP_POP | OP_ACK => {
            let node = node.clone();
            let req = req.clone();
            tokio::task::spawn_blocking(move || dispatch_messaging(&node, &req))
                .await
                .unwrap_or(Response::ServerError)
        }
        OP_STREAM_APPEND => {
            let node = node.clone();
            let req = req.clone();
            tokio::task::spawn_blocking(move || dispatch_stream_append(&node, &req))
                .await
                .unwrap_or(Response::ServerError)
        }
        _ => Response::BadRequest,
    }
}

/// Falcon Event Stream producer path: append `value` to the stream named
/// by `keyspace`, routed to a partition by `key`. Returns the assigned
/// partition + offset. Consumer poll/commit go over REST.
fn dispatch_stream_append(node: &Node, req: &Request) -> Response {
    let name = match std::str::from_utf8(&req.keyspace) {
        Ok(s) => s,
        Err(_) => return Response::BadRequest,
    };
    match node.messaging().stream(name) {
        Some(stream) => match stream.append_keyed(&req.key, req.value.to_vec()) {
            Ok((partition, offset)) => Response::Stored {
                partition: partition as u32,
                offset,
            },
            Err(_) => Response::ServerError,
        },
        None => Response::UnknownStream,
    }
}

async fn dispatch_kv(node: &Node, req: &Request) -> Response {
    let ks_name = match name_str(&req.keyspace, DEFAULT_KEYSPACE) {
        Some(s) => s,
        None => return Response::BadRequest,
    };
    let ks = match node.keyspace(ks_name) {
        Some(ks) => ks,
        None => return Response::UnknownKeyspace,
    };
    match req.op {
        OP_GET => match ks.get(&req.key).await {
            Ok(Some(v)) => Response::Value(v),
            Ok(None) => Response::NotFound,
            Err(_) => Response::ServerError,
        },
        OP_SET => match ks.put(&req.key, &req.value).await {
            Ok(_) => Response::Ok,
            Err(_) => Response::ServerError,
        },
        OP_DEL => match ks.delete(&req.key).await {
            Ok(_) => Response::Ok,
            Err(_) => Response::ServerError,
        },
        _ => Response::BadRequest,
    }
}

fn dispatch_messaging(node: &Node, req: &Request) -> Response {
    let name = match std::str::from_utf8(&req.keyspace) {
        Ok(s) => s,
        Err(_) => return Response::BadRequest,
    };
    let messaging = node.messaging();

    match req.op {
        OP_PUBLISH => match messaging.topic(name) {
            Some(topic) => match topic.publish(req.value.to_vec()) {
                Ok(_) => Response::Ok,
                Err(_) => Response::ServerError,
            },
            None => Response::UnknownTopic,
        },
        OP_PUSH => match messaging.queue(name) {
            Some(queue) => match queue.push(&req.value) {
                Ok(_) => Response::Ok,
                Err(_) => Response::ServerError,
            },
            None => Response::UnknownQueue,
        },
        OP_POP => {
            let group = std::str::from_utf8(&req.key).unwrap_or("default");
            match messaging.queue(name) {
                Some(queue) => match queue.pop(group) {
                    Ok(Some(msg)) => Response::Message {
                        offset: msg.offset,
                        payload: msg.payload,
                    },
                    Ok(None) => Response::Empty,
                    Err(_) => Response::ServerError,
                },
                None => Response::UnknownQueue,
            }
        }
        OP_ACK => {
            let group = std::str::from_utf8(&req.key).unwrap_or("default");
            let Ok(off_bytes) = <[u8; 8]>::try_from(&req.value[..]) else {
                return Response::BadRequest;
            };
            let offset = u64::from_le_bytes(off_bytes);
            match messaging.queue(name) {
                Some(queue) => {
                    queue.ack(group, offset);
                    Response::Ok
                }
                None => Response::UnknownQueue,
            }
        }
        _ => Response::BadRequest,
    }
}

/// Turns the connection into a live subscriber: streams every message
/// published to the topic from now on as `Message` frames until the client
/// disconnects. (Durable replay-from-offset can be layered on later via a
/// flag; this delivers the live tail, which is the low-latency path.)
async fn run_subscription<W>(
    node: &Node,
    req: &Request,
    mut writer: BufWriter<W>,
) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let name = match std::str::from_utf8(&req.keyspace) {
        Ok(s) => s.to_string(),
        Err(_) => return Ok(()),
    };
    let topic = match node.messaging().topic(&name) {
        Some(t) => t.clone(),
        None => {
            let mut out = BytesMut::new();
            Response::UnknownTopic.encode(&mut out);
            let _ = writer.write_all(&out).await;
            let _ = writer.flush().await;
            return Ok(());
        }
    };

    let mut rx = topic.subscribe();
    let mut out = BytesMut::with_capacity(BUF_CAPACITY);
    loop {
        match rx.recv().await {
            Ok(delivery) => {
                out.clear();
                Response::Message {
                    offset: delivery.offset,
                    payload: (*delivery.payload).clone(),
                }
                .encode(&mut out);
                if writer.write_all(&out).await.is_err() {
                    return Ok(()); // client gone
                }
                if writer.flush().await.is_err() {
                    return Ok(());
                }
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                // Slow subscriber missed messages; keep going with the tail.
                continue;
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => return Ok(()),
        }
    }
}
