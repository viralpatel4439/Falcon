//! Wire protocol constants and response encoding.
//!
//! A lean, length-delimited binary protocol over a persistent TCP stream,
//! designed for pipelining: because every field is length-prefixed, a
//! reader can walk a stream of concatenated requests and a client can send
//! many requests back-to-back without waiting for replies.
//!
//! Request frame (little-endian):
//!   [op:u8][flags:u8][keyspace_len:u16][keyspace][key_len:u32][key][val_len:u32][val]
//! Response frame:
//!   [status:u8][val_len:u32][val]
//!
//! One TCP connection is a single strictly-ordered stream; responses are
//! written back in request order, so no request IDs are needed (same model
//! as Redis RESP).

use bytes::{BufMut, Bytes, BytesMut};

// KV opcodes.
pub const OP_PING: u8 = 0x00;
pub const OP_GET: u8 = 0x01;
pub const OP_SET: u8 = 0x02;
pub const OP_DEL: u8 = 0x03;
/// Authenticate a connection: value = token. Required as the first frame
/// when auth is enabled; a no-op (always OK) when auth is off.
pub const OP_AUTH: u8 = 0x05;

// Messaging opcodes. For these, the frame is reused as:
//   keyspace = topic/queue name, key = group/offset, value = payload.
pub const OP_PUBLISH: u8 = 0x10; // keyspace=topic, value=payload
pub const OP_SUBSCRIBE: u8 = 0x11; // keyspace=topic; connection becomes a subscriber stream
pub const OP_PUSH: u8 = 0x12; // keyspace=queue, value=payload
pub const OP_POP: u8 = 0x13; // keyspace=queue, key=group -> returns offset(8B)+payload
pub const OP_ACK: u8 = 0x14; // keyspace=queue, key=group, value=offset(8B)
// Falcon Event Streaming. keyspace=stream name, key=partition key,
// value=payload. Returns a Stored{partition,offset} frame. The high-throughput
// producer path; consumer poll/commit go over REST (request/response).
pub const OP_STREAM_APPEND: u8 = 0x20;

// Status codes.
pub const STATUS_OK: u8 = 0x00;
pub const STATUS_NOT_FOUND: u8 = 0x01;
pub const STATUS_BAD_REQUEST: u8 = 0x02;
pub const STATUS_UNKNOWN_KEYSPACE: u8 = 0x03;
pub const STATUS_SERVER_ERROR: u8 = 0x04;
pub const STATUS_PONG: u8 = 0x05;
pub const STATUS_EMPTY: u8 = 0x06; // POP found nothing
pub const STATUS_MESSAGE: u8 = 0x07; // a pushed subscription message / POP result
pub const STATUS_UNKNOWN_TOPIC: u8 = 0x08;
pub const STATUS_UNKNOWN_QUEUE: u8 = 0x09;
pub const STATUS_UNAUTHORIZED: u8 = 0x0a; // auth required or token mismatch
pub const STATUS_UNKNOWN_STREAM: u8 = 0x0b; // stream name not configured
pub const STATUS_STORED: u8 = 0x0c; // stream append: payload = partition(4B)+offset(8B)

/// Reject absurd/hostile frame sizes rather than allocating for them.
pub const MAX_FRAME: usize = 64 * 1024 * 1024;

/// A decoded request. `key`/`value`/`keyspace` are zero-copy views into
/// the connection's read buffer.
#[derive(Debug, Clone)]
pub struct Request {
    pub op: u8,
    pub flags: u8,
    pub keyspace: Bytes, // empty => default keyspace
    pub key: Bytes,
    pub value: Bytes, // empty for GET/DEL/PING
}

/// A response to encode back to the client.
#[derive(Debug, Clone)]
pub enum Response {
    Ok,                    // SET/DEL/PUBLISH/PUSH/ACK/SUBSCRIBE success
    Value(Vec<u8>),        // GET hit
    NotFound,              // GET miss
    Pong,                  //
    BadRequest,            //
    UnknownKeyspace,       //
    ServerError,           //
    Empty,                 // POP found nothing
    UnknownTopic,          //
    UnknownQueue,          //
    Unauthorized,          // auth required / token mismatch
    UnknownStream,         // stream name not configured
    /// A queue POP result or a pushed subscription message: an 8-byte
    /// big-endian offset followed by the payload.
    Message { offset: u64, payload: Vec<u8> },
    /// A stream append result: the partition (4B) and offset (8B) assigned.
    Stored { partition: u32, offset: u64 },
}

/// Appends a request frame to `out` (little-endian). Public so clients
/// (e.g. the benchmark harness) can build pipelined requests without
/// re-implementing the framing.
pub fn encode_request(out: &mut BytesMut, op: u8, keyspace: &[u8], key: &[u8], value: &[u8]) {
    out.put_u8(op);
    out.put_u8(0); // flags
    out.put_u16_le(keyspace.len() as u16);
    out.put_slice(keyspace);
    out.put_u32_le(key.len() as u32);
    out.put_slice(key);
    out.put_u32_le(value.len() as u32);
    out.put_slice(value);
}

impl Response {
    /// Appends this response's frame to `out` (little-endian).
    pub fn encode(&self, out: &mut BytesMut) {
        match self {
            Response::Ok => {
                out.put_u8(STATUS_OK);
                out.put_u32_le(0);
            }
            Response::Value(v) => {
                out.put_u8(STATUS_OK);
                out.put_u32_le(v.len() as u32);
                out.put_slice(v);
            }
            Response::NotFound => {
                out.put_u8(STATUS_NOT_FOUND);
                out.put_u32_le(0);
            }
            Response::Pong => {
                out.put_u8(STATUS_PONG);
                out.put_u32_le(0);
            }
            Response::BadRequest => {
                out.put_u8(STATUS_BAD_REQUEST);
                out.put_u32_le(0);
            }
            Response::UnknownKeyspace => {
                out.put_u8(STATUS_UNKNOWN_KEYSPACE);
                out.put_u32_le(0);
            }
            Response::ServerError => {
                out.put_u8(STATUS_SERVER_ERROR);
                out.put_u32_le(0);
            }
            Response::Empty => {
                out.put_u8(STATUS_EMPTY);
                out.put_u32_le(0);
            }
            Response::UnknownTopic => {
                out.put_u8(STATUS_UNKNOWN_TOPIC);
                out.put_u32_le(0);
            }
            Response::UnknownQueue => {
                out.put_u8(STATUS_UNKNOWN_QUEUE);
                out.put_u32_le(0);
            }
            Response::Unauthorized => {
                out.put_u8(STATUS_UNAUTHORIZED);
                out.put_u32_le(0);
            }
            Response::Message { offset, payload } => {
                out.put_u8(STATUS_MESSAGE);
                // payload framing carries offset(8B) + payload bytes
                out.put_u32_le((8 + payload.len()) as u32);
                out.put_u64_le(*offset);
                out.put_slice(payload);
            }
            Response::UnknownStream => {
                out.put_u8(STATUS_UNKNOWN_STREAM);
                out.put_u32_le(0);
            }
            Response::Stored { partition, offset } => {
                out.put_u8(STATUS_STORED);
                // payload carries partition(4B) + offset(8B)
                out.put_u32_le(12);
                out.put_u32_le(*partition);
                out.put_u64_le(*offset);
            }
        }
    }
}
