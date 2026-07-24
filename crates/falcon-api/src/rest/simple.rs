//! The simple, plug-and-play REST surface.
//!
//! One product = one URL root. A user never sees Falcon's internal concepts
//! (keyspaces, consumer groups, partitions, offsets, topic/queue/stream names).
//! They send a small JSON body and pick the operation with the HTTP method:
//!
//! ```text
//! Cache (exact-key lookup with TTL; no scan — see the note below):
//!   POST   /cache        { "key": "...", "value": "...", "ttl": 300 }  -> { "ok": true }
//!   GET    /cache?key=...                                              -> { "value": "..." }
//!   DELETE /cache?key=...                                              -> { "ok": true }
//!
//! KV Store (durable key-value; adds scan since a store is meant to be listed):
//!   POST   /kv           { "key": "...", "value": "..." }              -> { "ok": true }
//!   GET    /kv?key=...                                                 -> { "value": "..." }
//!   DELETE /kv?key=...                                                 -> { "ok": true }
//!   GET    /kv/scan?prefix=...                                         -> { "items": [...] }
//!
//! Pub/Sub:
//!   POST   /pubsub       { "value": "..." }                            -> { "ok": true }   (publish)
//!
//! Queue:
//!   POST   /queue        { "value": "..." }                            -> { "ok": true }   (enqueue)
//!   GET    /queue                                                      -> { "id": N, "value": "..." } (dequeue)
//!   POST   /queue/ack    { "id": N }                                   -> { "ok": true }
//!
//! Stream:
//!   POST   /stream       { "key": "...", "value": "..." }              -> { "ok": true }   (append)
//!   GET    /stream                                                     -> { "items": [ { "value": "..." } ] }
//! ```
//!
//! `value` is always a string on the wire — the client JSON-stringifies whatever
//! it has (number, string, object) before sending and parses it back on read, so
//! Falcon stores the value verbatim and stays schema-free.

use crate::rest::error::ApiError;
use crate::state::AppState;
use axum::extract::{Query, State};
use axum::Json;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// The single, fixed resource each product owns on a node. Users never name
// these — the route already knows which product it is.
pub const CACHE_KEYSPACE: &str = "cache";
pub const KV_KEYSPACE: &str = "default";
const TOPIC: &str = "events";
const QUEUE: &str = "jobs";
const STREAM: &str = "events";
// Hidden defaults for the messaging internals a simple user shouldn't manage.
const GROUP: &str = "default";

// ---------- request / response bodies ----------

#[derive(Deserialize)]
pub struct KvWrite {
    pub key: String,
    /// The value as a string (client JSON-stringifies anything into it).
    pub value: String,
    /// Optional time-to-live in seconds. Omit for no expiry.
    #[serde(default)]
    pub ttl: Option<u64>,
}

#[derive(Deserialize)]
pub struct ValueOnly {
    pub value: String,
}

#[derive(Deserialize)]
pub struct KeyedValue {
    #[serde(default)]
    pub key: String,
    pub value: String,
}

#[derive(Deserialize)]
pub struct AckBody {
    pub id: u64,
}

#[derive(Serialize)]
pub struct Ok {
    pub ok: bool,
}
fn ok() -> Json<Ok> {
    Json(Ok { ok: true })
}

#[derive(Serialize)]
pub struct ValueResponse {
    pub value: String,
}

#[derive(Serialize)]
pub struct Item {
    pub key: String,
    pub value: String,
}

#[derive(Serialize)]
pub struct Items {
    pub items: Vec<Item>,
}

#[derive(Serialize)]
pub struct Dequeued {
    pub id: u64,
    pub value: String,
}

fn key_param(params: &HashMap<String, String>) -> Result<String, ApiError> {
    params
        .get("key")
        .filter(|k| !k.is_empty())
        .cloned()
        .ok_or_else(|| ApiError::BadRequest("missing ?key=".into()))
}

// ---------- Cache / KV ----------

/// `POST /cache` or `/kv` — write a key. `keyspace` is bound by the route.
async fn kv_write(state: &AppState, keyspace: &str, body: KvWrite) -> Result<Json<Ok>, ApiError> {
    let m = state.node.metrics();
    m.http_requests_total.inc();
    m.kv_put_total.inc();
    let _timer = m.kv_put_latency.start();
    let ks = state.node.require_keyspace(keyspace)?;
    ks.put_with_ttl(body.key.as_bytes(), body.value.as_bytes(), body.ttl)
        .await
        .map_err(|e| {
            m.kv_errors_total.inc();
            ApiError::from(e)
        })?;
    m.wal_bytes.set(state.node.total_durable_bytes());
    Ok(ok())
}

async fn kv_read(
    state: &AppState,
    keyspace: &str,
    params: &HashMap<String, String>,
) -> Result<Json<ValueResponse>, ApiError> {
    let m = state.node.metrics();
    m.http_requests_total.inc();
    m.kv_get_total.inc();
    let _timer = m.kv_get_latency.start();
    let key = key_param(params)?;
    let ks = state.node.require_keyspace(keyspace)?;
    match ks.get(key.as_bytes()).await {
        Ok(Some(value)) => {
            m.kv_get_hit_total.inc();
            Ok(Json(ValueResponse {
                value: String::from_utf8_lossy(&value).to_string(),
            }))
        }
        Ok(None) => {
            m.kv_get_miss_total.inc();
            Err(ApiError::NotFound)
        }
        Err(e) => {
            m.kv_errors_total.inc();
            Err(e.into())
        }
    }
}

async fn kv_delete(
    state: &AppState,
    keyspace: &str,
    params: &HashMap<String, String>,
) -> Result<Json<Ok>, ApiError> {
    let m = state.node.metrics();
    m.http_requests_total.inc();
    m.kv_delete_total.inc();
    let key = key_param(params)?;
    let ks = state.node.require_keyspace(keyspace)?;
    ks.delete(key.as_bytes()).await.map_err(|e| {
        m.kv_errors_total.inc();
        ApiError::from(e)
    })?;
    Ok(ok())
}

async fn kv_scan(
    state: &AppState,
    keyspace: &str,
    params: &HashMap<String, String>,
) -> Result<Json<Items>, ApiError> {
    state.node.metrics().http_requests_total.inc();
    state.node.metrics().kv_scan_total.inc();
    let ks = state.node.require_keyspace(keyspace)?;
    let prefix = params.get("prefix").cloned().unwrap_or_default();
    let items = ks
        .scan_prefix(prefix.as_bytes())
        .await?
        .into_iter()
        .map(|(k, v)| Item {
            key: String::from_utf8_lossy(&k).to_string(),
            value: String::from_utf8_lossy(&v).to_string(),
        })
        .collect();
    Ok(Json(Items { items }))
}

// Cache route handlers (bound to the `cache` keyspace).
pub async fn cache_write(State(s): State<AppState>, Json(b): Json<KvWrite>) -> Result<Json<Ok>, ApiError> {
    kv_write(&s, CACHE_KEYSPACE, b).await
}
pub async fn cache_read(State(s): State<AppState>, Query(p): Query<HashMap<String, String>>) -> Result<Json<ValueResponse>, ApiError> {
    kv_read(&s, CACHE_KEYSPACE, &p).await
}
pub async fn cache_delete(State(s): State<AppState>, Query(p): Query<HashMap<String, String>>) -> Result<Json<Ok>, ApiError> {
    kv_delete(&s, CACHE_KEYSPACE, &p).await
}
// Note: the cache has no scan/list. A cache is exact-key lookup; its entries
// expire and evict, so enumerating it is racy and defeats the tiering. Use /kv
// (a store) when you need to list keys.

// KV route handlers (bound to the `default` keyspace).
pub async fn kv_write_h(State(s): State<AppState>, Json(b): Json<KvWrite>) -> Result<Json<Ok>, ApiError> {
    kv_write(&s, KV_KEYSPACE, b).await
}
pub async fn kv_read_h(State(s): State<AppState>, Query(p): Query<HashMap<String, String>>) -> Result<Json<ValueResponse>, ApiError> {
    kv_read(&s, KV_KEYSPACE, &p).await
}
pub async fn kv_delete_h(State(s): State<AppState>, Query(p): Query<HashMap<String, String>>) -> Result<Json<Ok>, ApiError> {
    kv_delete(&s, KV_KEYSPACE, &p).await
}
pub async fn kv_scan_h(State(s): State<AppState>, Query(p): Query<HashMap<String, String>>) -> Result<Json<Items>, ApiError> {
    kv_scan(&s, KV_KEYSPACE, &p).await
}

// ---------- Pub/Sub ----------

/// `POST /pubsub` — publish a value to the node's topic.
pub async fn pubsub_publish(State(s): State<AppState>, Json(b): Json<ValueOnly>) -> Result<Json<Ok>, ApiError> {
    s.node.metrics().http_requests_total.inc();
    let t = s
        .node
        .messaging()
        .topic(TOPIC)
        .ok_or_else(|| ApiError::Internal("pub/sub not installed".into()))?;
    t.publish(b.value.into_bytes())
        .map_err(|e| ApiError::Internal(e.to_string()))?;
    Ok(ok())
}

// ---------- Queue ----------

/// `POST /queue` — enqueue a value.
pub async fn queue_push(State(s): State<AppState>, Json(b): Json<ValueOnly>) -> Result<Json<Ok>, ApiError> {
    s.node.metrics().http_requests_total.inc();
    let q = s
        .node
        .messaging()
        .queue(QUEUE)
        .ok_or_else(|| ApiError::Internal("queue not installed".into()))?;
    q.push(b.value.as_bytes())
        .map_err(|e| ApiError::Internal(e.to_string()))?;
    Ok(ok())
}

/// `GET /queue` — dequeue the next job (starts the ack timer). Returns 204 when
/// the queue is drained. Confirm with `POST /queue/ack {id}`.
pub async fn queue_pop(State(s): State<AppState>) -> Result<axum::response::Response, ApiError> {
    use axum::response::IntoResponse;
    s.node.metrics().http_requests_total.inc();
    let q = s
        .node
        .messaging()
        .queue(QUEUE)
        .ok_or_else(|| ApiError::Internal("queue not installed".into()))?;
    match q.pop(GROUP).map_err(|e| ApiError::Internal(e.to_string()))? {
        Some(msg) => Ok(Json(Dequeued {
            id: msg.offset,
            value: String::from_utf8_lossy(&msg.payload).to_string(),
        })
        .into_response()),
        None => Ok(axum::http::StatusCode::NO_CONTENT.into_response()),
    }
}

/// `POST /queue/ack` — confirm a dequeued job so it isn't redelivered.
pub async fn queue_ack(State(s): State<AppState>, Json(b): Json<AckBody>) -> Result<Json<Ok>, ApiError> {
    s.node.metrics().http_requests_total.inc();
    let q = s
        .node
        .messaging()
        .queue(QUEUE)
        .ok_or_else(|| ApiError::Internal("queue not installed".into()))?;
    q.ack(GROUP, b.id);
    Ok(ok())
}

// ---------- Stream ----------

/// `POST /stream` — append a record. `key` (optional) keeps same-key records
/// ordered; the partition is chosen internally.
pub async fn stream_append(State(s): State<AppState>, Json(b): Json<KeyedValue>) -> Result<Json<Ok>, ApiError> {
    s.node.metrics().http_requests_total.inc();
    let stream = s
        .node
        .messaging()
        .stream(STREAM)
        .ok_or_else(|| ApiError::Internal("stream not installed".into()))?
        .clone();
    let key = b.key.into_bytes();
    let payload = b.value.into_bytes();
    tokio::task::spawn_blocking(move || stream.append_keyed(&key, payload))
        .await
        .map_err(|e| ApiError::Internal(e.to_string()))?
        .map_err(|e| ApiError::Internal(e.to_string()))?;
    Ok(ok())
}

/// `GET /stream` — return the next batch of records for the node's consumer
/// group across every partition, and commit them (at-least-once). A simple
/// consumer just calls this in a loop; ordering per key is preserved because a
/// key's records all live on one partition.
pub async fn stream_next(State(s): State<AppState>) -> Result<Json<Items>, ApiError> {
    s.node.metrics().http_requests_total.inc();
    let stream = s
        .node
        .messaging()
        .stream(STREAM)
        .ok_or_else(|| ApiError::Internal("stream not installed".into()))?;
    let mut items = Vec::new();
    for partition in 0..stream.partition_count() {
        let records = stream
            .poll(GROUP, partition)
            .map_err(|e| ApiError::Internal(e.to_string()))?;
        if let Some(last) = records.last() {
            let last_offset = last.offset;
            for r in &records {
                items.push(Item {
                    key: String::new(),
                    value: String::from_utf8_lossy(&r.payload).to_string(),
                });
            }
            // Commit so the next call returns only new records.
            stream
                .commit(GROUP, partition, last_offset)
                .map_err(|e| ApiError::Internal(e.to_string()))?;
        }
    }
    Ok(Json(Items { items }))
}
