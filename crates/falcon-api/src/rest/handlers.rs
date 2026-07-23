use crate::rest::error::ApiError;
use crate::state::AppState;
use axum::extract::{Path, Query, State};
use axum::response::IntoResponse;
use axum::Json;
use serde::Serialize;
use std::collections::HashMap;

const DEFAULT_KEYSPACE: &str = "default";

#[derive(Serialize)]
pub struct GetResponse {
    pub key: String,
    pub value: String,
    pub sequence: u64,
}

#[derive(Serialize)]
pub struct WriteResponse {
    pub key: String,
    pub sequence: u64,
}

#[derive(Serialize)]
pub struct ScanEntry {
    pub key: String,
    pub value: String,
}

#[derive(Serialize)]
pub struct ScanResponse {
    pub items: Vec<ScanEntry>,
}

#[derive(Serialize)]
pub struct KeyspaceHealth {
    pub name: String,
    pub tier: &'static str,
    pub subscriptions_enabled: bool,
    pub last_applied_sequence: u64,
    pub ttl_tracked_keys: u64,
    /// Present only for `tiered` keyspaces — the hot/cold cost story.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tiering: Option<TieringHealth>,
}

#[derive(Serialize)]
pub struct TieringHealth {
    pub hot_hit_rate: f64,
    pub hot_keys: u64,
    pub hot_bytes: u64,
    pub evictions: u64,
    pub promotions: u64,
}

#[derive(Serialize)]
pub struct FeatureSet {
    pub auth: bool,
    pub wire_protocol: bool,
    pub replication: bool,
    pub subscriptions: bool,
    pub topics: usize,
    pub queues: usize,
    pub streams: usize,
}

#[derive(Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
    /// Product name.
    pub product: &'static str,
    /// The Falcon components exposed by this node.
    pub components: FalconComponents,
    pub node_id: String,
    pub region: String,
    pub replication_enabled: bool,
    pub replication_role: Option<&'static str>,
    /// Exactly which optional features are active. Anything false/0 costs
    /// zero at runtime (skip-if-not-configured).
    pub features: FeatureSet,
    pub keyspaces: Vec<KeyspaceHealth>,
    /// Configured messaging object names (for the dashboard UI).
    pub topics: Vec<String>,
    pub queues: Vec<String>,
    pub streams: Vec<String>,
}

/// The named Falcon components and whether each is active on this node.
#[derive(Serialize)]
pub struct FalconComponents {
    /// FalconDB — the key-value store (always present).
    pub falcon_db: bool,
    /// Falcon Queue — durable work queues.
    pub falcon_queue: bool,
    /// Falcon Pub/Sub — topics.
    pub falcon_pubsub: bool,
    /// Falcon Realtime DB — live WebSocket subscriptions.
    pub falcon_realtime_db: bool,
}

pub async fn get_key_default(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    get_key(&state, DEFAULT_KEYSPACE, &key).await
}

pub async fn put_key_default(
    State(state): State<AppState>,
    Path(key): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    body: axum::body::Bytes,
) -> Result<impl IntoResponse, ApiError> {
    put_key(&state, DEFAULT_KEYSPACE, &key, &body, ttl_from(&params)).await
}

pub async fn delete_key_default(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    delete_key(&state, DEFAULT_KEYSPACE, &key).await
}

pub async fn get_key_keyspace(
    State(state): State<AppState>,
    Path((keyspace, key)): Path<(String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    get_key(&state, &keyspace, &key).await
}

pub async fn put_key_keyspace(
    State(state): State<AppState>,
    Path((keyspace, key)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    body: axum::body::Bytes,
) -> Result<impl IntoResponse, ApiError> {
    put_key(&state, &keyspace, &key, &body, ttl_from(&params)).await
}

pub async fn delete_key_keyspace(
    State(state): State<AppState>,
    Path((keyspace, key)): Path<(String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    delete_key(&state, &keyspace, &key).await
}

pub async fn scan_default(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, ApiError> {
    scan(&state, DEFAULT_KEYSPACE, &params).await
}

pub async fn scan_keyspace(
    State(state): State<AppState>,
    Path(keyspace): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, ApiError> {
    scan(&state, &keyspace, &params).await
}

async fn get_key(state: &AppState, keyspace: &str, key: &str) -> Result<impl IntoResponse, ApiError> {
    let m = state.node.metrics();
    m.http_requests_total.inc();
    m.kv_get_total.inc();
    let _timer = m.kv_get_latency.start();
    let ks = match state.node.require_keyspace(keyspace) {
        Ok(ks) => ks,
        Err(e) => {
            m.kv_errors_total.inc();
            return Err(e.into());
        }
    };
    match ks.get(key.as_bytes()).await {
        Ok(Some(value)) => {
            m.kv_get_hit_total.inc();
            Ok(Json(GetResponse {
                key: key.to_string(),
                value: String::from_utf8_lossy(&value).to_string(),
                sequence: ks.engine().last_applied_sequence(),
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

fn ttl_from(params: &HashMap<String, String>) -> Option<u64> {
    params.get("ttl").and_then(|s| s.parse::<u64>().ok())
}

async fn put_key(
    state: &AppState,
    keyspace: &str,
    key: &str,
    value: &[u8],
    ttl_secs: Option<u64>,
) -> Result<impl IntoResponse, ApiError> {
    let m = state.node.metrics();
    m.http_requests_total.inc();
    m.kv_put_total.inc();
    let _timer = m.kv_put_latency.start();
    let ks = state.node.require_keyspace(keyspace)?;
    let sequence = match ks.put_with_ttl(key.as_bytes(), value, ttl_secs).await {
        Ok(s) => s,
        Err(e) => {
            m.kv_errors_total.inc();
            return Err(e.into());
        }
    };
    m.wal_bytes.set(state.node.total_durable_bytes());
    Ok(Json(WriteResponse {
        key: key.to_string(),
        sequence,
    }))
}

async fn delete_key(state: &AppState, keyspace: &str, key: &str) -> Result<impl IntoResponse, ApiError> {
    let m = state.node.metrics();
    m.http_requests_total.inc();
    m.kv_delete_total.inc();
    let _timer = m.kv_delete_latency.start();
    let ks = state.node.require_keyspace(keyspace)?;
    let sequence = match ks.delete(key.as_bytes()).await {
        Ok(s) => s,
        Err(e) => {
            m.kv_errors_total.inc();
            return Err(e.into());
        }
    };
    Ok(Json(WriteResponse {
        key: key.to_string(),
        sequence,
    }))
}

async fn scan(
    state: &AppState,
    keyspace: &str,
    params: &HashMap<String, String>,
) -> Result<impl IntoResponse, ApiError> {
    state.node.metrics().http_requests_total.inc();
    state.node.metrics().kv_scan_total.inc();
    let ks = state.node.require_keyspace(keyspace)?;
    let prefix = params.get("prefix").cloned().unwrap_or_default();
    let items = ks
        .scan_prefix(prefix.as_bytes())
        .await?
        .into_iter()
        .map(|(k, v)| ScanEntry {
            key: String::from_utf8_lossy(&k).to_string(),
            value: String::from_utf8_lossy(&v).to_string(),
        })
        .collect();
    Ok(Json(ScanResponse { items }))
}

/// The embedded dashboard UI — a single self-contained HTML page (no external
/// assets, no build step) baked into the binary. Served at `/`. It drives the
/// same REST API and `/metrics` a human would.
pub async fn dashboard() -> impl IntoResponse {
    (
        [(axum::http::header::CONTENT_TYPE, "text/html; charset=utf-8")],
        include_str!("../../assets/dashboard.html"),
    )
}

/// Prometheus text-format metrics. Refreshes the durable-size gauge on each
/// scrape so it's current without a background poller. Unauthenticated by
/// design (like `/healthz`) so scrapers/probes work without a key — put the
/// deployment behind network policy if the numbers are sensitive.
pub async fn metrics(State(state): State<AppState>) -> impl IntoResponse {
    let m = state.node.metrics();
    m.wal_bytes.set(state.node.total_durable_bytes());
    (
        [(axum::http::header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        m.encode_prometheus(),
    )
}

/// Readiness probe, distinct from liveness (`/healthz`). Returns 200 only
/// when the node is ready to serve — the process has finished startup and,
/// for a follower, isn't so far behind its leader that it should take
/// traffic. k8s routes traffic on readiness but only restarts on liveness,
/// so a catching-up follower stays alive without receiving reads.
pub async fn readyz(State(state): State<AppState>) -> impl IntoResponse {
    if state.node.metrics().ready.get() == 1 {
        (axum::http::StatusCode::OK, "ready")
    } else {
        (axum::http::StatusCode::SERVICE_UNAVAILABLE, "not ready")
    }
}

pub async fn healthz(State(state): State<AppState>) -> impl IntoResponse {
    let cfg = state.node.config();
    let keyspaces = state
        .node
        .keyspace_names()
        .filter_map(|name| state.node.keyspace(name).map(|ks| (name, ks)))
        .map(|(name, ks)| {
            KeyspaceHealth {
                name: name.to_string(),
                tier: ks.tier().as_str(),
                subscriptions_enabled: ks.events().is_some(),
                last_applied_sequence: ks.engine().last_applied_sequence(),
                ttl_tracked_keys: ks.tracked_ttl_keys() as u64,
                tiering: ks.tier_stats().map(|s| TieringHealth {
                    hot_hit_rate: s.hit_rate(),
                    hot_keys: s.hot_keys,
                    hot_bytes: s.hot_bytes,
                    evictions: s.evictions,
                    promotions: s.promotions,
                }),
            }
        })
        .collect();

    let subscriptions_active = cfg.subscriptions.enabled
        || cfg.keyspaces.iter().any(|k| k.subscriptions);

    Json(HealthResponse {
        status: "ok",
        product: "Falcon",
        components: FalconComponents {
            falcon_db: true, // the KV store is always present
            falcon_queue: !cfg.queues.is_empty(),
            falcon_pubsub: !cfg.topics.is_empty(),
            falcon_realtime_db: subscriptions_active,
        },
        node_id: cfg.node.id.clone(),
        region: cfg.node.region.clone(),
        replication_enabled: cfg.replication.enabled,
        replication_role: cfg.replication.enabled.then_some(match cfg.replication.role {
            falcon_core::ReplicationRole::Leader => "leader",
            falcon_core::ReplicationRole::Follower => "follower",
        }),
        features: FeatureSet {
            auth: cfg.auth.is_enabled(),
            wire_protocol: cfg.wire.enabled,
            replication: cfg.replication.enabled,
            subscriptions: subscriptions_active,
            topics: cfg.topics.len(),
            queues: cfg.queues.len(),
            streams: cfg.streams.len(),
        },
        keyspaces,
        topics: cfg.topics.iter().map(|t| t.name.clone()).collect(),
        queues: cfg.queues.iter().map(|q| q.name.clone()).collect(),
        streams: cfg.streams.iter().map(|s| s.name.clone()).collect(),
    })
}
