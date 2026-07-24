use crate::state::AppState;
use axum::extract::State;
use axum::response::IntoResponse;
use axum::Json;
use serde::Serialize;

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
    /// The installed Falcon products active on this node (from its profile),
    /// e.g. `["cache"]`. Drives which UI the browser renders.
    pub products: Vec<String>,
    /// The primary product's short name, so the UI can pick its single view
    /// without guessing. `None` only if nothing is installed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub primary_product: Option<String>,
}

/// The named Falcon components and whether each is active on this node.
#[derive(Serialize)]
pub struct FalconComponents {
    /// Falcon KV Store — the key-value store (always present).
    pub falcon_db: bool,
    /// Falcon Queue — durable work queues.
    pub falcon_queue: bool,
    /// Falcon Pub/Sub — topics.
    pub falcon_pubsub: bool,
    /// Falcon KV Store — real-time WebSocket updates.
    pub falcon_realtime_db: bool,
}

/// The embedded UI — a *separate* self-contained page per product. The page
/// served at `/` is chosen from the node's primary installed product, so a
/// cache-only node shows the Cache UI, a pubsub node shows the Pub/Sub UI, and
/// so on. A full/multi-product node (or an empty profile) falls back to the
/// combined dashboard. No external assets, no build step.
pub async fn dashboard(State(state): State<AppState>) -> impl IntoResponse {
    use falcon_core::Feature;
    let html: &'static str = match state.features.iter().next() {
        // Exactly-one-product nodes get that product's dedicated UI.
        Some(f) if state.features.len() == 1 => match f {
            Feature::Cache => include_str!("../../assets/ui_cache.html"),
            Feature::Kv => include_str!("../../assets/ui_kv.html"),
            Feature::Pubsub => include_str!("../../assets/ui_pubsub.html"),
            Feature::Queue => include_str!("../../assets/ui_queue.html"),
            Feature::Stream => include_str!("../../assets/ui_stream.html"),
        },
        // Multi-product (full) or none-installed: the combined dashboard.
        _ => include_str!("../../assets/dashboard.html"),
    };
    (
        [(axum::http::header::CONTENT_TYPE, "text/html; charset=utf-8")],
        html,
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
                #[cfg(feature = "cold")]
                tiering: ks.tier_stats().map(|s| TieringHealth {
                    hot_hit_rate: s.hit_rate(),
                    hot_keys: s.hot_keys,
                    hot_bytes: s.hot_bytes,
                    evictions: s.evictions,
                    promotions: s.promotions,
                }),
                #[cfg(not(feature = "cold"))]
                tiering: None,
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
        products: state.features.iter().map(|f| f.as_str().to_string()).collect(),
        primary_product: state.features.iter().next().map(|f| f.as_str().to_string()),
    })
}

/// `/health` — the same payload as `/healthz`, exposed under the name the CLI
/// and per-feature UI fetch. Kept as a thin alias so both paths stay in sync.
pub async fn health(state: State<AppState>) -> impl IntoResponse {
    healthz(state).await
}
