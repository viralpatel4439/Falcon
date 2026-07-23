use crate::rest::{config as config_api, handlers, messaging, streams};
use crate::state::AppState;
use axum::routing::post;
use crate::ws;
use axum::extract::State;
use axum::http::{Request, StatusCode};
use axum::middleware::{self, Next};
use axum::response::Response;
use axum::routing::get;
use axum::Router;
use falcon_core::{Feature, FeatureSet, Node};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

/// Build a router with every product enabled (the `full` node). Convenience
/// for embedders and tests; production `serve` uses [`router_for`] with the
/// node's actual profile feature set.
pub fn router(node: Arc<Node>) -> Router {
    router_for(node, FeatureSet::all(), falcon_core::default_profile_path())
}

/// Build the router for a node, gating each product's routes on whether that
/// product is active in the node's profile. A cache-only node exposes only the
/// KV routes; `/topics/*`, `/queues/*`, `/streams/*` are simply not mounted, so
/// they 404 with "feature not installed" semantics.
pub fn router_for(node: Arc<Node>, features: FeatureSet, profile_path: PathBuf) -> Router {
    let features = Arc::new(features);
    let state = AppState {
        node,
        features: features.clone(),
        profile_path: Arc::new(profile_path),
    };

    let max_body = state.node.config().storage.max_value_bytes;

    // Always-on: UI, probes, metrics, health, and the config read/write API
    // (the UI's CLI-equivalent config path).
    let mut app = Router::new()
        .route("/", get(handlers::dashboard))
        .route("/healthz", get(handlers::healthz))
        .route("/readyz", get(handlers::readyz))
        .route("/metrics", get(handlers::metrics))
        .route("/health", get(handlers::health))
        .route("/config", get(config_api::get_config).post(config_api::set_config));

    // Key-value + realtime routes: present for the Cache and KV products.
    if features.contains(Feature::Cache) || features.contains(Feature::Kv) {
        app = app
            .route("/subscribe", get(ws::handler::ws_handler))
            .route("/kv", get(handlers::scan_default))
            .route(
                "/kv/{key}",
                get(handlers::get_key_default)
                    .put(handlers::put_key_default)
                    .delete(handlers::delete_key_default),
            )
            .route("/keyspaces/{keyspace}/kv", get(handlers::scan_keyspace))
            .route(
                "/keyspaces/{keyspace}/kv/{key}",
                get(handlers::get_key_keyspace)
                    .put(handlers::put_key_keyspace)
                    .delete(handlers::delete_key_keyspace),
            );
    }

    // Falcon Event Stream.
    if features.contains(Feature::Stream) {
        app = app
            .route("/streams/{stream}", get(streams::info))
            .route("/streams/{stream}/records", post(streams::append))
            .route("/streams/{stream}/poll", get(streams::poll))
            .route("/streams/{stream}/commit", post(streams::commit));
    }

    // Falcon Pub/Sub.
    if features.contains(Feature::Pubsub) {
        app = app.route("/topics/{topic}/publish", post(messaging::publish));
    }

    // Falcon Queue.
    if features.contains(Feature::Queue) {
        app = app
            .route("/queues/{queue}/push", post(messaging::push))
            .route("/queues/{queue}/pop", post(messaging::pop))
            .route("/queues/{queue}/ack", post(messaging::ack));
    }

    // Anti-OOM: cap request body size so a single huge PUT can't exhaust
    // memory. 0 disables the cap. Applied before handlers run.
    if max_body > 0 {
        app = app.layer(axum::extract::DefaultBodyLimit::max(max_body));
    }

    // Only attach the auth layer when a token is configured — zero cost
    // (not even a layer in the stack) when auth is off.
    if state.node.config().auth.is_enabled() {
        app = app.layer(middleware::from_fn_with_state(state.clone(), auth_middleware));
    }

    app.with_state(state)
}

/// Rejects requests without the API key. The key may be presented as an
/// `Authorization: Bearer <key>` header (preferred — not logged) or an
/// `api_key=<key>` query parameter (fallback for browser WebSocket clients,
/// which cannot set handshake headers).
///
/// The query-param form is only as safe as the transport: use TLS so the URL
/// isn't sniffable, and note URLs may appear in proxy/access logs.
/// `/healthz` is always exempt so liveness probes work unauthenticated.
async fn auth_middleware(
    State(state): State<AppState>,
    req: Request<axum::body::Body>,
    next: Next,
) -> Result<Response, StatusCode> {
    // Liveness/readiness/metrics endpoints and the static dashboard page are
    // always unauthenticated so probes, scrapers, and the UI shell load without
    // a key. (The dashboard's data calls DO carry the key and are gated.)
    // GET /config and /health are read-only shell data the UI needs before a
    // key is entered; POST /config (the config write path) is NOT exempt.
    let path = req.uri().path();
    let exempt = matches!(path, "/" | "/healthz" | "/readyz" | "/metrics" | "/health")
        || (path == "/config" && req.method() == axum::http::Method::GET);
    if exempt {
        return Ok(next.run(req).await);
    }
    let token = &state.node.config().auth.api_key;

    // 1. Authorization: Bearer <key>
    let header_key = req
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));

    // 2. ?api_key=<key> query param (WebSocket/browser fallback).
    let query_key = req.uri().query().and_then(|q| {
        q.split('&')
            .find_map(|kv| kv.strip_prefix("api_key="))
    });

    let presented = header_key.or(query_key).unwrap_or("");
    if constant_time_eq(presented.as_bytes(), token.as_bytes()) {
        Ok(next.run(req).await)
    } else {
        Err(StatusCode::UNAUTHORIZED)
    }
}

/// Length-independent-ish constant-time comparison to avoid leaking the
/// token via response timing.
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

pub async fn serve(
    node: Arc<Node>,
    bind: SocketAddr,
    features: FeatureSet,
    profile_path: PathBuf,
) -> std::io::Result<()> {
    let app = router_for(node, features, profile_path);
    let listener = tokio::net::TcpListener::bind(bind).await?;
    tracing::info!(%bind, "HTTP/WebSocket server listening");
    axum::serve(listener, app).await
}

/// Like `serve`, but stops accepting new connections and drains in-flight
/// requests when `shutdown` resolves — the graceful path for SIGTERM during
/// an autoscale/rollout. The caller performs the final durable flush after
/// this returns.
pub async fn serve_with_shutdown<F>(
    node: Arc<Node>,
    bind: SocketAddr,
    features: FeatureSet,
    profile_path: PathBuf,
    shutdown: F,
) -> std::io::Result<()>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let app = router_for(node, features, profile_path);
    let listener = tokio::net::TcpListener::bind(bind).await?;
    tracing::info!(%bind, "HTTP/WebSocket server listening (graceful shutdown enabled)");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await
}
