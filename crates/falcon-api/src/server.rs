use crate::rest::{handlers, streams};
use crate::state::AppState;
use axum::routing::post;
use crate::ws;
use axum::extract::State;
use axum::http::{Request, StatusCode};
use axum::middleware::{self, Next};
use axum::response::Response;
use axum::routing::get;
use axum::Router;
use falcon_core::Node;
use std::net::SocketAddr;
use std::sync::Arc;

pub fn router(node: Arc<Node>) -> Router {
    let state = AppState { node };

    let max_body = state.node.config().storage.max_value_bytes;

    let mut app = Router::new()
        .route("/healthz", get(handlers::healthz))
        .route("/readyz", get(handlers::readyz))
        .route("/metrics", get(handlers::metrics))
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
        )
        // Falcon Event Streaming: append / poll / commit + metadata.
        .route("/streams/{stream}", get(streams::info))
        .route("/streams/{stream}/records", post(streams::append))
        .route("/streams/{stream}/poll", get(streams::poll))
        .route("/streams/{stream}/commit", post(streams::commit));

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
    // Liveness/readiness/metrics endpoints are always unauthenticated so
    // probes and scrapers work without the key.
    if matches!(req.uri().path(), "/healthz" | "/readyz" | "/metrics") {
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

pub async fn serve(node: Arc<Node>, bind: SocketAddr) -> std::io::Result<()> {
    let app = router(node);
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
    shutdown: F,
) -> std::io::Result<()>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let app = router(node);
    let listener = tokio::net::TcpListener::bind(bind).await?;
    tracing::info!(%bind, "HTTP/WebSocket server listening (graceful shutdown enabled)");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await
}
