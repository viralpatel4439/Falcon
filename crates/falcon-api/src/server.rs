use crate::rest::{config as config_api, handlers, simple};
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

    // Realtime subscription (WebSocket) is available whenever a KV/cache
    // product is present.
    if features.contains(Feature::Cache) || features.contains(Feature::Kv) {
        app = app.route("/subscribe", get(ws::handler::ws_handler));
    }

    // Falcon Cache — POST /cache {key,value,ttl?} · GET/DELETE /cache?key=
    // No scan: a cache is exact-key lookup by design. Entries expire and are
    // evicted, so enumerating a cache returns a racy, partial snapshot and walks
    // the very keyspace the tiering exists to avoid. Listing is a store's job — see /kv/scan.
    if features.contains(Feature::Cache) {
        app = app.route(
            "/cache",
            get(simple::cache_read)
                .post(simple::cache_write)
                .delete(simple::cache_delete),
        );
    }

    // Falcon KV Store — POST /kv {key,value} · GET/DELETE /kv?key=
    if features.contains(Feature::Kv) {
        app = app
            .route(
                "/kv",
                get(simple::kv_read_h)
                    .post(simple::kv_write_h)
                    .delete(simple::kv_delete_h),
            )
            .route("/kv/scan", get(simple::kv_scan_h));
    }

    // Falcon Event Stream — POST /stream {key,value} · GET /stream (next batch)
    if features.contains(Feature::Stream) {
        app = app.route("/stream", get(simple::stream_next).post(simple::stream_append));
    }

    // Falcon Pub/Sub — POST /pubsub {value}
    if features.contains(Feature::Pubsub) {
        app = app.route("/pubsub", post(simple::pubsub_publish));
    }

    // Falcon Queue — POST /queue {value} · GET /queue (dequeue) · POST /queue/ack {id}
    if features.contains(Feature::Queue) {
        app = app
            .route("/queue", get(simple::queue_pop).post(simple::queue_push))
            .route("/queue/ack", post(simple::queue_ack));
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
    // Load TLS once (shared loader) before building the app so a cert error
    // fails fast at startup rather than mid-serve.
    let tls = falcon_core::tls::load_server_config(&node.config().tls)?;
    let app = router_for(node, features, profile_path);
    let listener = tokio::net::TcpListener::bind(bind).await?;
    match tls {
        None => {
            tracing::info!(%bind, "HTTP/WebSocket server listening (graceful shutdown enabled)");
            axum::serve(listener, app)
                .with_graceful_shutdown(shutdown)
                .await
        }
        Some(tls) => {
            tracing::info!(%bind, "HTTPS/WSS server listening [TLS] (graceful shutdown enabled)");
            serve_tls(listener, app, tls, shutdown).await
        }
    }
}

/// Serve the axum app over rustls. Each accepted TCP connection is TLS-wrapped
/// with `tokio-rustls`, then handed to hyper with HTTP/1 + HTTP/2 auto-detect.
/// The TLS handshake is per-connection (Falcon uses persistent connections), so
/// the per-request cost is just AES-NI-accelerated record encryption (µs).
async fn serve_tls<F>(
    listener: tokio::net::TcpListener,
    app: Router,
    tls: std::sync::Arc<rustls::ServerConfig>,
    shutdown: F,
) -> std::io::Result<()>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    use hyper_util::rt::{TokioExecutor, TokioIo};
    use hyper_util::server::conn::auto::Builder as ConnBuilder;
    use tower::Service;

    let acceptor = tokio_rustls::TlsAcceptor::from(tls);
    let mut shutdown = std::pin::pin!(shutdown);
    loop {
        let (stream, _peer) = tokio::select! {
            _ = &mut shutdown => break,
            accepted = listener.accept() => match accepted {
                Ok(v) => v,
                Err(e) => { tracing::warn!(error = %e, "accept failed"); continue; }
            },
        };
        let acceptor = acceptor.clone();
        let app = app.clone();
        tokio::spawn(async move {
            let tls_stream = match acceptor.accept(stream).await {
                Ok(s) => s,
                Err(e) => {
                    tracing::debug!(error = %e, "TLS handshake failed");
                    return;
                }
            };
            // Adapt the tower Service (axum Router) into a hyper service.
            let svc = hyper::service::service_fn(move |req| {
                let mut app = app.clone();
                async move { app.call(req).await }
            });
            if let Err(e) = ConnBuilder::new(TokioExecutor::new())
                .serve_connection_with_upgrades(TokioIo::new(tls_stream), svc)
                .await
            {
                tracing::debug!(error = %e, "TLS connection error");
            }
        });
    }
    Ok(())
}
