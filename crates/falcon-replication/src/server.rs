use crate::log_reader::ReplicationLogReader;
use futures::Stream;
use falcon_events::EventBus;
use falcon_proto::replication::replication_server::Replication;
use falcon_proto::replication::{
    forward_write_request, ChangeEventProto, ForwardWriteRequest, ForwardWriteResponse,
    HandshakeRequest, HandshakeResponse, SnapshotChunk, SnapshotRequest, StreamChangesRequest,
};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tonic::{Request, Response, Status};

/// Applies a write forwarded from a non-primary region on the primary. Returns
/// the sequence the primary assigned after committing it durably. Wired to the
/// keyspace's normal ordered write path, so forwarded writes join the same
/// single-writer queue as the primary's own writes (total order, no loss).
pub type ForwardApplyFn = Arc<
    dyn Fn(ForwardedWrite) -> Pin<Box<dyn Future<Output = Result<u64, String>> + Send>>
        + Send
        + Sync,
>;

/// A decoded forwarded write handed to [`ForwardApplyFn`].
pub struct ForwardedWrite {
    pub key: Vec<u8>,
    /// `Some(value)` = put, `None` = delete.
    pub value: Option<Vec<u8>>,
    pub ttl_secs: u64,
}

/// Per-keyspace replication resources a leader needs to serve followers:
/// the durable log to read from, and the event bus to wake up a caught-up
/// follower stream with low latency instead of polling. In primary-queue mode
/// the primary also carries an `apply_forwarded` callback to commit writes
/// forwarded from other regions.
pub struct KeyspaceReplicationSource {
    pub log_reader: Arc<dyn ReplicationLogReader>,
    pub events: EventBus,
    pub apply_forwarded: Option<ForwardApplyFn>,
}

pub struct ReplicationServerImpl {
    node_id: String,
    keyspaces: HashMap<String, KeyspaceReplicationSource>,
    /// Optional shared-secret token. Empty = auth off (no checks).
    auth_token: String,
}

impl ReplicationServerImpl {
    pub fn new(node_id: String, keyspaces: HashMap<String, KeyspaceReplicationSource>) -> Self {
        Self {
            node_id,
            keyspaces,
            auth_token: String::new(),
        }
    }

    pub fn with_auth_token(mut self, token: String) -> Self {
        self.auth_token = token;
        self
    }

    /// Checks the `authorization` metadata against the configured token.
    /// A no-op when auth is off. (`Status` is large but that's tonic's type,
    /// used by every RPC method here — boxing one helper would be inconsistent.)
    #[allow(clippy::result_large_err)]
    fn check_auth<T>(&self, request: &Request<T>) -> Result<(), Status> {
        if self.auth_token.is_empty() {
            return Ok(());
        }
        let presented = request
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        // constant-time-ish compare
        let (a, b) = (presented.as_bytes(), self.auth_token.as_bytes());
        let ok = a.len() == b.len() && a.iter().zip(b).fold(0u8, |d, (x, y)| d | (x ^ y)) == 0;
        if ok {
            Ok(())
        } else {
            Err(Status::unauthenticated("invalid or missing replication token"))
        }
    }
}

type ChangeStream = Pin<Box<dyn Stream<Item = Result<ChangeEventProto, Status>> + Send>>;
type SnapshotStream = Pin<Box<dyn Stream<Item = Result<SnapshotChunk, Status>> + Send>>;

#[tonic::async_trait]
impl Replication for ReplicationServerImpl {
    type StreamChangesStream = ChangeStream;
    type GetSnapshotStream = SnapshotStream;

    async fn handshake(
        &self,
        request: Request<HandshakeRequest>,
    ) -> Result<Response<HandshakeResponse>, Status> {
        self.check_auth(&request)?;
        let req = request.into_inner();
        tracing::info!(follower_node_id = %req.node_id, region = %req.region, "follower handshake");
        // Single-keyspace-agnostic handshake: current_sequence reported here
        // is informational; StreamChanges reports the authoritative one per
        // keyspace since different keyspaces may be at different sequences.
        let current_sequence = self
            .keyspaces
            .values()
            .map(|src| src.log_reader.current_sequence())
            .max()
            .unwrap_or(0);
        Ok(Response::new(HandshakeResponse {
            leader_node_id: self.node_id.clone(),
            role: "leader".to_string(),
            current_sequence,
        }))
    }

    async fn get_snapshot(
        &self,
        request: Request<SnapshotRequest>,
    ) -> Result<Response<Self::GetSnapshotStream>, Status> {
        self.check_auth(&request)?;
        let req = request.into_inner();
        let source = self
            .keyspaces
            .get(&req.keyspace)
            .ok_or_else(|| Status::not_found(format!("unknown keyspace '{}'", req.keyspace)))?;

        let entries = source
            .log_reader
            .read_from(0)
            .map_err(|e| Status::internal(e.to_string()))?;
        let snapshot_sequence = source.log_reader.current_sequence();

        let chunk = SnapshotChunk {
            entries: entries.iter().map(ChangeEventProto::from).collect(),
            snapshot_sequence,
            is_final: true,
        };

        let stream = tokio_stream::once(Ok(chunk));
        Ok(Response::new(Box::pin(stream)))
    }

    async fn stream_changes(
        &self,
        request: Request<StreamChangesRequest>,
    ) -> Result<Response<Self::StreamChangesStream>, Status> {
        self.check_auth(&request)?;
        let req = request.into_inner();
        let source = self
            .keyspaces
            .get(&req.keyspace)
            .ok_or_else(|| Status::not_found(format!("unknown keyspace '{}'", req.keyspace)))?;

        tracing::info!(
            follower_node_id = %req.follower_node_id,
            keyspace = %req.keyspace,
            resume_sequence = req.resume_sequence,
            "follower starting stream"
        );

        let log_reader = Arc::clone(&source.log_reader);
        let mut wake = source.events.subscribe();
        let keyspace = req.keyspace.clone();

        let stream = async_stream::stream! {
            let mut last_sent = req.resume_sequence;

            // Historical catch-up first.
            match log_reader.read_from(last_sent) {
                Ok(events) => {
                    for event in &events {
                        last_sent = last_sent.max(event.sequence);
                        yield Ok(ChangeEventProto::from(event));
                    }
                }
                Err(e) => {
                    yield Err(Status::internal(e.to_string()));
                    return;
                }
            }

            // Live tail. A new local write fires a wake on the broadcast
            // channel; we then re-read the durable log from `last_sent` (which
            // safely absorbs bursts, group-commit coalescing, and out-of-order
            // submit). The wake is only a *hint* — never the source of truth.
            //
            // A wake can be missed (channel lag, or a write landing between the
            // historical read and the first `recv`). To guarantee the follower
            // always converges regardless of any wake race, we wait on the wake
            // with a short timeout and re-read on EVERY tick as a safety poll:
            // even if no wake ever arrives, a stranded tail is picked up within
            // one poll interval. Correctness no longer depends on the wake.
            const SAFETY_POLL: std::time::Duration = std::time::Duration::from_millis(25);
            // Loop until the wake channel closes (the keyspace is gone). Every
            // other outcome — a wake, a lag, or a poll timeout — re-reads the log.
            while !matches!(
                tokio::time::timeout(SAFETY_POLL, wake.recv()).await,
                Ok(Err(tokio::sync::broadcast::error::RecvError::Closed))
            ) {
                match log_reader.read_from(last_sent) {
                    Ok(events) => {
                        for event in &events {
                            last_sent = last_sent.max(event.sequence);
                            yield Ok(ChangeEventProto::from(event));
                        }
                    }
                    Err(e) => {
                        yield Err(Status::internal(e.to_string()));
                        return;
                    }
                }
            }
            let _ = keyspace;
        };

        Ok(Response::new(Box::pin(stream)))
    }

    async fn forward_write(
        &self,
        request: Request<ForwardWriteRequest>,
    ) -> Result<Response<ForwardWriteResponse>, Status> {
        self.check_auth(&request)?;
        let req = request.into_inner();
        let source = self
            .keyspaces
            .get(&req.keyspace)
            .ok_or_else(|| Status::not_found(format!("unknown keyspace '{}'", req.keyspace)))?;
        let apply = source.apply_forwarded.as_ref().ok_or_else(|| {
            Status::failed_precondition(format!(
                "keyspace '{}' is not a primary-queue primary here",
                req.keyspace
            ))
        })?;

        let value = match req.value {
            Some(forward_write_request::Value::PutValue(v)) => Some(v),
            Some(forward_write_request::Value::Tombstone(_)) | None => None,
        };
        let write = ForwardedWrite {
            key: req.key,
            value,
            ttl_secs: req.ttl_secs,
        };
        // Commit through the primary's ordered write path.
        let sequence = apply(write)
            .await
            .map_err(|e| Status::internal(format!("forwarded write failed: {e}")))?;
        Ok(Response::new(ForwardWriteResponse { sequence }))
    }
}
