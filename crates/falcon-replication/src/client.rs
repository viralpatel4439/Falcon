use falcon_events::ChangeEvent;
use falcon_proto::replication::replication_client::ReplicationClient;
use falcon_proto::replication::{HandshakeRequest, SnapshotRequest, StreamChangesRequest};
use falcon_storage::StorageEngine;
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::Channel;

/// A threshold beyond which a follower prefers a full snapshot over
/// replaying the leader's log entry by entry.
const SNAPSHOT_CATCHUP_THRESHOLD: u64 = 10_000;

const RECONNECT_BACKOFF: [Duration; 5] = [
    Duration::from_millis(200),
    Duration::from_millis(500),
    Duration::from_secs(1),
    Duration::from_secs(3),
    Duration::from_secs(5),
];

/// Follows a single keyspace on a single leader. Runs forever, reconnecting
/// with backoff on failure; never loses data because it always resumes
/// from `engine.last_applied_sequence()`, which is durable on the follower.
pub async fn run_follower(
    leader_addr: String,
    node_id: String,
    region: String,
    keyspace: String,
    engine: Arc<dyn StorageEngine>,
    auth_token: String,
) {
    let mut attempt = 0usize;
    loop {
        match follow_once(&leader_addr, &node_id, &region, &keyspace, &engine, &auth_token).await {
            Ok(()) => {
                tracing::warn!(%keyspace, "replication stream ended, reconnecting");
                attempt = 0;
            }
            Err(e) => {
                tracing::warn!(%keyspace, error = %e, "replication stream failed, will retry");
                attempt += 1;
            }
        }
        let delay = RECONNECT_BACKOFF[attempt.min(RECONNECT_BACKOFF.len() - 1)];
        tokio::time::sleep(delay).await;
    }
}

/// Wrap a message into a `Request` carrying the auth token (if any) as
/// `authorization` metadata. Empty token = no metadata added.
fn authed<T>(msg: T, token: &str) -> tonic::Request<T> {
    let mut req = tonic::Request::new(msg);
    if !token.is_empty() {
        if let Ok(val) = token.parse() {
            req.metadata_mut().insert("authorization", val);
        }
    }
    req
}

async fn follow_once(
    leader_addr: &str,
    node_id: &str,
    region: &str,
    keyspace: &str,
    engine: &Arc<dyn StorageEngine>,
    token: &str,
) -> Result<(), tonic::Status> {
    let channel = Channel::from_shared(leader_addr.to_string())
        .map_err(|e| tonic::Status::invalid_argument(e.to_string()))?
        .connect()
        .await
        .map_err(|e| tonic::Status::unavailable(e.to_string()))?;
    let mut client = ReplicationClient::new(channel);

    let handshake = client
        .handshake(authed(
            HandshakeRequest {
                node_id: node_id.to_string(),
                region: region.to_string(),
            },
            token,
        ))
        .await?
        .into_inner();

    let resume_from = engine.last_applied_sequence();
    let behind = handshake.current_sequence.saturating_sub(resume_from);

    if behind > SNAPSHOT_CATCHUP_THRESHOLD || resume_from == 0 {
        tracing::info!(%keyspace, behind, "requesting snapshot catch-up");
        let mut snapshot_stream = client
            .get_snapshot(authed(
                SnapshotRequest {
                    keyspace: keyspace.to_string(),
                },
                token,
            ))
            .await?
            .into_inner();

        while let Some(chunk) = snapshot_stream.message().await? {
            for proto in chunk.entries {
                let event: ChangeEvent = proto.into();
                apply(engine, &event).await?;
            }
            if chunk.is_final {
                break;
            }
        }
    }

    let resume_from = engine.last_applied_sequence();
    tracing::info!(%keyspace, resume_from, "starting live replication stream");

    let mut stream = client
        .stream_changes(authed(
            StreamChangesRequest {
                keyspace: keyspace.to_string(),
                resume_sequence: resume_from,
                follower_node_id: node_id.to_string(),
            },
            token,
        ))
        .await?
        .into_inner();

    while let Some(proto) = stream.message().await? {
        let event: ChangeEvent = proto.into();
        apply(engine, &event).await?;
    }

    Ok(())
}

async fn apply(engine: &Arc<dyn StorageEngine>, event: &ChangeEvent) -> Result<(), tonic::Status> {
    engine
        .apply_replicated(event)
        .await
        .map_err(|e| tonic::Status::internal(e.to_string()))
}

/// Callback that applies a replicated event (e.g. through
/// `Keyspace::apply_replicated`, which does HLC last-write-wins in
/// multi-leader mode). Boxed future so `kv-core` types don't leak here.
pub type ApplyFn = Arc<
    dyn Fn(ChangeEvent) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
        + Send
        + Sync,
>;

/// Multi-leader peer follower: connects to ONE peer, streams its changes,
/// and applies each through `apply_fn` (LWW). Resumes from sequence 0 on
/// every (re)connect — safe and convergent because LWW apply is idempotent
/// and commutative, so re-delivering already-seen writes is a harmless
/// no-op. Runs forever with reconnect backoff.
pub async fn run_peer_follower(
    peer_addr: String,
    node_id: String,
    region: String,
    keyspace: String,
    apply_fn: ApplyFn,
    auth_token: String,
) {
    let mut attempt = 0usize;
    loop {
        match peer_follow_once(&peer_addr, &node_id, &region, &keyspace, &apply_fn, &auth_token).await {
            Ok(()) => attempt = 0,
            Err(e) => {
                tracing::debug!(%keyspace, peer = %peer_addr, error = %e, "peer stream failed, retrying");
                attempt += 1;
            }
        }
        tokio::time::sleep(RECONNECT_BACKOFF[attempt.min(RECONNECT_BACKOFF.len() - 1)]).await;
    }
}

async fn peer_follow_once(
    peer_addr: &str,
    node_id: &str,
    region: &str,
    keyspace: &str,
    apply_fn: &ApplyFn,
    token: &str,
) -> Result<(), tonic::Status> {
    let channel = Channel::from_shared(peer_addr.to_string())
        .map_err(|e| tonic::Status::invalid_argument(e.to_string()))?
        .connect()
        .await
        .map_err(|e| tonic::Status::unavailable(e.to_string()))?;
    let mut client = ReplicationClient::new(channel);

    client
        .handshake(authed(
            HandshakeRequest {
                node_id: node_id.to_string(),
                region: region.to_string(),
            },
            token,
        ))
        .await?;

    // Always resume from 0: LWW makes re-application idempotent, so we
    // converge regardless of duplicates.
    let mut stream = client
        .stream_changes(authed(
            StreamChangesRequest {
                keyspace: keyspace.to_string(),
                resume_sequence: 0,
                follower_node_id: node_id.to_string(),
            },
            token,
        ))
        .await?
        .into_inner();

    while let Some(proto) = stream.message().await? {
        let event: ChangeEvent = proto.into();
        apply_fn(event).await;
    }
    Ok(())
}
