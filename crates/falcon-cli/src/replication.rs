use falcon_core::{Node, ReplicationRole, WriteMode};
use falcon_replication::{
    build_log_reader, run_follower, run_peer_follower, KeyspaceReplicationSource,
    ReplicationServerImpl,
};
use std::collections::HashMap;
use std::sync::Arc;
use tonic::transport::Server;

/// Starts whatever replication role this node is configured for, for every
/// keyspace marked `replication = true`. No-ops entirely if replication is
/// disabled, so a standalone node pays nothing for this feature.
pub async fn start(node: Arc<Node>) -> anyhow::Result<()> {
    let config = node.config().clone();
    if !config.replication.enabled {
        return Ok(());
    }

    let replicated_keyspaces: Vec<_> = config
        .keyspaces
        .iter()
        .filter(|ks| ks.replication)
        .map(|ks| ks.name.clone())
        .collect();

    if replicated_keyspaces.is_empty() {
        tracing::warn!("replication is enabled but no keyspace has replication=true");
        return Ok(());
    }

    let multi_leader_keyspaces: Vec<String> = config
        .keyspaces
        .iter()
        .filter(|ks| ks.replication && ks.write_mode == WriteMode::MultiLeader)
        .map(|ks| ks.name.clone())
        .collect();

    // A multi-leader node is BOTH a server (serving its log to peers) AND a
    // client of every peer. Start the server side unconditionally when any
    // multi-leader keyspace exists, then a peer-follower per (keyspace, peer).
    if !multi_leader_keyspaces.is_empty() {
        start_leader(&node, &config, &replicated_keyspaces).await?;
        start_multi_leader_peers(&node, &config, &multi_leader_keyspaces).await;
        // Single-leader keyspaces (if any) still follow their configured
        // leader; but a node is one role overall, so in a mixed config the
        // multi-leader server is already up. Fall through for follower-only
        // single-leader keyspaces handled by role below is skipped here.
        return Ok(());
    }

    match config.replication.role {
        ReplicationRole::Leader => start_leader(&node, &config, &replicated_keyspaces).await,
        ReplicationRole::Follower => {
            start_follower(&node, &config, &replicated_keyspaces).await;
            Ok(())
        }
    }
}

/// For each multi-leader keyspace and each configured peer, spawn a
/// peer-follower that streams the peer's changes and applies them through
/// `Keyspace::apply_replicated` (HLC last-write-wins).
async fn start_multi_leader_peers(
    node: &Arc<Node>,
    config: &falcon_core::Config,
    keyspaces: &[String],
) {
    if config.replication.peers.is_empty() {
        tracing::warn!("multi-leader enabled but no peers configured");
    }
    for name in keyspaces {
        for peer in &config.replication.peers {
            let node = node.clone();
            let ks_name = name.clone();
            let peer_addr = peer.addr.clone();
            let node_id = config.node.id.clone();
            let region = config.node.region.clone();

            // Apply callback routes into the keyspace's LWW path.
            let apply_node = node.clone();
            let apply_ks = ks_name.clone();
            let apply_fn: falcon_replication::ApplyFn = Arc::new(move |event| {
                let node = apply_node.clone();
                let ks = apply_ks.clone();
                Box::pin(async move {
                    if let Some(keyspace) = node.keyspace(&ks) {
                        let _ = keyspace.apply_replicated(&event).await;
                    }
                })
            });

            let token = config.auth.api_key.clone();
            tokio::spawn(async move {
                let _ = &node;
                run_peer_follower(peer_addr, node_id, region, ks_name, apply_fn, token).await;
            });
        }
    }
}

async fn start_leader(
    node: &Arc<Node>,
    config: &falcon_core::Config,
    keyspaces: &[String],
) -> anyhow::Result<()> {
    let mut sources = HashMap::new();
    for name in keyspaces {
        let ks = node
            .keyspace(name)
            .expect("keyspace listed in config must exist on Node");
        let log_reader = build_log_reader(ks.engine())
            .expect("replicated keyspace must be warm or cold tier (validated at config load)");
        let events = ks
            .events()
            .cloned()
            .expect("replicated keyspace must have an event bus (enabled at Node::build)");
        sources.insert(
            name.clone(),
            KeyspaceReplicationSource { log_reader, events },
        );
    }

    let server_impl = ReplicationServerImpl::new(config.node.id.clone(), sources)
        .with_auth_token(config.auth.api_key.clone());
    let bind: std::net::SocketAddr = config.replication.grpc_bind.parse()?;
    tracing::info!(%bind, "replication gRPC server (leader) listening");

    tokio::spawn(async move {
        if let Err(e) = Server::builder()
            .add_service(falcon_proto::replication::replication_server::ReplicationServer::new(
                server_impl,
            ))
            .serve(bind)
            .await
        {
            tracing::error!(error = %e, "replication gRPC server exited");
        }
    });

    Ok(())
}

async fn start_follower(node: &Arc<Node>, config: &falcon_core::Config, keyspaces: &[String]) {
    let leader_addr = match &config.replication.leader_addr {
        Some(addr) => addr.clone(),
        None => {
            tracing::error!("role=follower but no leader_addr configured; skipping replication");
            return;
        }
    };

    for name in keyspaces {
        let ks = node
            .keyspace(name)
            .expect("keyspace listed in config must exist on Node");
        let engine = ks.engine().clone();
        let leader_addr = leader_addr.clone();
        let node_id = config.node.id.clone();
        let region = config.node.region.clone();
        let keyspace = name.clone();
        let token = config.auth.api_key.clone();

        tokio::spawn(async move {
            run_follower(leader_addr, node_id, region, keyspace, engine, token).await;
        });
    }
}
