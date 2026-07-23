//! REST surface for Falcon Event Stream. Exposes the append / poll /
//! commit lifecycle so producers and consumer groups can drive a partitioned,
//! durable, replayable stream over plain HTTP:
//!
//! - `POST /streams/{stream}/records?key=<k>`  — append a record; the body is
//!   the payload, `?key=` routes it to a partition (same key ⇒ same partition
//!   ⇒ ordered). Returns the assigned `{partition, offset}`.
//! - `GET  /streams/{stream}/poll?group=<g>&partition=<p>` — fetch records for
//!   a consumer group after its committed offset (does NOT commit).
//! - `POST /streams/{stream}/commit?group=<g>&partition=<p>&offset=<o>` —
//!   durably advance the group's committed offset (at-least-once boundary).
//! - `GET  /streams/{stream}` — stream metadata (partition count).

use crate::rest::error::ApiError;
use crate::state::AppState;
use axum::extract::{Path, Query, State};
use axum::Json;
use serde::Serialize;
use std::collections::HashMap;

#[derive(Serialize)]
pub struct AppendResponse {
    pub stream: String,
    pub partition: usize,
    pub offset: u64,
}

#[derive(Serialize)]
pub struct StreamRecordJson {
    pub partition: usize,
    pub offset: u64,
    pub payload: String,
}

#[derive(Serialize)]
pub struct PollResponse {
    pub stream: String,
    pub group: String,
    pub partition: usize,
    pub records: Vec<StreamRecordJson>,
}

#[derive(Serialize)]
pub struct CommitResponse {
    pub stream: String,
    pub group: String,
    pub partition: usize,
    pub committed: u64,
}

#[derive(Serialize)]
pub struct StreamInfo {
    pub name: String,
    pub partitions: usize,
}

fn stream<'a>(
    state: &'a AppState,
    name: &str,
) -> Result<&'a std::sync::Arc<falcon_messaging::Stream>, ApiError> {
    state
        .node
        .messaging()
        .stream(name)
        .ok_or_else(|| ApiError::UnknownKeyspace(format!("stream '{name}'")))
}

/// Append a record. `?key=` chooses the partition (defaults to an empty key,
/// which is stable — all keyless records share one partition).
pub async fn append(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    body: axum::body::Bytes,
) -> Result<Json<AppendResponse>, ApiError> {
    let s = stream(&state, &name)?.clone();
    let key = params.get("key").map(|k| k.as_bytes().to_vec()).unwrap_or_default();
    let payload = body.to_vec();
    // The append fsyncs a durable log (blocking); run it off the async worker.
    let (partition, offset) = tokio::task::spawn_blocking(move || s.append_keyed(&key, payload))
        .await
        .map_err(|e| ApiError::Internal(e.to_string()))?
        .map_err(|e| ApiError::Internal(e.to_string()))?;
    Ok(Json(AppendResponse {
        stream: name,
        partition,
        offset,
    }))
}

/// Poll a partition for a consumer group: records after its committed offset.
pub async fn poll(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<PollResponse>, ApiError> {
    let s = stream(&state, &name)?;
    let group = params
        .get("group")
        .cloned()
        .ok_or_else(|| ApiError::BadRequest("missing ?group=".into()))?;
    let partition = parse_partition(&params)?;
    let records = s
        .poll(&group, partition)
        .map_err(|e| ApiError::Internal(e.to_string()))?
        .into_iter()
        .map(|r| StreamRecordJson {
            partition: r.partition,
            offset: r.offset,
            payload: String::from_utf8_lossy(&r.payload).to_string(),
        })
        .collect();
    Ok(Json(PollResponse {
        stream: name,
        group,
        partition,
        records,
    }))
}

/// Durably commit a consumer group's progress on a partition.
pub async fn commit(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<CommitResponse>, ApiError> {
    let s = stream(&state, &name)?;
    let group = params
        .get("group")
        .cloned()
        .ok_or_else(|| ApiError::BadRequest("missing ?group=".into()))?;
    let partition = parse_partition(&params)?;
    let offset: u64 = params
        .get("offset")
        .and_then(|o| o.parse().ok())
        .ok_or_else(|| ApiError::BadRequest("missing/invalid ?offset=".into()))?;
    s.commit(&group, partition, offset)
        .map_err(|e| ApiError::Internal(e.to_string()))?;
    Ok(Json(CommitResponse {
        stream: name,
        group,
        partition,
        committed: offset,
    }))
}

/// Stream metadata.
pub async fn info(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<StreamInfo>, ApiError> {
    let s = stream(&state, &name)?;
    Ok(Json(StreamInfo {
        name,
        partitions: s.partition_count(),
    }))
}

fn parse_partition(params: &HashMap<String, String>) -> Result<usize, ApiError> {
    params
        .get("partition")
        .and_then(|p| p.parse().ok())
        .ok_or_else(|| ApiError::BadRequest("missing/invalid ?partition=".into()))
}
