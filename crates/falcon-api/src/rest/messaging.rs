//! REST surface for pub/sub topics and work queues, so the CLI, the dashboard
//! UI, and plain HTTP clients can drive them without the binary wire protocol.
//!
//! - `POST /topics/{topic}/publish`        — body = payload → {offset}
//! - `POST /queues/{queue}/push`           — body = payload → {offset}
//! - `POST /queues/{queue}/pop?group=G`    — deliver+start-ack-timer → {offset,payload} or 204
//! - `POST /queues/{queue}/ack?group=G&offset=O` — confirm delivery

use crate::rest::error::ApiError;
use crate::state::AppState;
use axum::extract::{Path, Query, State};
use axum::Json;
use serde::Serialize;
use std::collections::HashMap;

#[derive(Serialize)]
pub struct OffsetResponse {
    pub offset: u64,
}

#[derive(Serialize)]
pub struct PopResponse {
    pub offset: u64,
    pub payload: String,
}

pub async fn publish(
    State(state): State<AppState>,
    Path(topic): Path<String>,
    body: axum::body::Bytes,
) -> Result<Json<OffsetResponse>, ApiError> {
    let t = state
        .node
        .messaging()
        .topic(&topic)
        .ok_or_else(|| ApiError::UnknownKeyspace(format!("topic '{topic}'")))?;
    let offset = t
        .publish(body.to_vec())
        .map_err(|e| ApiError::Internal(e.to_string()))?;
    Ok(Json(OffsetResponse { offset }))
}

pub async fn push(
    State(state): State<AppState>,
    Path(queue): Path<String>,
    body: axum::body::Bytes,
) -> Result<Json<OffsetResponse>, ApiError> {
    let q = state
        .node
        .messaging()
        .queue(&queue)
        .ok_or_else(|| ApiError::UnknownKeyspace(format!("queue '{queue}'")))?;
    let offset = q
        .push(&body)
        .map_err(|e| ApiError::Internal(e.to_string()))?;
    Ok(Json(OffsetResponse { offset }))
}

pub async fn pop(
    State(state): State<AppState>,
    Path(queue): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<axum::response::Response, ApiError> {
    use axum::response::IntoResponse;
    let group = params.get("group").cloned().unwrap_or_else(|| "default".into());
    let q = state
        .node
        .messaging()
        .queue(&queue)
        .ok_or_else(|| ApiError::UnknownKeyspace(format!("queue '{queue}'")))?;
    match q.pop(&group).map_err(|e| ApiError::Internal(e.to_string()))? {
        Some(msg) => Ok(Json(PopResponse {
            offset: msg.offset,
            payload: String::from_utf8_lossy(&msg.payload).to_string(),
        })
        .into_response()),
        None => Ok(axum::http::StatusCode::NO_CONTENT.into_response()),
    }
}

pub async fn ack(
    State(state): State<AppState>,
    Path(queue): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<axum::http::StatusCode, ApiError> {
    let group = params.get("group").cloned().unwrap_or_else(|| "default".into());
    let offset: u64 = params
        .get("offset")
        .and_then(|o| o.parse().ok())
        .ok_or_else(|| ApiError::BadRequest("missing/invalid ?offset=".into()))?;
    let q = state
        .node
        .messaging()
        .queue(&queue)
        .ok_or_else(|| ApiError::UnknownKeyspace(format!("queue '{queue}'")))?;
    q.ack(&group, offset);
    Ok(axum::http::StatusCode::OK)
}
