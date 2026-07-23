use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use falcon_core::NodeError;
use falcon_storage::StorageError;
use serde_json::json;

pub enum ApiError {
    NotFound,
    UnknownKeyspace(String),
    BadRequest(String),
    Internal(String),
}

impl From<NodeError> for ApiError {
    fn from(e: NodeError) -> Self {
        match e {
            NodeError::UnknownKeyspace(name) => ApiError::UnknownKeyspace(name),
            NodeError::Storage(e) => ApiError::Internal(e.to_string()),
            NodeError::Messaging(e) => ApiError::Internal(e.to_string()),
            other => ApiError::Internal(other.to_string()),
        }
    }
}

impl From<StorageError> for ApiError {
    fn from(e: StorageError) -> Self {
        ApiError::Internal(e.to_string())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            ApiError::NotFound => (StatusCode::NOT_FOUND, "key not found".to_string()),
            ApiError::UnknownKeyspace(name) => (
                StatusCode::NOT_FOUND,
                format!("unknown keyspace '{name}'"),
            ),
            ApiError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg),
            ApiError::Internal(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg),
        };
        (status, Json(json!({ "error": message }))).into_response()
    }
}
