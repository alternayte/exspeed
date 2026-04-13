use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde::Deserialize;
use serde_json::json;

use exspeed_common::StreamName;
use exspeed_storage::file::stream_config::StreamConfig;
use exspeed_streams::StorageEngine;

use crate::state::AppState;

#[derive(Deserialize)]
pub struct CreateStreamRequest {
    pub name: String,
    #[serde(default)]
    pub max_age_secs: u64,
    #[serde(default)]
    pub max_bytes: u64,
}

pub async fn list_streams(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let names = state.storage.list_streams();
    let mut streams = Vec::new();

    for name in &names {
        let storage_bytes = state.storage.stream_storage_bytes(name).unwrap_or(0);
        let head_offset = state.storage.stream_head_offset(name).unwrap_or(0);
        let stream_dir = state.storage.data_dir().join("streams").join(name);
        let config = StreamConfig::load(&stream_dir).unwrap_or_default();

        streams.push(json!({
            "name": name,
            "storage_bytes": storage_bytes,
            "head_offset": head_offset,
            "max_age_secs": config.max_age_secs,
            "max_bytes": config.max_bytes,
        }));
    }

    (StatusCode::OK, Json(json!(streams)))
}

pub async fn create_stream(
    State(state): State<Arc<AppState>>,
    Json(body): Json<CreateStreamRequest>,
) -> impl IntoResponse {
    let stream_name = match StreamName::try_from(body.name.as_str()) {
        Ok(name) => name,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": e.to_string()})),
            );
        }
    };

    match state
        .storage
        .create_stream(&stream_name, body.max_age_secs, body.max_bytes)
    {
        Ok(()) => (
            StatusCode::CREATED,
            Json(json!({"name": body.name, "status": "created"})),
        ),
        Err(exspeed_streams::StorageError::StreamAlreadyExists(_)) => (
            StatusCode::CONFLICT,
            Json(json!({"error": format!("stream '{}' already exists", body.name)})),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        ),
    }
}

pub async fn get_stream(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let storage_bytes = match state.storage.stream_storage_bytes(&name) {
        Some(b) => b,
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": format!("stream '{}' not found", name)})),
            );
        }
    };

    let head_offset = state.storage.stream_head_offset(&name).unwrap_or(0);
    let stream_dir = state.storage.data_dir().join("streams").join(&name);
    let config = StreamConfig::load(&stream_dir).unwrap_or_default();

    (
        StatusCode::OK,
        Json(json!({
            "name": name,
            "storage_bytes": storage_bytes,
            "head_offset": head_offset,
            "max_age_secs": config.max_age_secs,
            "max_bytes": config.max_bytes,
        })),
    )
}
