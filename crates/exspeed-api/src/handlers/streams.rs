use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::IntoResponse;
use axum::Json;
use bytes::Bytes;
use serde::Deserialize;
use serde_json::json;

use exspeed_broker::broker_append::AppendResult;
use exspeed_common::StreamName;
use exspeed_storage::file::stream_config::StreamConfig;
use exspeed_streams::{Record, StorageError, StorageEngine};

use crate::state::AppState;

#[derive(Deserialize)]
pub struct CreateStreamRequest {
    pub name: String,
    #[serde(default)]
    pub max_age_secs: u64,
    #[serde(default)]
    pub max_bytes: u64,
    pub dedup_window_secs: Option<u64>,
    pub dedup_max_entries: Option<u64>,
}

/// Build a `StreamInfo`-shaped JSON value from a name + config.
fn stream_info_json(
    name: &str,
    config: &StreamConfig,
    storage_bytes: u64,
    head_offset: u64,
) -> serde_json::Value {
    json!({
        "name": name,
        "storage_bytes": storage_bytes,
        "head_offset": head_offset,
        "max_age_secs": config.max_age_secs,
        "max_bytes": config.max_bytes,
        "dedup_window_secs": config.dedup_window_secs,
        "dedup_max_entries": config.dedup_max_entries,
    })
}

pub async fn list_streams(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let names = state.storage.list_streams();
    let mut streams = Vec::new();

    for name in &names {
        let storage_bytes = state.storage.stream_storage_bytes(name).unwrap_or(0);
        let head_offset = state.storage.stream_head_offset(name).unwrap_or(0);
        let stream_dir = state.storage.data_dir().join("streams").join(name);
        let config = StreamConfig::load(&stream_dir).unwrap_or_default();

        streams.push(stream_info_json(name, &config, storage_bytes, head_offset));
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

    // Build a full config with defaults applied for any missing dedup fields.
    let cfg = StreamConfig::from_request(
        body.max_age_secs,
        body.max_bytes,
        body.dedup_window_secs.unwrap_or(0),
        body.dedup_max_entries.unwrap_or(0),
    );

    // Validate before touching storage.
    if let Err(msg) = StreamConfig::validate(
        cfg.max_age_secs,
        cfg.max_bytes,
        cfg.dedup_window_secs,
        cfg.dedup_max_entries,
    ) {
        return (StatusCode::BAD_REQUEST, Json(json!({"error": msg})));
    }

    match state
        .storage
        .create_stream(&stream_name, body.max_age_secs, body.max_bytes)
        .await
    {
        Ok(()) => {
            // Overwrite the config written by create_stream_sync with the full
            // config including dedup fields.
            let stream_dir = state
                .storage
                .data_dir()
                .join("streams")
                .join(stream_name.as_str());
            if let Err(e) = cfg.save(&stream_dir) {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": format!("failed to save stream config: {e}")})),
                );
            }
            state
                .broker
                .broker_append
                .configure_stream(&stream_name, cfg.dedup_window_secs, cfg.dedup_max_entries)
                .await;
            (
                StatusCode::CREATED,
                Json(json!({"name": body.name, "status": "created"})),
            )
        }
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
        Json(stream_info_json(&name, &config, storage_bytes, head_offset)),
    )
}

// ---------------------------------------------------------------------------
// PATCH /api/v1/streams/:name
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct UpdateStreamRequest {
    pub max_age_secs: Option<u64>,
    pub max_bytes: Option<u64>,
    pub dedup_window_secs: Option<u64>,
    pub dedup_max_entries: Option<u64>,
}

pub async fn patch_stream(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    Json(req): Json<UpdateStreamRequest>,
) -> impl IntoResponse {
    let stream_name = match StreamName::try_from(name.as_str()) {
        Ok(n) => n,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": e.to_string()})),
            );
        }
    };

    let stream_dir = state.storage.data_dir().join("streams").join(&name);

    // 404 if the stream doesn't exist.
    if state.storage.stream_storage_bytes(&name).is_none() {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"error": format!("stream '{}' not found", name)})),
        );
    }

    let mut cfg = match StreamConfig::load(&stream_dir) {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("failed to load stream config: {e}")})),
            );
        }
    };

    // Apply partial updates.
    if let Some(v) = req.max_age_secs {
        cfg.max_age_secs = v;
    }
    if let Some(v) = req.max_bytes {
        cfg.max_bytes = v;
    }
    if let Some(v) = req.dedup_window_secs {
        cfg.dedup_window_secs = v;
    }
    if let Some(v) = req.dedup_max_entries {
        cfg.dedup_max_entries = v;
    }

    // Validate the merged config.
    if let Err(msg) = StreamConfig::validate(
        cfg.max_age_secs,
        cfg.max_bytes,
        cfg.dedup_window_secs,
        cfg.dedup_max_entries,
    ) {
        return (StatusCode::BAD_REQUEST, Json(json!({"error": msg})));
    }

    // Guard against shrinking dedup_max_entries below the current live count.
    if let Some(new_cap) = req.dedup_max_entries {
        let current = state
            .broker
            .broker_append
            .entry_count(&stream_name)
            .await;
        if (new_cap as usize) < current {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": format!(
                        "cannot shrink dedup_max_entries below current entry count ({current})"
                    )
                })),
            );
        }
    }

    // Persist.
    if let Err(e) = cfg.save(&stream_dir) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("failed to save stream config: {e}")})),
        );
    }

    // Hot-reconfigure the in-memory dedup map.
    state
        .broker
        .broker_append
        .configure_stream(&stream_name, cfg.dedup_window_secs, cfg.dedup_max_entries)
        .await;

    let storage_bytes = state.storage.stream_storage_bytes(&name).unwrap_or(0);
    let head_offset = state.storage.stream_head_offset(&name).unwrap_or(0);

    (
        StatusCode::OK,
        Json(stream_info_json(&name, &cfg, storage_bytes, head_offset)),
    )
}

// ---------------------------------------------------------------------------
// Publish
// ---------------------------------------------------------------------------

#[derive(serde::Deserialize)]
pub struct PublishBody {
    #[serde(default)]
    pub subject: String,
    #[serde(default)]
    pub key: Option<String>,
    pub data: serde_json::Value,
    #[serde(default)]
    pub msg_id: Option<String>,
}

pub async fn publish_to_stream(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    Json(body): Json<PublishBody>,
) -> axum::response::Response {
    let stream_name = match StreamName::try_from(name.as_str()) {
        Ok(n) => n,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": e.to_string()})),
            )
                .into_response();
        }
    };

    let subject = if body.subject.is_empty() {
        name.clone()
    } else {
        body.subject
    };

    let value = match serde_json::to_vec(&body.data) {
        Ok(bytes) => Bytes::from(bytes),
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": format!("failed to serialize data: {}", e)})),
            )
                .into_response();
        }
    };

    let key = body.key.map(|k| Bytes::from(k.into_bytes()));

    // Translate msg_id → x-idempotency-key header.
    let mut headers = vec![];
    if let Some(ref id) = body.msg_id {
        headers.push(("x-idempotency-key".to_string(), id.clone()));
    }

    let record = Record {
        key,
        value,
        subject,
        headers,
    };

    let start = std::time::Instant::now();
    match state.broker.broker_append.append(&stream_name, &record).await {
        Ok(AppendResult::Written(offset)) => {
            let elapsed_secs = start.elapsed().as_secs_f64();
            state
                .metrics
                .record_publish_latency(stream_name.as_str(), elapsed_secs);
            state.metrics.record_publish(stream_name.as_str());
            (
                StatusCode::CREATED,
                Json(json!({"offset": offset.0, "duplicate": false})),
            )
                .into_response()
        }
        Ok(AppendResult::Duplicate(offset)) => {
            let elapsed_secs = start.elapsed().as_secs_f64();
            state
                .metrics
                .record_publish_latency(stream_name.as_str(), elapsed_secs);
            state.metrics.record_publish(stream_name.as_str());
            (
                StatusCode::OK,
                Json(json!({"offset": offset.0, "duplicate": true})),
            )
                .into_response()
        }
        Err(StorageError::StreamNotFound(_)) => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": format!("stream '{}' not found", name)})),
        )
            .into_response(),
        Err(StorageError::KeyCollision { stored_offset }) => (
            StatusCode::CONFLICT,
            Json(json!({
                "error": "msg_id already used for a different message body",
                "stored_offset": stored_offset,
            })),
        )
            .into_response(),
        Err(StorageError::DedupMapFull { retry_after_secs }) => {
            let mut resp_headers = HeaderMap::new();
            if let Ok(val) = HeaderValue::from_str(&retry_after_secs.to_string()) {
                resp_headers.insert(axum::http::header::RETRY_AFTER, val);
            }
            (
                StatusCode::SERVICE_UNAVAILABLE,
                resp_headers,
                Json(json!({"error": "dedup map full, retry later", "retry_after_secs": retry_after_secs})),
            )
                .into_response()
        }
        Err(e) => {
            let kind = match &e {
                StorageError::Io(io_err)
                    if exspeed_storage::file::io_errors::is_storage_full(io_err) =>
                {
                    "storage_full"
                }
                _ => "other",
            };
            state
                .metrics
                .record_storage_write_error(stream_name.as_str(), kind);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": e.to_string()})),
            )
                .into_response()
        }
    }
}
