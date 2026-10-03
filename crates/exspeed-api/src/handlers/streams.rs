use std::sync::Arc;

use axum::extract::{Extension, Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use bytes::Bytes;
use serde::Deserialize;
use serde_json::json;

use exspeed_broker::broker_append::AppendResult;
use exspeed_common::auth::Identity;
use exspeed_common::StreamName;
use exspeed_storage::file::stream_config::{StreamConfig, StreamConfigFile};
use exspeed_streams::{Record, StorageError};

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
    /// Keep only the latest record per key (plus unkeyed records).
    #[serde(default)]
    pub compaction: bool,
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
        "compaction": config.compaction,
        "internal": name.starts_with(exspeed_common::INTERNAL_STREAM_PREFIX),
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
    identity: Option<Extension<Arc<Identity>>>,
    Json(body): Json<CreateStreamRequest>,
) -> Response {
    let stream_name = match StreamName::try_from(body.name.as_str()) {
        Ok(name) => name,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": e.to_string()})),
            )
                .into_response();
        }
    };

    // Scoped-admin gate: the proposed stream name is the target. When auth
    // is globally off (no credential_store), `identity` is None and we
    // allow — the middleware already skipped the bearer check.
    if let Some(Extension(id)) = identity {
        if let Some(resp) = super::require_scoped_admin(&id, &stream_name) {
            return resp;
        }
    }

    // Build a full config with defaults applied for any missing dedup fields.
    let mut cfg = StreamConfig::from_request(
        body.max_age_secs,
        body.max_bytes,
        body.dedup_window_secs.unwrap_or(0),
        body.dedup_max_entries.unwrap_or(0),
    );
    cfg.compaction = body.compaction;

    // Validate before touching storage.
    if let Err(msg) = StreamConfig::validate(
        cfg.max_age_secs,
        cfg.max_bytes,
        cfg.dedup_window_secs,
        cfg.dedup_max_entries,
    ) {
        return (StatusCode::BAD_REQUEST, Json(json!({"error": msg}))).into_response();
    }

    match state.broker.log.create_stream(&stream_name, &cfg).await {
        Ok(()) => (
            StatusCode::CREATED,
            Json(json!({"name": body.name, "status": "created"})),
        )
            .into_response(),
        Err(e) => log_error_response(&state, &stream_name, e),
    }
}

/// Map a write-path error to an HTTP response.
pub(crate) fn log_error_response(
    state: &AppState,
    stream: &StreamName,
    e: exspeed_broker::log::LogError,
) -> Response {
    use exspeed_broker::log::LogError;
    match e {
        LogError::NotLeader | LogError::DedupNotReady => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
        LogError::InvalidRecord(_) | LogError::InvalidConfig(_) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
        LogError::Storage(StorageError::StreamNotFound(_)) => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": format!("stream '{stream}' not found")})),
        )
            .into_response(),
        LogError::Storage(StorageError::StreamAlreadyExists(_)) => (
            StatusCode::CONFLICT,
            Json(json!({"error": format!("stream '{stream}' already exists")})),
        )
            .into_response(),
        LogError::Storage(StorageError::KeyCollision { stored_offset }) => (
            StatusCode::CONFLICT,
            Json(json!({
                "error": "msg_id already used for a different message body",
                "stored_offset": stored_offset,
            })),
        )
            .into_response(),
        LogError::Storage(StorageError::DedupMapFull { retry_after_secs }) => {
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
        LogError::Storage(e) => {
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
                .record_storage_write_error(stream.as_str(), kind);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": e.to_string()})),
            )
                .into_response()
        }
    }
}

pub async fn get_stream(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    identity: Option<Extension<Arc<Identity>>>,
) -> Response {
    // Validate the path param into a StreamName first so the authz check has
    // something to glob-match against. An invalid stream name can't match any
    // permission, so this also doubles as a 400 guard.
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

    if let Some(Extension(id)) = identity {
        if let Some(resp) = super::require_scoped_admin(&id, &stream_name) {
            return resp;
        }
    }

    let storage_bytes = match state.storage.stream_storage_bytes(&name) {
        Some(b) => b,
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": format!("stream '{}' not found", name)})),
            )
                .into_response();
        }
    };

    let head_offset = state.storage.stream_head_offset(&name).unwrap_or(0);
    let stream_dir = state.storage.data_dir().join("streams").join(&name);
    let config = StreamConfig::load(&stream_dir).unwrap_or_default();

    (
        StatusCode::OK,
        Json(stream_info_json(&name, &config, storage_bytes, head_offset)),
    )
        .into_response()
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
    identity: Option<Extension<Arc<Identity>>>,
    Json(req): Json<UpdateStreamRequest>,
) -> Response {
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

    if let Some(Extension(id)) = identity {
        if let Some(resp) = super::require_scoped_admin(&id, &stream_name) {
            return resp;
        }
    }

    let stream_dir = state.storage.data_dir().join("streams").join(&name);

    // 404 if the stream doesn't exist.
    if state.storage.stream_storage_bytes(&name).is_none() {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"error": format!("stream '{}' not found", name)})),
        )
            .into_response();
    }

    let mut cfg = match StreamConfig::load(&stream_dir) {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("failed to load stream config: {e}")})),
            )
                .into_response();
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
        return (StatusCode::BAD_REQUEST, Json(json!({"error": msg}))).into_response();
    }

    // Guard against shrinking dedup_max_entries below the current live count.
    if let Some(new_cap) = req.dedup_max_entries {
        let current = state.broker.broker_append.entry_count(&stream_name).await;
        if (new_cap as usize) < current {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": format!(
                        "cannot shrink dedup_max_entries below current entry count ({current})"
                    )
                })),
            )
                .into_response();
        }
    }

    if let Err(e) = state
        .broker
        .log
        .update_stream_config(&stream_name, &cfg)
        .await
    {
        return log_error_response(&state, &stream_name, e);
    }

    let storage_bytes = state.storage.stream_storage_bytes(&name).unwrap_or(0);
    let head_offset = state.storage.stream_head_offset(&name).unwrap_or(0);

    (
        StatusCode::OK,
        Json(stream_info_json(&name, &cfg, storage_bytes, head_offset)),
    )
        .into_response()
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
    identity: Option<Extension<Arc<Identity>>>,
    headers_in: HeaderMap,
    Json(body): Json<PublishBody>,
) -> Response {
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

    // HTTP publish is an admin surface (not Action::Publish): it lives under
    // the admin-bearer router, and scope is enforced per-stream.
    if let Some(Extension(id)) = identity {
        if let Some(resp) = super::require_scoped_admin(&id, &stream_name) {
            return resp;
        }
    }

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

    // Read x-idempotency-key from the HTTP request header (if present).
    let header_msg_id = headers_in
        .get("x-idempotency-key")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    // Prefer the explicit body field; log DEBUG when both are present but differ.
    let effective_msg_id = match (&body.msg_id, &header_msg_id) {
        (Some(field), Some(header)) if field != header => {
            tracing::debug!(
                stream = %name,
                "publish has both explicit msg_id and x-idempotency-key header; using explicit field"
            );
            Some(field.clone())
        }
        (Some(field), _) => Some(field.clone()),
        (None, Some(header)) => Some(header.clone()),
        (None, None) => None,
    };

    // Translate effective_msg_id → x-idempotency-key header.
    let mut headers = vec![];
    if let Some(ref id) = effective_msg_id {
        headers.push(("x-idempotency-key".to_string(), id.clone()));
    }

    let record = Record {
        key,
        value,
        subject,
        headers,
        timestamp_ns: None,
    };

    let start = std::time::Instant::now();
    match state.broker.log.append(&stream_name, record).await {
        Ok(result) => {
            state
                .metrics
                .record_publish_latency(stream_name.as_str(), start.elapsed().as_secs_f64());
            match result {
                AppendResult::Written(offset, _) => (
                    StatusCode::CREATED,
                    Json(json!({"offset": offset.0, "duplicate": false})),
                )
                    .into_response(),
                AppendResult::Duplicate(offset) => (
                    StatusCode::OK,
                    Json(json!({"offset": offset.0, "duplicate": true})),
                )
                    .into_response(),
            }
        }
        Err(e) => log_error_response(&state, &stream_name, e),
    }
}

// ---------------------------------------------------------------------------
// DELETE /api/v1/streams/:name
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct DeleteStreamQuery {
    #[serde(default)]
    pub force: bool,
}

pub async fn delete_stream(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    Query(q): Query<DeleteStreamQuery>,
    identity: Option<Extension<Arc<Identity>>>,
) -> Response {
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

    if let Some(Extension(id)) = identity {
        if let Some(resp) = super::require_scoped_admin(&id, &stream_name) {
            return resp;
        }
    }

    if state.storage.stream_storage_bytes(&name).is_none() {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"error": format!("stream '{}' not found", name)})),
        )
            .into_response();
    }

    let blockers = collect_blockers(&state, &name).await;

    if !q.force && !blockers.is_empty() {
        return (
            StatusCode::CONFLICT,
            Json(json!({
                "error": "stream has active references; retry with ?force=true to cascade",
                "blockers": blockers.to_json(),
            })),
        )
            .into_response();
    }

    if q.force {
        let cascaded_connectors = blockers.connectors.clone();
        for name in &cascaded_connectors {
            if let Err(e) = state.connector_manager.delete(name).await {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": format!("cascade: failed to delete connector '{name}': {e}")})),
                )
                    .into_response();
            }
        }

        let cascaded_queries = blockers.queries.clone();
        for id in &cascaded_queries {
            if let Err(e) = state.exql.remove_query(id) {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": format!("cascade: failed to remove query '{id}': {e}")})),
                )
                    .into_response();
            }
        }

        let cascaded_consumers = blockers.consumers.clone();
        for cname in &cascaded_consumers {
            if let Err(e) = state.broker.consumers.delete(cname).await {
                tracing::warn!(
                    consumer = %cname,
                    error = %e,
                    "cascade: consumer delete returned error, continuing"
                );
            }
        }

        let dropped_subs = blockers.subscriptions;

        match state.broker.delete_stream(&stream_name).await {
            Ok(()) => {
                return (
                    StatusCode::OK,
                    Json(json!({
                        "deleted": name,
                        "cascaded": {
                            "consumers": cascaded_consumers,
                            "connectors": cascaded_connectors,
                            "queries": cascaded_queries,
                            "subscriptions_dropped": dropped_subs,
                        }
                    })),
                )
                    .into_response();
            }
            Err(e) => return log_error_response(&state, &stream_name, e),
        }
    }

    match state.broker.delete_stream(&stream_name).await {
        Ok(()) => (
            StatusCode::OK,
            Json(json!({
                "deleted": name,
                "cascaded": {"consumers": [], "connectors": [], "queries": [], "subscriptions_dropped": 0}
            })),
        )
            .into_response(),
        Err(e) => log_error_response(&state, &stream_name, e),
    }
}

#[derive(Default)]
struct Blockers {
    consumers: Vec<String>,
    connectors: Vec<String>,
    queries: Vec<String>,
    subscriptions: usize,
}

impl Blockers {
    fn is_empty(&self) -> bool {
        self.consumers.is_empty()
            && self.connectors.is_empty()
            && self.queries.is_empty()
            && self.subscriptions == 0
    }

    fn to_json(&self) -> serde_json::Value {
        json!({
            "consumers": self.consumers,
            "connectors": self.connectors,
            "queries": self.queries,
            "subscriptions": self.subscriptions,
        })
    }
}

async fn collect_blockers(state: &Arc<AppState>, stream: &str) -> Blockers {
    let mut b = Blockers::default();

    if let Ok(list) = state.broker.consumers.list(Some(stream)).await {
        for info in list {
            b.subscriptions += info.subscribers as usize;
            b.consumers.push(info.spec.name);
        }
    }

    for info in state.connector_manager.list().await {
        if info.stream == stream {
            b.connectors.push(info.name);
        }
    }

    for q in state.exql.list_queries() {
        if q.target_stream == stream {
            b.queries.push(q.id);
        }
    }

    b
}

#[derive(Deserialize)]
pub struct ReadParams {
    /// First offset to return (default: the earliest retained record).
    pub from: Option<u64>,
    /// Maximum records to return (default 100, max 1000).
    pub limit: Option<usize>,
    /// Subject filter (`orders.*`, `orders.>`); empty matches all.
    #[serde(default)]
    pub filter: String,
}

/// Render a payload for JSON: embedded as JSON when it parses, as a string
/// when it is UTF-8, and base64 otherwise.
fn payload_json(b: &[u8]) -> (serde_json::Value, &'static str) {
    if let Ok(v) = serde_json::from_slice::<serde_json::Value>(b) {
        return (v, "json");
    }
    match std::str::from_utf8(b) {
        Ok(s) => (serde_json::Value::String(s.to_string()), "utf8"),
        Err(_) => {
            use base64::Engine;
            (
                serde_json::Value::String(base64::engine::general_purpose::STANDARD.encode(b)),
                "base64",
            )
        }
    }
}

/// `GET /api/v1/streams/{name}/records?from=&limit=&filter=` — browse a
/// stream without creating a consumer. Returns `next_offset` to continue.
pub async fn read_records(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    Query(params): Query<ReadParams>,
    identity: Option<Extension<Arc<Identity>>>,
) -> Response {
    let stream_name = match StreamName::try_from(name.as_str()) {
        Ok(n) => n,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": format!("invalid stream name: {e}")})),
            )
                .into_response()
        }
    };
    if let Some(Extension(id)) = identity.as_ref() {
        if let Some(resp) = super::require_scoped_admin(id, &stream_name) {
            return resp;
        }
    }
    let filter = match exspeed_common::SubjectFilter::parse(&params.filter) {
        Ok(f) => f,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, Json(json!({"error": e}))).into_response();
        }
    };
    let limit = params.limit.unwrap_or(100).clamp(1, 1000);
    let storage = &state.broker.storage;
    let (earliest, _) = match storage.stream_bounds(&stream_name).await {
        Ok(b) => b,
        Err(e) => return log_error_response(&state, &stream_name, e.into()),
    };
    let mut cursor = exspeed_common::Offset(params.from.unwrap_or(earliest.0).max(earliest.0));
    let mut out = Vec::new();
    let mut high_watermark = cursor;
    // Bounded scan so a selective filter can't turn one request into a
    // full-stream read.
    'scan: for _ in 0..16 {
        let batch = match storage
            .read_batch(
                &stream_name,
                cursor,
                exspeed_streams::ReadLimits {
                    max_records: 1000,
                    max_bytes: 4 * 1024 * 1024,
                },
            )
            .await
        {
            Ok(b) => b,
            Err(e) => return log_error_response(&state, &stream_name, e.into()),
        };
        high_watermark = batch.high_watermark;
        cursor = batch.next_offset;
        if batch.records.is_empty() {
            break;
        }
        for r in &batch.records {
            if !filter.matches(&r.subject) {
                continue;
            }
            let (value, encoding) = payload_json(&r.value);
            out.push(json!({
                "offset": r.offset.0,
                "timestamp_ms": r.timestamp / 1_000_000,
                "subject": r.subject,
                "key": r.key.as_ref().map(|k| String::from_utf8_lossy(k).into_owned()),
                "value": value,
                "encoding": encoding,
                "headers": r.headers.iter().map(|(k, v)| json!([k, v])).collect::<Vec<_>>(),
            }));
            if out.len() >= limit {
                cursor = exspeed_common::Offset(r.offset.0 + 1);
                break 'scan;
            }
        }
        if cursor >= high_watermark {
            break;
        }
    }
    (
        StatusCode::OK,
        Json(json!({
            "stream": name,
            "records": out,
            "next_offset": cursor.0,
            "high_watermark": high_watermark.0,
        })),
    )
        .into_response()
}
