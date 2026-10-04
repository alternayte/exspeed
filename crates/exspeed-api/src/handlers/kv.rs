//! Key-value buckets over HTTP. A bucket `B` is the stream `KV_B`; reading
//! needs `subscribe` (or `admin`) on it, writing `publish`, creating a bucket
//! `admin`.

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Extension, Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use base64::Engine;
use exspeed_broker::kv::{bucket_stream, BucketConfig, KvEntry, KvError};
use exspeed_common::auth::{Action, Identity};
use serde::{Deserialize, Serialize};
use serde_json::json;
use utoipa::{IntoParams, ToSchema};

use crate::openapi::ErrorBody;
use crate::state::AppState;

/// Body of `POST /api/v1/kv`.
#[derive(Deserialize, ToSchema)]
pub struct CreateBucketRequest {
    pub bucket: String,
    /// Values kept per key, 1..=64 (default 1).
    #[serde(default)]
    pub history: u64,
    /// Keys expire this long after their last put; 0 or absent = never.
    #[serde(default)]
    pub ttl_ms: u64,
    /// Size limit; 0 or absent = server default.
    #[serde(default)]
    pub max_bytes: u64,
}

/// A key's revision (from `GET .../history`).
#[derive(Serialize, ToSchema)]
pub struct KvEntryView {
    pub key: String,
    pub revision: u64,
    /// Append time, milliseconds since the Unix epoch.
    pub timestamp_ms: u64,
    /// `put`, `delete` or `purge`.
    pub op: String,
    /// The value, base64 (empty for a delete or purge).
    pub value_base64: String,
}

/// Result of a put or delete.
#[derive(Serialize, ToSchema)]
pub struct KvWriteResult {
    pub revision: u64,
}

#[derive(Deserialize, IntoParams)]
pub struct KeysParams {
    /// Subject filter over keys (`users.*`); absent = all.
    #[serde(default)]
    pub filter: String,
}

#[derive(Deserialize, IntoParams)]
pub struct GetParams {
    /// Read this revision instead of the current value.
    pub revision: Option<u64>,
}

#[derive(Deserialize, IntoParams)]
pub struct DeleteParams {
    /// Also hide the key's older values.
    #[serde(default)]
    pub purge: bool,
}

#[derive(Deserialize, IntoParams)]
pub struct PutParams {
    /// This value expires after this many milliseconds.
    pub ttl_ms: Option<u64>,
}

fn err(status: StatusCode, msg: impl Into<String>) -> Response {
    (status, Json(json!({"error": msg.into()}))).into_response()
}

fn authorize(
    state: &AppState,
    identity: &Option<Extension<Arc<Identity>>>,
    bucket: &str,
    action: Action,
) -> Option<Response> {
    let stream = match bucket_stream(bucket) {
        Ok(s) => s,
        Err(e) => return Some(err(StatusCode::BAD_REQUEST, e.to_string())),
    };
    let Some(Extension(id)) = identity else {
        return None;
    };
    if id.authorize(action, &stream) || id.authorize(Action::Admin, &stream) {
        None
    } else {
        state.metrics.auth_denied("forbidden", "http", "/api/v1/kv");
        Some(super::forbid())
    }
}

fn kv_error(e: KvError) -> Response {
    match e {
        KvError::Invalid(_) | KvError::NotABucket(_) => err(StatusCode::BAD_REQUEST, e.to_string()),
        KvError::BucketNotFound(_) => err(StatusCode::NOT_FOUND, e.to_string()),
        KvError::WrongRevision { current, .. } => (
            StatusCode::CONFLICT,
            Json(json!({"error": e.to_string(), "current_revision": current})),
        )
            .into_response(),
        KvError::Log(exspeed_broker::log::LogError::NotLeader) => {
            err(StatusCode::SERVICE_UNAVAILABLE, "not the leader")
        }
        KvError::Log(e) => err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

fn op_name(e: &KvEntry) -> &'static str {
    match e.op {
        exspeed_broker::kv::KvOp::Put => "put",
        exspeed_broker::kv::KvOp::Delete => "delete",
        exspeed_broker::kv::KvOp::Purge => "purge",
    }
}

/// Expected revision from `If-Match: <rev>` or `If-None-Match: *` (absent).
fn expected_revision(headers: &HeaderMap) -> Result<Option<u64>, &'static str> {
    if let Some(v) = headers.get(axum::http::header::IF_MATCH) {
        let s = v.to_str().unwrap_or_default().trim().trim_matches('"');
        return s
            .parse()
            .map(Some)
            .map_err(|_| "If-Match must be a revision number");
    }
    if headers
        .get(axum::http::header::IF_NONE_MATCH)
        .is_some_and(|v| v.as_bytes() == b"*")
    {
        return Ok(Some(0));
    }
    Ok(None)
}

/// Create a key-value bucket (idempotent for the same settings).
#[utoipa::path(
    post,
    path = "/api/v1/kv",
    tag = "kv",
    security(("bearer" = [])),
    request_body = CreateBucketRequest,
    responses(
        (status = 201, description = "Created (or already there with these settings)"),
        (status = 400, description = "Invalid name or settings, or the bucket exists with others", body = ErrorBody),
        (status = 403, description = "No admin permission on KV_<bucket>", body = ErrorBody),
    )
)]
pub async fn create_bucket(
    State(state): State<Arc<AppState>>,
    identity: Option<Extension<Arc<Identity>>>,
    Json(body): Json<CreateBucketRequest>,
) -> Response {
    if let Some(r) = authorize(&state, &identity, &body.bucket, Action::Admin) {
        return r;
    }
    let cfg = BucketConfig {
        history: body.history.max(1),
        ttl_ms: body.ttl_ms,
        max_bytes: body.max_bytes,
    };
    match state.broker.kv.create_bucket(&body.bucket, &cfg).await {
        Ok(()) => (StatusCode::CREATED, Json(json!({"bucket": body.bucket}))).into_response(),
        Err(e) => kv_error(e),
    }
}

/// Keys that currently have a value.
#[utoipa::path(
    get,
    path = "/api/v1/kv/{bucket}",
    tag = "kv",
    security(("bearer" = [])),
    params(("bucket" = String, Path, description = "Bucket name"), KeysParams),
    responses(
        (status = 200, description = "Sorted keys", body = Vec<String>),
        (status = 404, description = "No such bucket", body = ErrorBody),
    )
)]
pub async fn list_keys(
    State(state): State<Arc<AppState>>,
    Path(bucket): Path<String>,
    Query(p): Query<KeysParams>,
    identity: Option<Extension<Arc<Identity>>>,
) -> Response {
    if let Some(r) = authorize(&state, &identity, &bucket, Action::Subscribe) {
        return r;
    }
    match state.broker.kv.keys(&bucket, &p.filter).await {
        Ok(keys) => Json(keys).into_response(),
        Err(e) => kv_error(e),
    }
}

/// A key's value: the raw bytes, with the revision in `X-Exspeed-Revision`
/// (and `ETag`).
#[utoipa::path(
    get,
    path = "/api/v1/kv/{bucket}/{key}",
    tag = "kv",
    security(("bearer" = [])),
    params(
        ("bucket" = String, Path, description = "Bucket name"),
        ("key" = String, Path, description = "Key"),
        GetParams,
    ),
    responses(
        (status = 200, description = "The value (raw bytes)"),
        (status = 404, description = "No such bucket or key", body = ErrorBody),
    )
)]
pub async fn get_key(
    State(state): State<Arc<AppState>>,
    Path((bucket, key)): Path<(String, String)>,
    Query(p): Query<GetParams>,
    identity: Option<Extension<Arc<Identity>>>,
) -> Response {
    if let Some(r) = authorize(&state, &identity, &bucket, Action::Subscribe) {
        return r;
    }
    match state.broker.kv.get(&bucket, &key, p.revision).await {
        Ok(Some(e)) => {
            let mut h = HeaderMap::new();
            if let Ok(v) = HeaderValue::from_str(&e.revision.to_string()) {
                h.insert("x-exspeed-revision", v.clone());
            }
            if let Ok(v) = HeaderValue::from_str(&format!("\"{}\"", e.revision)) {
                h.insert(axum::http::header::ETAG, v);
            }
            h.insert("x-exspeed-kv-op", HeaderValue::from_static(op_name(&e)));
            (StatusCode::OK, h, e.value).into_response()
        }
        Ok(None) => err(StatusCode::NOT_FOUND, format!("key '{key}' not found")),
        Err(e) => kv_error(e),
    }
}

/// Set a key to the request body. `If-Match: <revision>` makes it a
/// compare-and-set; `If-None-Match: *` creates the key only if absent.
#[utoipa::path(
    put,
    path = "/api/v1/kv/{bucket}/{key}",
    tag = "kv",
    security(("bearer" = [])),
    params(
        ("bucket" = String, Path, description = "Bucket name"),
        ("key" = String, Path, description = "Key"),
        PutParams,
    ),
    request_body(content = Vec<u8>, content_type = "application/octet-stream"),
    responses(
        (status = 200, description = "The new revision", body = KvWriteResult),
        (status = 409, description = "The key is not at the expected revision", body = ErrorBody),
    )
)]
pub async fn put_key(
    State(state): State<Arc<AppState>>,
    Path((bucket, key)): Path<(String, String)>,
    Query(p): Query<PutParams>,
    identity: Option<Extension<Arc<Identity>>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if let Some(r) = authorize(&state, &identity, &bucket, Action::Publish) {
        return r;
    }
    let expected = match expected_revision(&headers) {
        Ok(e) => e,
        Err(m) => return err(StatusCode::BAD_REQUEST, m),
    };
    match state
        .broker
        .kv
        .put(&bucket, &key, body, expected, p.ttl_ms)
        .await
    {
        Ok(revision) => Json(KvWriteResult { revision }).into_response(),
        Err(e) => kv_error(e),
    }
}

/// Delete a key (`?purge=true` also hides its older values). Honors
/// `If-Match` like a put.
#[utoipa::path(
    delete,
    path = "/api/v1/kv/{bucket}/{key}",
    tag = "kv",
    security(("bearer" = [])),
    params(
        ("bucket" = String, Path, description = "Bucket name"),
        ("key" = String, Path, description = "Key"),
        DeleteParams,
    ),
    responses(
        (status = 200, description = "The tombstone's revision", body = KvWriteResult),
        (status = 409, description = "The key is not at the expected revision", body = ErrorBody),
    )
)]
pub async fn delete_key(
    State(state): State<Arc<AppState>>,
    Path((bucket, key)): Path<(String, String)>,
    Query(p): Query<DeleteParams>,
    identity: Option<Extension<Arc<Identity>>>,
    headers: HeaderMap,
) -> Response {
    if let Some(r) = authorize(&state, &identity, &bucket, Action::Publish) {
        return r;
    }
    let expected = match expected_revision(&headers) {
        Ok(e) => e,
        Err(m) => return err(StatusCode::BAD_REQUEST, m),
    };
    match state
        .broker
        .kv
        .delete(&bucket, &key, p.purge, expected)
        .await
    {
        Ok(revision) => Json(KvWriteResult { revision }).into_response(),
        Err(e) => kv_error(e),
    }
}

/// A key's kept revisions, oldest first.
#[utoipa::path(
    get,
    path = "/api/v1/kv/{bucket}/{key}/history",
    tag = "kv",
    security(("bearer" = [])),
    params(
        ("bucket" = String, Path, description = "Bucket name"),
        ("key" = String, Path, description = "Key"),
    ),
    responses(
        (status = 200, description = "Revisions", body = Vec<KvEntryView>),
        (status = 404, description = "No such bucket", body = ErrorBody),
    )
)]
pub async fn key_history(
    State(state): State<Arc<AppState>>,
    Path((bucket, key)): Path<(String, String)>,
    identity: Option<Extension<Arc<Identity>>>,
) -> Response {
    if let Some(r) = authorize(&state, &identity, &bucket, Action::Subscribe) {
        return r;
    }
    match state.broker.kv.history(&bucket, &key).await {
        Ok(entries) => Json(
            entries
                .iter()
                .map(|e| KvEntryView {
                    key: e.key.clone(),
                    revision: e.revision,
                    timestamp_ms: e.timestamp_ns / 1_000_000,
                    op: op_name(e).to_string(),
                    value_base64: base64::engine::general_purpose::STANDARD.encode(&e.value),
                })
                .collect::<Vec<_>>(),
        )
        .into_response(),
        Err(e) => kv_error(e),
    }
}
