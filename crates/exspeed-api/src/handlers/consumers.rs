use std::sync::Arc;

use axum::extract::{Extension, Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use exspeed_common::auth::Identity;
use exspeed_common::StreamName;
use exspeed_protocol::messages::{ClientMessage, DeleteConsumerRequest};
use serde_json::json;

use crate::state::AppState;

pub async fn list_consumers(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let consumers = state.broker.consumers.read().unwrap();
    let list: Vec<_> = consumers
        .values()
        .map(|cs| {
            json!({
                "name": cs.config.name,
                "stream": cs.config.stream,
                "group": cs.config.group,
                "subject_filter": cs.config.subject_filter,
                "offset": cs.config.offset,
            })
        })
        .collect();
    (StatusCode::OK, Json(json!(list)))
}

/// Helper: look up the stream name attached to a consumer. Returns a parsed
/// `StreamName` if the consumer exists and its stream is a valid name, or
/// `None` if the consumer doesn't exist (caller should return 404). If the
/// stored stream name is invalid (e.g. legacy data), treat as 404.
fn consumer_stream(state: &AppState, consumer: &str) -> Option<StreamName> {
    let consumers = state.broker.consumers.read().ok()?;
    let cs = consumers.get(consumer)?;
    StreamName::try_from(cs.config.stream.as_str()).ok()
}

pub async fn get_consumer(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    identity: Option<Extension<Arc<Identity>>>,
) -> Response {
    // Resolve the consumer -> stream mapping BEFORE the authz check so we
    // 403 (not 404) for a caller who can't admin the attached stream.
    // If the consumer doesn't exist we have nothing to authz against; fall
    // through to the 404 branch below (handled after the authz gate, which
    // is a no-op in that case because the Option chain never returns Some).
    let stream = consumer_stream(&state, &name);

    if let Some(Extension(id)) = identity.as_ref() {
        if let Some(ref s) = stream {
            if let Some(resp) = super::require_scoped_admin(id, s) {
                return resp;
            }
        } else {
            // Unknown consumer: 404 without leaking existence of other
            // consumers. No authz check possible — fall through.
        }
    }

    let consumers = state.broker.consumers.read().unwrap();
    match consumers.get(&name) {
        Some(cs) => {
            let head = state
                .storage
                .stream_head_offset(&cs.config.stream)
                .unwrap_or(0);
            let lag = head.saturating_sub(cs.config.offset);
            (
                StatusCode::OK,
                Json(json!({
                    "name": cs.config.name,
                    "stream": cs.config.stream,
                    "group": cs.config.group,
                    "subject_filter": cs.config.subject_filter,
                    "offset": cs.config.offset,
                    "lag": lag,
                })),
            )
                .into_response()
        }
        None => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": format!("consumer '{}' not found", name)})),
        )
            .into_response(),
    }
}

pub async fn delete_consumer(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    identity: Option<Extension<Arc<Identity>>>,
) -> Response {
    let stream = consumer_stream(&state, &name);

    if let Some(Extension(id)) = identity.as_ref() {
        if let Some(ref s) = stream {
            if let Some(resp) = super::require_scoped_admin(id, s) {
                return resp;
            }
        }
    }

    let req = DeleteConsumerRequest { name: name.clone() };
    match state
        .broker
        .handle_message(ClientMessage::DeleteConsumer(req))
        .await
    {
        exspeed_protocol::messages::ServerMessage::Ok => {
            (StatusCode::OK, Json(json!({"deleted": name}))).into_response()
        }
        _ => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": format!("consumer '{}' not found", name)})),
        )
            .into_response(),
    }
}
