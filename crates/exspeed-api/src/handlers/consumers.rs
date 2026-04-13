use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
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

pub async fn get_consumer(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> impl IntoResponse {
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
        }
        None => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": format!("consumer '{}' not found", name)})),
        ),
    }
}

pub async fn delete_consumer(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let req = DeleteConsumerRequest { name: name.clone() };
    match state
        .broker
        .handle_message(ClientMessage::DeleteConsumer(req))
    {
        exspeed_protocol::messages::ServerMessage::Ok => {
            (StatusCode::OK, Json(json!({"deleted": name})))
        }
        _ => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": format!("consumer '{}' not found", name)})),
        ),
    }
}
