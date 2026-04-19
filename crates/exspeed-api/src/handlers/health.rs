use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde_json::json;

use crate::state::AppState;

/// `GET /healthz` — leader-aware readiness probe.
///
/// Returns `200` with `{"leader": true, "holder": "<uuid>"}` when this pod
/// holds the cluster-leader lease, `503` with `{"leader": false}`
/// otherwise. Load balancers should probe this endpoint and route traffic
/// only to pods returning 200.
pub async fn healthz(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let leader = state.leadership.is_currently_leader();
    let status = if leader {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        status,
        Json(json!({
            "leader": leader,
            "holder": state.leadership.holder_id.to_string(),
        })),
    )
}

/// `GET /readyz` — startup + data-dir writability probe.
///
/// Distinct from `/healthz`. Returns 503 while startup is still in
/// progress (`state.ready == false`) and 503 if a touch+remove probe
/// against `data_dir` fails (read-only mount, full disk, permission
/// loss, etc.). Returns 200 only when both checks pass. Intended for
/// k8s readiness gates; `/healthz` is the load-balancer routing probe.
pub async fn readyz(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    use std::sync::atomic::Ordering;
    if !state.ready.load(Ordering::Acquire) {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"status": "starting"})),
        );
    }

    if let Err(e) = probe_data_dir(&state.data_dir).await {
        tracing::warn!(error = %e, data_dir = %state.data_dir.display(), "/readyz probe failed");
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"status": "data_dir_unwritable"})),
        );
    }

    (StatusCode::OK, Json(json!({"status": "ready"})))
}

async fn probe_data_dir(data_dir: &std::path::Path) -> std::io::Result<()> {
    let probe = data_dir.join(".readyz-probe");
    tokio::task::spawn_blocking(move || {
        std::fs::write(&probe, b"ok")?;
        std::fs::remove_file(&probe)
    })
    .await
    .map_err(std::io::Error::other)??;
    Ok(())
}
