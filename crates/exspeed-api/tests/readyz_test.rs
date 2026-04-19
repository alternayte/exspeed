mod common;
use common::make_state_with_leader;

use std::sync::atomic::Ordering;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use tower::ServiceExt;

/// Covers the startup-in-progress branch of `/readyz`. The data_dir
/// writability branch is exercised by the integration test
/// `readyz_returns_503_when_data_dir_unwritable` in
/// `crates/exspeed/tests/lifecycle_test.rs`.
#[tokio::test]
async fn readyz_returns_503_with_starting_when_not_ready() {
    let state = make_state_with_leader(true).await;
    // Common helper pre-flips ready=true. Override for this test so we
    // hit the "startup still in progress" branch before the data_dir
    // writability probe runs.
    state.ready.store(false, Ordering::Release);

    let app = exspeed_api::handlers::build_router(state);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/readyz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(json, serde_json::json!({"status": "starting"}));
}

/// `/readyz` returns 503 while dedup rebuild is still in progress.
#[tokio::test]
async fn readyz_returns_503_while_dedup_rebuilding() {
    use std::sync::atomic::Ordering;
    let state = make_state_with_leader(true).await;
    // Override the dedup_ready flag to simulate an ongoing rebuild.
    state
        .broker
        .dedup_ready
        .store(false, Ordering::Release);

    let app = exspeed_api::handlers::build_router(state);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/readyz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        json,
        serde_json::json!({"status": "dedup_rebuild_in_progress"})
    );
}

/// Sanity check the happy path so a regression in the helper doesn't
/// silently turn the 503 test into a tautology.
#[tokio::test]
async fn readyz_returns_200_when_ready_and_writable() {
    let state = make_state_with_leader(true).await;
    // Helper pre-flips ready=true and uses a writable tempdir.
    let app = exspeed_api::handlers::build_router(state);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/readyz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(json, serde_json::json!({"status": "ready"}));
}
