mod common;
use common::make_state_with_leader;

use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use tower::ServiceExt;

#[tokio::test]
async fn standby_returns_503_on_protected_get() {
    let state = make_state_with_leader(false).await;
    let app = exspeed_api::handlers::build_router(state);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/streams")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn standby_returns_503_on_protected_post() {
    let state = make_state_with_leader(false).await;
    let app = exspeed_api::handlers::build_router(state);

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/streams")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"name":"x"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn standby_serves_leases_and_metrics() {
    let state = make_state_with_leader(false).await;
    let app = exspeed_api::handlers::build_router(state);

    // /api/v1/leases is bearer-gated but NOT leader-gated (auth_token is
    // None in tests so bearer check is skipped too).
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/leases")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // /metrics is completely unauthenticated and not leader-gated.
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // /healthz returns 503 on standby.
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn leader_serves_protected_route() {
    let state = make_state_with_leader(true).await;
    let app = exspeed_api::handlers::build_router(state);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/streams")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}
