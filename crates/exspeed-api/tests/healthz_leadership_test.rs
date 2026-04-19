mod common;
use common::make_state_with_leader;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use tower::ServiceExt;

#[tokio::test]
async fn healthz_returns_200_when_leader() {
    let state = make_state_with_leader(true).await;
    let app = exspeed_api::handlers::build_router(state);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(json["leader"], serde_json::json!(true));
    assert!(
        json["holder"].is_string(),
        "holder UUID should be present; got: {:?}",
        json
    );
}

#[tokio::test]
async fn healthz_returns_503_when_standby() {
    let state = make_state_with_leader(false).await;
    let app = exspeed_api::handlers::build_router(state);

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
