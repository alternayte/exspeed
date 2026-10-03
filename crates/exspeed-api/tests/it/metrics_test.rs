use crate::common::make_state;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

async fn scrape(token: Option<&str>, bearer: Option<&str>) -> StatusCode {
    let app = exspeed_api::handlers::build_router(make_state(true, token).await);
    let mut req = Request::builder().uri("/metrics");
    if let Some(b) = bearer {
        req = req.header("authorization", format!("Bearer {b}"));
    }
    app.oneshot(req.body(Body::empty()).unwrap())
        .await
        .unwrap()
        .status()
}

#[tokio::test]
async fn metrics_are_open_without_a_token() {
    assert_eq!(scrape(None, None).await, StatusCode::OK);
}

#[tokio::test]
async fn metrics_token_is_required_when_configured() {
    assert_eq!(scrape(Some("s3cret"), None).await, StatusCode::UNAUTHORIZED);
    assert_eq!(
        scrape(Some("s3cret"), Some("wrong")).await,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(scrape(Some("s3cret"), Some("s3cret")).await, StatusCode::OK);
}
