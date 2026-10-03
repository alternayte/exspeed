//! A fenced (failed) partition is visible: `/readyz` detail, the
//! `exspeed_partition_failed` gauge and the stream's `status`.

use crate::common::make_state_with_leader;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use tower::ServiceExt;

async fn get(app: &axum::Router, uri: &str) -> (StatusCode, String) {
    let resp = app
        .clone()
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8_lossy(&body).into_owned())
}

#[tokio::test]
async fn fenced_partition_shows_in_readyz_metrics_and_stream_status() {
    let state = make_state_with_leader(true).await;
    for s in ["ok-stream", "bad-stream"] {
        let name = exspeed_common::StreamName::try_from(s).unwrap();
        state
            .broker
            .log
            .create_stream(&name, &Default::default())
            .await
            .unwrap();
    }
    let app = exspeed_api::handlers::build_router(state.clone());

    let (code, body) = get(&app, "/readyz").await;
    assert_eq!(
        (code, body.as_str()),
        (StatusCode::OK, r#"{"status":"ready"}"#)
    );
    let (_, m) = get(&app, "/metrics").await;
    assert!(
        m.contains(r#"exspeed_partition_failed{stream="bad-stream"} 0"#),
        "{m}"
    );

    assert!(state
        .storage
        .fence_stream("bad-stream", "injected: fsync failed"));

    let (code, body) = get(&app, "/readyz").await;
    assert_eq!(code, StatusCode::OK);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["status"], "degraded");
    assert_eq!(
        v["failed_streams"],
        serde_json::json!([{"stream": "bad-stream", "reason": "injected: fsync failed"}])
    );

    let (_, m) = get(&app, "/metrics").await;
    assert!(
        m.contains(r#"exspeed_partition_failed{stream="bad-stream"} 1"#),
        "{m}"
    );
    assert!(
        m.contains(r#"exspeed_partition_failed{stream="ok-stream"} 0"#),
        "{m}"
    );

    let (code, body) = get(&app, "/api/v1/streams/bad-stream").await;
    assert_eq!(code, StatusCode::OK);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["status"], "failed");
    assert_eq!(v["failure"], "injected: fsync failed");
    let (_, body) = get(&app, "/api/v1/streams/ok-stream").await;
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["status"], "healthy");
    assert!(v.get("failure").is_none());
}
