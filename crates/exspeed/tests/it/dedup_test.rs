//! Idempotent publish (`msg_id` / `x-idempotency-key`) end to end,
//! including across restarts.

use std::time::Duration;

use exspeed_client::{code, PublishRecord};

use crate::common::{create_stream, TestServer};

async fn http_create_stream_with_dedup(server: &TestServer, name: &str, window: u64, max: u64) {
    let resp = reqwest::Client::new()
        .post(server.api_url("/api/v1/streams"))
        .json(&serde_json::json!({
            "name": name,
            "dedup_window_secs": window,
            "dedup_max_entries": max,
        }))
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "create stream: {}",
        resp.status()
    );
}

#[tokio::test]
async fn same_id_and_body_is_a_duplicate() {
    let server = TestServer::start().await;
    let c = server.client().await;
    create_stream(&c, "orders").await;
    let a = c
        .publish("orders", PublishRecord::new("o", "body").msg_id("m1"))
        .await
        .unwrap();
    assert!(!a.duplicate);
    let b = c
        .publish("orders", PublishRecord::new("o", "body").msg_id("m1"))
        .await
        .unwrap();
    assert!(b.duplicate);
    assert_eq!(a.offset, b.offset);
}

#[tokio::test]
async fn same_id_different_body_is_a_conflict() {
    let server = TestServer::start().await;
    let c = server.client().await;
    create_stream(&c, "s").await;
    let a = c
        .publish("s", PublishRecord::new("e", "A").msg_id("m"))
        .await
        .unwrap();
    let err = c
        .publish("s", PublishRecord::new("e", "B").msg_id("m"))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some(code::CONFLICT));
    match err {
        exspeed_client::Error::Server {
            detail: Some(d), ..
        } => assert_eq!(d["stored_offset"], a.offset),
        other => panic!("expected detail, got {other:?}"),
    }
}

#[tokio::test]
async fn header_form_is_equivalent() {
    let server = TestServer::start().await;
    let c = server.client().await;
    create_stream(&c, "s").await;
    let rec = || PublishRecord::new("e", "body").header("x-idempotency-key", "k1");
    let a = c.publish("s", rec()).await.unwrap();
    let b = c.publish("s", rec()).await.unwrap();
    assert!(b.duplicate);
    assert_eq!(a.offset, b.offset);
}

async fn publish_ten(c: &exspeed_client::Client, stream: &str, expect_dup: bool) {
    for i in 0..10 {
        let ack = c
            .publish(
                stream,
                PublishRecord::new("e", format!("body-{i}")).msg_id(format!("msg-{i}")),
            )
            .await
            .unwrap();
        assert_eq!(ack.duplicate, expect_dup, "msg-{i}");
        assert_eq!(ack.offset, i);
    }
}

#[tokio::test]
async fn dedup_survives_restart_via_snapshot() {
    let server = TestServer::start().await;
    {
        let c = server.client().await;
        create_stream(&c, "snap").await;
        publish_ten(&c, "snap", false).await;
    }
    let server = server.restart().await;
    let c = server.client().await;
    publish_ten(&c, "snap", true).await;
}

#[tokio::test]
async fn dedup_survives_restart_without_snapshot() {
    let mut server = TestServer::start().await;
    {
        let c = server.client().await;
        create_stream(&c, "scan").await;
        publish_ten(&c, "scan", false).await;
    }
    server.stop().await;
    let snapshot = server
        .data_dir
        .join("streams")
        .join("scan")
        .join("dedup_snapshot.bin");
    let _ = std::fs::remove_file(snapshot);
    let server = server.restart().await;
    let c = server.client().await;
    publish_ten(&c, "scan", true).await;
}

#[tokio::test]
async fn full_dedup_map_rejects_then_recovers_after_window() {
    let server = TestServer::start().await;
    http_create_stream_with_dedup(&server, "cap", 1, 2).await;
    let c = server.client().await;
    for (i, k) in ["k1", "k2"].iter().enumerate() {
        c.publish("cap", PublishRecord::new("e", format!("b{i}")).msg_id(*k))
            .await
            .unwrap();
    }
    let err = c
        .publish("cap", PublishRecord::new("e", "b3").msg_id("k3"))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some(code::TOO_MANY_REQUESTS));

    // Records without a msg_id are unaffected by the cap.
    let plain = c
        .publish("cap", PublishRecord::new("e", "plain"))
        .await
        .unwrap();
    assert!(!plain.duplicate);

    tokio::time::sleep(Duration::from_millis(1500)).await;
    let ok = c
        .publish("cap", PublishRecord::new("e", "b3").msg_id("k3"))
        .await
        .unwrap();
    assert!(!ok.duplicate);
}
