use std::path::PathBuf;
use std::time::Duration;

use tempfile::tempdir;

use crate::common::{create_stream, publish_n, TestServer};

async fn start_server() -> (TestServer, u16) {
    let server = TestServer::start().await;
    let port = server.api_addr.rsplit(':').next().unwrap().parse().unwrap();
    (server, port)
}

#[tokio::test]
async fn consumer_lag_reported_via_metrics() {
    let (server, api_port) = start_server().await;
    let c = server.client().await;
    create_stream(&c, "lagging").await;
    c.create_consumer(exspeed_client::ConsumerSpec::new("slowpoke", "lagging"))
        .await
        .unwrap();
    publish_n(&c, "lagging", "x", 7).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let metrics = reqwest::get(format!("http://127.0.0.1:{api_port}/metrics"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let line = metrics
        .lines()
        .find(|l| l.starts_with("exspeed_consumer_lag{") && l.contains("consumer=\"slowpoke\""))
        .unwrap_or_else(|| panic!("no consumer_lag line for slowpoke:\n{metrics}"));
    assert!(line.trim_end().ends_with(" 7"), "lag should be 7: {line}");
}

#[tokio::test]
async fn publish_latency_histogram_reported_via_metrics() {
    let (_server, api_port) = start_server().await;

    let client = reqwest::Client::new();

    // Create stream via HTTP API.
    let resp = client
        .post(format!("http://127.0.0.1:{api_port}/api/v1/streams"))
        .json(&serde_json::json!({"name": "lat-test", "max_age_secs": 0, "max_bytes": 0}))
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "create stream: {}",
        resp.status()
    );

    // Publish a record via the HTTP API.
    let resp = client
        .post(format!(
            "http://127.0.0.1:{api_port}/api/v1/streams/lat-test/publish"
        ))
        .json(&serde_json::json!({"subject": "test", "data": {"msg": "hello"}}))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "publish: {}", resp.status());

    // Scrape /metrics.
    let metrics = reqwest::get(format!("http://127.0.0.1:{api_port}/metrics"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    assert!(
        metrics.contains("exspeed_publish_latency_seconds_bucket"),
        "metrics body did not include publish_latency_seconds; got:\n{metrics}"
    );
    assert!(
        metrics.contains("stream=\"lat-test\""),
        "metrics body did not include stream=lat-test label; got:\n{metrics}"
    );
}

// ---------------------------------------------------------------------------
// Snapshot tests (Task 7)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn snapshot_creates_tar_gz_of_data_dir() {
    let tmp = tempdir().unwrap();
    let data_dir: PathBuf = tmp.path().to_path_buf();

    std::fs::create_dir_all(data_dir.join("streams/example/partitions/0")).unwrap();
    std::fs::write(
        data_dir.join("streams/example/partitions/0/dummy"),
        b"hello",
    )
    .unwrap();

    let out = tmp.path().join("snap.tar.gz");

    exspeed::cli::snapshot::run(exspeed::cli::snapshot::SnapshotArgs {
        data_dir: data_dir.clone(),
        output: out.clone(),
    })
    .await
    .expect("snapshot should succeed against unlocked data_dir");

    let metadata = std::fs::metadata(&out).unwrap();
    assert!(metadata.len() > 0, "snapshot file should not be empty");
    assert!(metadata.is_file());
}

#[tokio::test]
async fn snapshot_refuses_when_server_holds_lock() {
    let tmp = tempdir().unwrap();
    let data_dir: PathBuf = tmp.path().to_path_buf();

    // Take the lock as if we were a running server.
    let _lock = exspeed::cli::server_lock::acquire_data_dir_lock(&data_dir)
        .expect("first lock should succeed");

    let out = tmp.path().join("snap.tar.gz");
    let err = exspeed::cli::snapshot::run(exspeed::cli::snapshot::SnapshotArgs {
        data_dir,
        output: out,
    })
    .await
    .expect_err("snapshot should fail while data_dir is locked");

    let msg = format!("{err:#}");
    assert!(
        msg.contains("already in use") || msg.contains("in use"),
        "unexpected error: {msg}"
    );
}

async fn scrape(server: &TestServer) -> String {
    reqwest::get(server.api_url("/metrics"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap()
}

#[tokio::test]
async fn consumer_lag_excludes_acked_records() {
    let server = TestServer::start().await;
    let c = server.client().await;
    create_stream(&c, "lag2").await;
    c.create_consumer(exspeed_client::ConsumerSpec::new("half", "lag2"))
        .await
        .unwrap();
    publish_n(&c, "lag2", "x", 5).await;
    let got = c.pull("half", 2, Duration::from_secs(2)).await.unwrap();
    assert_eq!(got.len(), 2);
    c.ack("half", got.iter().map(|r| r.offset).collect())
        .await
        .unwrap();
    let want = "exspeed_consumer_lag{consumer=\"half\",stream=\"lag2\"} 3";
    crate::common::eventually(Duration::from_secs(5), || async {
        scrape(&server).await.contains(want).then_some(())
    })
    .await;
}

#[tokio::test]
async fn deleted_streams_and_consumers_leave_no_series() {
    let server = TestServer::start().await;
    let c = server.client().await;
    for s in ["gone", "kept"] {
        create_stream(&c, s).await;
        publish_n(&c, s, "x", 2).await;
    }
    c.create_consumer(exspeed_client::ConsumerSpec::new("gone-c", "gone"))
        .await
        .unwrap();
    c.create_consumer(exspeed_client::ConsumerSpec::new("kept-c", "kept"))
        .await
        .unwrap();
    let before = scrape(&server).await;
    assert!(
        before.contains("exspeed_records_published_total{stream=\"gone\"} 2"),
        "{before}"
    );
    assert!(
        before.contains("exspeed_storage_bytes{stream=\"gone\"}"),
        "{before}"
    );
    assert!(before.contains("consumer=\"gone-c\""), "{before}");

    let r = reqwest::Client::new()
        .delete(server.api_url("/api/v1/streams/gone?force=true"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    c.delete_consumer("kept-c").await.unwrap();

    let after = scrape(&server).await;
    assert!(!after.contains("\"gone\""), "stale stream series:\n{after}");
    assert!(!after.contains("gone-c"), "stale consumer series:\n{after}");
    assert!(!after.contains("kept-c"), "stale consumer series:\n{after}");
    assert!(after.contains("exspeed_records_published_total{stream=\"kept\"} 2"));
}

#[tokio::test]
async fn auth_denials_are_labelled_with_the_route_not_the_path() {
    let server = TestServer::builder().auth_token("right").start().await;
    let http = reqwest::Client::new();
    for name in ["s1", "s2", "s3"] {
        let r = http
            .get(server.api_url(&format!("/api/v1/streams/{name}")))
            .bearer_auth("wrong")
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 401);
    }
    let m = scrape(&server).await;
    let line = m
        .lines()
        .find(|l| l.starts_with("exspeed_auth_denied_total{") && l.contains("transport=\"http\""))
        .unwrap_or_else(|| panic!("no http denial series:\n{m}"));
    assert!(line.contains("op=\"/api/v1/streams/{name}\""), "{line}");
    assert!(line.trim_end().ends_with(" 3"), "{line}");
    assert!(!m.contains("op=\"/api/v1/streams/s1\""), "{m}");
}

#[tokio::test]
async fn every_scraped_series_is_prefixed() {
    let server = TestServer::start().await;
    let c = server.client().await;
    create_stream(&c, "p").await;
    publish_n(&c, "p", "x", 1).await;
    let m = scrape(&server).await;
    for line in m.lines().filter(|l| !l.starts_with('#') && !l.is_empty()) {
        assert!(line.starts_with("exspeed_"), "{line}");
        assert!(!line.contains("_total_total"), "{line}");
    }
}
