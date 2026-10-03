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
        .find(|l| l.contains("consumer_lag") && l.contains("consumer=\"slowpoke\""))
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
        metrics.contains("publish_latency_seconds"),
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
