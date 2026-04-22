use std::time::Duration;

async fn start_server() -> (String, String) {
    let tcp_port = portpicker::pick_unused_port().unwrap();
    let http_port = portpicker::pick_unused_port().unwrap();
    let tcp_addr = format!("127.0.0.1:{}", tcp_port);
    let http_addr = format!("127.0.0.1:{}", http_port);

    let dir = tempfile::TempDir::new().unwrap();
    let data_dir = dir.path().to_path_buf();
    let tcp_addr_clone = tcp_addr.clone();
    let http_addr_clone = http_addr.clone();

    tokio::spawn(async move {
        let _keep = dir;
        exspeed::cli::server::run(exspeed::cli::server::ServerArgs {
            bind: tcp_addr_clone,
            api_bind: http_addr_clone,
            data_dir,
            auth_token: None,
            credentials_file: None,
            tls_cert: None,
            tls_key: None,
            storage_sync: exspeed::cli::server::StorageSyncArg::Sync,
            storage_flush_window_us: 500,
            storage_flush_threshold_records: 256,
            storage_flush_threshold_bytes: 1_048_576,
            storage_sync_interval_ms: 10,
            storage_sync_bytes: 4 * 1024 * 1024,
            delivery_buffer: 8192,
        })
        .await
        .unwrap();
    });

    tokio::time::sleep(Duration::from_millis(300)).await;
    (tcp_addr, format!("http://127.0.0.1:{}", http_port))
}

#[tokio::test]
async fn delete_nonexistent_returns_404() {
    let (_tcp, http) = start_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .delete(format!("{}/api/v1/streams/missing", http))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn delete_with_no_references_succeeds() {
    let (_tcp, http) = start_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/api/v1/streams", http))
        .json(&serde_json::json!({"name": "empty-stream"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);

    let resp = client
        .delete(format!("{}/api/v1/streams/empty-stream", http))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "delete should return 200");

    // GET should now 404.
    let resp = client
        .get(format!("{}/api/v1/streams/empty-stream", http))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404, "stream should be gone");
}
