// Shared helper for bench integration tests. Mirrors the pattern in
// crates/exspeed/tests/broker_test.rs.

use tokio::time::{sleep, Duration};

pub struct EmbeddedServer {
    pub tcp_addr: String,
    #[allow(dead_code)]
    pub api_addr: String,
}

pub async fn start() -> EmbeddedServer {
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().to_path_buf();
    let tcp_port = portpicker::pick_unused_port().unwrap();
    let api_port = portpicker::pick_unused_port().unwrap();
    let tcp_addr = format!("127.0.0.1:{tcp_port}");
    let api_addr = format!("127.0.0.1:{api_port}");
    let tcp_for_server = tcp_addr.clone();
    let api_for_server = api_addr.clone();

    tokio::spawn(async move {
        let _tmp = tmp; // keep TempDir alive for the full server task lifetime
        exspeed::cli::server::run(exspeed::cli::server::ServerArgs {
            bind: tcp_for_server,
            api_bind: api_for_server,
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

    sleep(Duration::from_millis(250)).await;
    EmbeddedServer { tcp_addr, api_addr }
}
