// Shared helper for bench integration tests. Mirrors the pattern in
// crates/exspeed/tests/broker_test.rs.

use std::path::PathBuf;
use tempfile::TempDir;
use tokio::time::{sleep, Duration};

pub struct EmbeddedServer {
    pub tcp_addr: String,
    #[allow(dead_code)]
    pub api_addr: String,
    _tmp: TempDir,
}

pub async fn start() -> EmbeddedServer {
    let tmp = tempfile::tempdir().unwrap();
    let data_dir: PathBuf = tmp.path().to_path_buf();
    let tcp_port = portpicker::pick_unused_port().unwrap();
    let api_port = portpicker::pick_unused_port().unwrap();
    let tcp_addr = format!("127.0.0.1:{tcp_port}");
    let api_addr = format!("127.0.0.1:{api_port}");
    let tcp_for_server = tcp_addr.clone();
    let api_for_server = api_addr.clone();
    let data_for_server = data_dir.clone();

    tokio::spawn(async move {
        exspeed::cli::server::run(exspeed::cli::server::ServerArgs {
            bind: tcp_for_server,
            api_bind: api_for_server,
            data_dir: data_for_server,
            auth_token: None,
            credentials_file: None,
            tls_cert: None,
            tls_key: None,
        })
        .await
        .unwrap();
    });

    sleep(Duration::from_millis(250)).await;
    EmbeddedServer { tcp_addr, api_addr, _tmp: tmp }
}
