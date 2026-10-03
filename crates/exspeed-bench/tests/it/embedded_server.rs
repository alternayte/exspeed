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
    let tcp_port_l = exspeed_testkit::bind_local();
    let tcp_port = tcp_port_l.local_addr().unwrap().port();
    let api_port_l = exspeed_testkit::bind_local();
    let api_port = api_port_l.local_addr().unwrap().port();
    let tcp_addr = format!("127.0.0.1:{tcp_port}");
    let api_addr = format!("127.0.0.1:{api_port}");
    let tcp_for_server = tcp_addr.clone();
    let api_for_server = api_addr.clone();

    tokio::spawn(async move {
        let _tmp = tmp; // keep TempDir alive for the full server task lifetime
        exspeed::cli::server::run(exspeed::cli::server::ServerArgs {
            bind: tcp_for_server,
            tcp_listener: Some(std::sync::Arc::new(tcp_port_l)),
            api_bind: api_for_server,
            api_listener: Some(std::sync::Arc::new(api_port_l)),
            data_dir,
            auth_token: None,
            credentials_file: None,
            tls_cert: None,
            tls_key: None,
            ..Default::default()
        })
        .await
        .unwrap();
    });

    sleep(Duration::from_millis(250)).await;
    EmbeddedServer { tcp_addr, api_addr }
}
