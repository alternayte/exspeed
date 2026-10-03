use std::sync::Arc;
use std::time::Duration;

use exspeed_client::{Client, ConnectOptions, PublishRecord, StreamSpec};
use tempfile::tempdir;

#[tokio::test]
async fn tls_cert_without_key_refuses_to_start() {
    let tmp = tempdir().unwrap();
    let fake_cert = tmp.path().join("cert.pem");
    std::fs::write(&fake_cert, "dummy").unwrap();

    let args = exspeed::cli::server::ServerArgs {
        bind: "127.0.0.1:0".to_string(),
        api_bind: "127.0.0.1:0".to_string(),
        data_dir: tmp.path().to_path_buf(),
        auth_token: None,
        credentials_file: None,
        tls_cert: Some(fake_cert),
        tls_key: None,
        storage_sync: exspeed::cli::server::StorageSyncArg::Sync,
        storage_flush_window_us: 500,
        storage_flush_threshold_records: 256,
        storage_flush_threshold_bytes: 1_048_576,
        storage_sync_interval_ms: 10,
        storage_sync_bytes: 4 * 1024 * 1024,
    };

    let result = exspeed::cli::server::run(args).await;
    let err = result.expect_err("expected failure when only tls_cert is set");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("TLS") && msg.contains("both"),
        "unexpected error message: {msg}"
    );
}

#[tokio::test]
async fn tls_key_without_cert_refuses_to_start() {
    let tmp = tempdir().unwrap();
    let fake_key = tmp.path().join("key.pem");
    std::fs::write(&fake_key, "dummy").unwrap();

    let args = exspeed::cli::server::ServerArgs {
        bind: "127.0.0.1:0".to_string(),
        api_bind: "127.0.0.1:0".to_string(),
        data_dir: tmp.path().to_path_buf(),
        auth_token: None,
        credentials_file: None,
        tls_cert: None,
        tls_key: Some(fake_key),
        storage_sync: exspeed::cli::server::StorageSyncArg::Sync,
        storage_flush_window_us: 500,
        storage_flush_threshold_records: 256,
        storage_flush_threshold_bytes: 1_048_576,
        storage_sync_interval_ms: 10,
        storage_sync_bytes: 4 * 1024 * 1024,
    };

    let result = exspeed::cli::server::run(args).await;
    let err = result.expect_err("expected failure when only tls_key is set");
    assert!(format!("{err:#}").contains("TLS"));
}

fn generate_self_signed() -> (std::path::PathBuf, std::path::PathBuf, tempfile::TempDir) {
    let cert =
        rcgen::generate_simple_self_signed(vec!["localhost".to_string(), "127.0.0.1".to_string()])
            .unwrap();

    let tmp = tempfile::tempdir().unwrap();
    let cert_path = tmp.path().join("cert.pem");
    let key_path = tmp.path().join("key.pem");

    std::fs::write(&cert_path, cert.cert.pem()).unwrap();
    std::fs::write(&key_path, cert.key_pair.serialize_pem()).unwrap();

    (cert_path, key_path, tmp)
}

#[tokio::test]
async fn tls_enabled_tcp_handshakes_with_rustls() {
    // Install rustls crypto provider for the test's client-side ClientConfig.
    // The server installs its own inside load_tls_config; this is a no-op if
    // a provider is already registered.
    let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();

    let (cert_path, key_path, _certs_tmp) = generate_self_signed();
    let data_tmp = tempfile::tempdir().unwrap();
    let port = exspeed_testkit::pick_unused_port().unwrap();
    let api_port = exspeed_testkit::pick_unused_port().unwrap();

    let args = exspeed::cli::server::ServerArgs {
        bind: format!("127.0.0.1:{port}"),
        api_bind: format!("127.0.0.1:{api_port}"),
        data_dir: data_tmp.path().to_path_buf(),
        auth_token: None,
        credentials_file: None,
        tls_cert: Some(cert_path.clone()),
        tls_key: Some(key_path),
        storage_sync: exspeed::cli::server::StorageSyncArg::Sync,
        storage_flush_window_us: 500,
        storage_flush_threshold_records: 256,
        storage_flush_threshold_bytes: 1_048_576,
        storage_sync_interval_ms: 10,
        storage_sync_bytes: 4 * 1024 * 1024,
    };

    tokio::spawn(async move {
        exspeed::cli::server::run(args).await.unwrap();
    });
    wait_for_port(port).await;

    let tls = client_config(&cert_path);
    let addr = format!("127.0.0.1:{port}");
    let c = Client::connect_tls(&addr, "localhost", tls.clone(), ConnectOptions::default())
        .await
        .unwrap();
    c.create_stream(StreamSpec::named("tls")).await.unwrap();
    c.publish("tls", PublishRecord::new("s", "over tls"))
        .await
        .unwrap();
    let r = c.read("tls", 0, 10, Duration::ZERO, "").await.unwrap();
    assert_eq!(r.records[0].value.as_ref(), b"over tls");

    // A plaintext client can't talk to the TLS port.
    assert!(Client::connect(&addr, ConnectOptions::default())
        .await
        .is_err());
}

fn client_config(cert_path: &std::path::Path) -> Arc<tokio_rustls::rustls::ClientConfig> {
    let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
    let cert_pem = std::fs::read(cert_path).unwrap();
    let cert = rustls_pemfile::certs(&mut cert_pem.as_slice())
        .next()
        .unwrap()
        .unwrap();
    let mut root_store = tokio_rustls::rustls::RootCertStore::empty();
    root_store.add(cert).unwrap();
    Arc::new(
        tokio_rustls::rustls::ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth(),
    )
}

async fn wait_for_port(port: u16) {
    for _ in 0..200 {
        if tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .is_ok()
        {
            tokio::time::sleep(Duration::from_millis(100)).await;
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("server did not listen on {port}");
}

#[tokio::test]
async fn tls_enabled_http_responds_to_rustls_request() {
    let (cert_path, key_path, _certs_tmp) = generate_self_signed();
    let data_tmp = tempfile::tempdir().unwrap();
    let port = exspeed_testkit::pick_unused_port().unwrap();
    let api_port = exspeed_testkit::pick_unused_port().unwrap();

    let args = exspeed::cli::server::ServerArgs {
        bind: format!("127.0.0.1:{port}"),
        api_bind: format!("127.0.0.1:{api_port}"),
        data_dir: data_tmp.path().to_path_buf(),
        auth_token: None,
        credentials_file: None,
        tls_cert: Some(cert_path.clone()),
        tls_key: Some(key_path),
        storage_sync: exspeed::cli::server::StorageSyncArg::Sync,
        storage_flush_window_us: 500,
        storage_flush_threshold_records: 256,
        storage_flush_threshold_bytes: 1_048_576,
        storage_sync_interval_ms: 10,
        storage_sync_bytes: 4 * 1024 * 1024,
    };

    tokio::spawn(async move {
        exspeed::cli::server::run(args).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(300)).await;

    let cert_pem = std::fs::read(&cert_path).unwrap();
    let client = reqwest::Client::builder()
        .add_root_certificate(reqwest::Certificate::from_pem(&cert_pem).unwrap())
        .build()
        .unwrap();

    let resp = client
        .get(format!("https://localhost:{api_port}/healthz"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn auth_and_tls_together_end_to_end() {
    let (cert_path, key_path, _certs_tmp) = generate_self_signed();
    let data_tmp = tempfile::tempdir().unwrap();
    let port = exspeed_testkit::pick_unused_port().unwrap();
    let api_port = exspeed_testkit::pick_unused_port().unwrap();

    let args = exspeed::cli::server::ServerArgs {
        bind: format!("127.0.0.1:{port}"),
        api_bind: format!("127.0.0.1:{api_port}"),
        data_dir: data_tmp.path().to_path_buf(),
        auth_token: Some("e2e-secret".into()),
        credentials_file: None,
        tls_cert: Some(cert_path.clone()),
        tls_key: Some(key_path),
        storage_sync: exspeed::cli::server::StorageSyncArg::Sync,
        storage_flush_window_us: 500,
        storage_flush_threshold_records: 256,
        storage_flush_threshold_bytes: 1_048_576,
        storage_sync_interval_ms: 10,
        storage_sync_bytes: 4 * 1024 * 1024,
    };

    tokio::spawn(async move {
        exspeed::cli::server::run(args).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(300)).await;

    // HTTP: GET /api/v1/streams with the bearer over TLS.
    let cert_pem = std::fs::read(&cert_path).unwrap();
    let client = reqwest::Client::builder()
        .add_root_certificate(reqwest::Certificate::from_pem(&cert_pem).unwrap())
        .build()
        .unwrap();
    let resp = client
        .get(format!("https://localhost:{api_port}/api/v1/streams"))
        .header("Authorization", "Bearer e2e-secret")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Same request without bearer → 401 even over TLS.
    let resp = client
        .get(format!("https://localhost:{api_port}/api/v1/streams"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);

    // TCP over TLS with and without the token.
    let tls = client_config(&cert_path);
    let addr = format!("127.0.0.1:{port}");
    let c = Client::connect_tls(
        &addr,
        "localhost",
        tls.clone(),
        ConnectOptions::default().token("e2e-secret"),
    )
    .await
    .unwrap();
    c.ping().await.unwrap();
    let err = Client::connect_tls(&addr, "localhost", tls, ConnectOptions::default())
        .await
        .err()
        .expect("unauthenticated connect must fail");
    assert_eq!(err.code(), Some(401));
}
