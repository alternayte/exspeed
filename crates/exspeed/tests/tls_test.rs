#![allow(unused_imports)]

use std::path::PathBuf;
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
        tls_cert: Some(fake_cert),
        tls_key: None,
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
        tls_cert: None,
        tls_key: Some(fake_key),
    };

    let result = exspeed::cli::server::run(args).await;
    let err = result.expect_err("expected failure when only tls_key is set");
    assert!(format!("{err:#}").contains("TLS"));
}

