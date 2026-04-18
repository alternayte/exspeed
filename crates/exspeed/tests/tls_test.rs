use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use exspeed_protocol::codec::ExspeedCodec;
use exspeed_protocol::frame::Frame;
use exspeed_protocol::messages::connect::{AuthType, ConnectRequest};
use exspeed_protocol::opcodes::OpCode;
use futures_util::{SinkExt, StreamExt};
use tempfile::tempdir;
use tokio::net::TcpStream;
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_rustls::TlsConnector;
use tokio_util::codec::{FramedRead, FramedWrite};

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

fn generate_self_signed() -> (std::path::PathBuf, std::path::PathBuf, tempfile::TempDir) {
    let cert = rcgen::generate_simple_self_signed(vec![
        "localhost".to_string(),
        "127.0.0.1".to_string(),
    ])
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
    let port = portpicker::pick_unused_port().unwrap();
    let api_port = portpicker::pick_unused_port().unwrap();

    let args = exspeed::cli::server::ServerArgs {
        bind: format!("127.0.0.1:{port}"),
        api_bind: format!("127.0.0.1:{api_port}"),
        data_dir: data_tmp.path().to_path_buf(),
        auth_token: None,
        tls_cert: Some(cert_path.clone()),
        tls_key: Some(key_path),
    };

    tokio::spawn(async move {
        exspeed::cli::server::run(args).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Build a rustls client that trusts our self-signed cert.
    let cert_pem = std::fs::read(&cert_path).unwrap();
    let cert = rustls_pemfile::certs(&mut cert_pem.as_slice())
        .next()
        .unwrap()
        .unwrap();
    let mut root_store = tokio_rustls::rustls::RootCertStore::empty();
    root_store.add(cert).unwrap();
    let client_cfg = tokio_rustls::rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(client_cfg));

    let tcp = TcpStream::connect(format!("127.0.0.1:{port}")).await.unwrap();
    let server_name = ServerName::try_from("localhost".to_string()).unwrap();
    let tls_stream = connector.connect(server_name, tcp).await.unwrap();

    let (r, w) = tokio::io::split(tls_stream);
    let mut fr = FramedRead::new(r, ExspeedCodec::new());
    let mut fw = FramedWrite::new(w, ExspeedCodec::new());

    let mut payload = BytesMut::new();
    ConnectRequest {
        client_id: "tls-test".to_string(),
        auth_type: AuthType::None,
        auth_payload: Bytes::new(),
    }
    .encode(&mut payload);
    fw.send(Frame::new(OpCode::Connect, 1, payload.freeze()))
        .await
        .unwrap();

    let resp = tokio::time::timeout(Duration::from_secs(2), fr.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(resp.opcode, OpCode::Ok);
}

#[tokio::test]
async fn tls_enabled_http_responds_to_rustls_request() {
    let (cert_path, key_path, _certs_tmp) = generate_self_signed();
    let data_tmp = tempfile::tempdir().unwrap();
    let port = portpicker::pick_unused_port().unwrap();
    let api_port = portpicker::pick_unused_port().unwrap();

    let args = exspeed::cli::server::ServerArgs {
        bind: format!("127.0.0.1:{port}"),
        api_bind: format!("127.0.0.1:{api_port}"),
        data_dir: data_tmp.path().to_path_buf(),
        auth_token: None,
        tls_cert: Some(cert_path.clone()),
        tls_key: Some(key_path),
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
        .send().await.unwrap();

    assert_eq!(resp.status(), 200);
}
