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
        ..Default::default()
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
        ..Default::default()
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
    let port_l = exspeed_testkit::bind_local();
    let port = port_l.local_addr().unwrap().port();
    let api_port_l = exspeed_testkit::bind_local();
    let api_port = api_port_l.local_addr().unwrap().port();

    let args = exspeed::cli::server::ServerArgs {
        bind: format!("127.0.0.1:{port}"),
        tcp_listener: Some(std::sync::Arc::new(port_l)),
        api_bind: format!("127.0.0.1:{api_port}"),
        api_listener: Some(std::sync::Arc::new(api_port_l)),
        data_dir: data_tmp.path().to_path_buf(),
        auth_token: None,
        credentials_file: None,
        tls_cert: Some(cert_path.clone()),
        tls_key: Some(key_path),
        ..Default::default()
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
    let port_l = exspeed_testkit::bind_local();
    let port = port_l.local_addr().unwrap().port();
    let api_port_l = exspeed_testkit::bind_local();
    let api_port = api_port_l.local_addr().unwrap().port();

    let args = exspeed::cli::server::ServerArgs {
        bind: format!("127.0.0.1:{port}"),
        tcp_listener: Some(std::sync::Arc::new(port_l)),
        api_bind: format!("127.0.0.1:{api_port}"),
        api_listener: Some(std::sync::Arc::new(api_port_l)),
        data_dir: data_tmp.path().to_path_buf(),
        auth_token: None,
        credentials_file: None,
        tls_cert: Some(cert_path.clone()),
        tls_key: Some(key_path),
        ..Default::default()
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
    let port_l = exspeed_testkit::bind_local();
    let port = port_l.local_addr().unwrap().port();
    let api_port_l = exspeed_testkit::bind_local();
    let api_port = api_port_l.local_addr().unwrap().port();

    let args = exspeed::cli::server::ServerArgs {
        bind: format!("127.0.0.1:{port}"),
        tcp_listener: Some(std::sync::Arc::new(port_l)),
        api_bind: format!("127.0.0.1:{api_port}"),
        api_listener: Some(std::sync::Arc::new(api_port_l)),
        data_dir: data_tmp.path().to_path_buf(),
        auth_token: Some("e2e-secret".into()),
        credentials_file: None,
        tls_cert: Some(cert_path.clone()),
        tls_key: Some(key_path),
        ..Default::default()
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

/// A test CA plus certificates it signs.
struct TestCa {
    cert: rcgen::Certificate,
    key: rcgen::KeyPair,
}

impl TestCa {
    fn new(name: &str) -> Self {
        let mut p = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        p.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        p.distinguished_name
            .push(rcgen::DnType::CommonName, name.to_string());
        let key = rcgen::KeyPair::generate().unwrap();
        let cert = p.self_signed(&key).unwrap();
        Self { cert, key }
    }

    /// A leaf certificate with `cn` and the DNS names `sans`.
    fn issue(&self, cn: &str, sans: &[&str]) -> (rcgen::Certificate, rcgen::KeyPair) {
        let mut p =
            rcgen::CertificateParams::new(sans.iter().map(|s| s.to_string()).collect::<Vec<_>>())
                .unwrap();
        p.distinguished_name
            .push(rcgen::DnType::CommonName, cn.to_string());
        let key = rcgen::KeyPair::generate().unwrap();
        let cert = p.signed_by(&key, &self.cert, &self.key).unwrap();
        (cert, key)
    }
}

fn mtls_client(
    ca: &TestCa,
    identity: Option<(&rcgen::Certificate, &rcgen::KeyPair)>,
) -> Arc<tokio_rustls::rustls::ClientConfig> {
    use tokio_rustls::rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};
    let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
    let mut roots = tokio_rustls::rustls::RootCertStore::empty();
    roots.add(ca.cert.der().clone()).unwrap();
    let b = tokio_rustls::rustls::ClientConfig::builder().with_root_certificates(roots);
    Arc::new(match identity {
        Some((cert, key)) => b
            .with_client_auth_cert(
                vec![cert.der().clone()],
                PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key.serialize_der())),
            )
            .unwrap(),
        None => b.with_no_client_auth(),
    })
}

#[tokio::test]
async fn mutual_tls_maps_client_certificates_to_credentials() {
    let ca = TestCa::new("exspeed test CA");
    let tmp = tempfile::tempdir().unwrap();
    let (server_cert, server_key) = ca.issue("exspeed", &["localhost"]);
    let p = |n: &str| tmp.path().join(n);
    std::fs::write(p("server.pem"), server_cert.pem()).unwrap();
    std::fs::write(p("server.key"), server_key.serialize_pem()).unwrap();
    std::fs::write(p("ca.pem"), ca.cert.pem()).unwrap();
    std::fs::write(
        p("credentials.toml"),
        r#"
[[credentials]]
name = "orders-svc"
cert_cn = "orders.internal"
permissions = [{ streams = "orders", actions = ["publish", "subscribe", "admin"] }]
"#,
    )
    .unwrap();

    let data = tempfile::tempdir().unwrap();
    let port_l = exspeed_testkit::bind_local();
    let port = port_l.local_addr().unwrap().port();
    let args = exspeed::cli::server::ServerArgs {
        bind: format!("127.0.0.1:{port}"),
        tcp_listener: Some(Arc::new(port_l)),
        api_bind: "127.0.0.1:0".into(),
        api_listener: Some(Arc::new(exspeed_testkit::bind_local())),
        data_dir: data.path().to_path_buf(),
        credentials_file: Some(p("credentials.toml")),
        tls_cert: Some(p("server.pem")),
        tls_key: Some(p("server.key")),
        tls_client_ca: Some(p("ca.pem")),
        ..Default::default()
    };
    tokio::spawn(async move {
        exspeed::cli::server::run(args).await.unwrap();
    });
    wait_for_port(port).await;
    let addr = format!("127.0.0.1:{port}");
    let connect = |cfg| Client::connect_tls(&addr, "localhost", cfg, ConnectOptions::default());

    // A valid certificate bound to a credential: no token needed, and its
    // permissions apply.
    let (cert, key) = ca.issue("orders.internal", &[]);
    let c = connect(mtls_client(&ca, Some((&cert, &key))))
        .await
        .unwrap();
    c.create_stream(StreamSpec::named("orders")).await.unwrap();
    c.publish("orders", PublishRecord::new("o", "x"))
        .await
        .unwrap();
    let err = c
        .create_stream(StreamSpec::named("billing"))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some(exspeed_client::code::FORBIDDEN));

    // No certificate, or one from another CA: the TLS handshake fails.
    assert!(connect(mtls_client(&ca, None)).await.is_err());
    let rogue = TestCa::new("rogue CA");
    let (rcert, rkey) = rogue.issue("orders.internal", &[]);
    assert!(connect(mtls_client(&ca, Some((&rcert, &rkey))))
        .await
        .is_err());

    // A valid certificate that no credential names: unauthorized.
    let (ucert, ukey) = ca.issue("stranger", &[]);
    let err = connect(mtls_client(&ca, Some((&ucert, &ukey))))
        .await
        .err()
        .expect("unbound certificate must not authenticate");
    assert_eq!(
        err.code(),
        Some(exspeed_client::code::UNAUTHORIZED),
        "{err}"
    );
}
