use std::time::Duration;

use exspeed_client::{Client, ConnectOptions, Request, Response};
use futures_util::{SinkExt, StreamExt};
use tempfile::TempDir;
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_util::codec::{FramedRead, FramedWrite};

async fn start_server(auth_token: Option<String>) -> (String, TempDir) {
    let (bind, _api, tmp) = start_server_with_api(auth_token).await;
    (bind, tmp)
}

async fn connect(addr: &str, token: Option<&str>) -> exspeed_client::Result<Client> {
    let opts = ConnectOptions {
        token: token.map(str::to_string),
        ..Default::default()
    };
    Client::connect(addr, opts).await
}

#[tokio::test]
async fn auth_disabled_accepts_any_connect() {
    let (addr, _tmp) = start_server(None).await;
    connect(&addr, None).await.unwrap().ping().await.unwrap();
    // A token is ignored when auth is off.
    connect(&addr, Some("whatever")).await.unwrap();
}

#[tokio::test]
async fn auth_enabled_rejects_missing_token() {
    let (addr, _tmp) = start_server(Some("secret123".into())).await;
    let err = connect(&addr, None).await.err().unwrap();
    assert_eq!(err.code(), Some(401));
}

#[tokio::test]
async fn auth_enabled_rejects_wrong_token() {
    let (addr, _tmp) = start_server(Some("secret123".into())).await;
    let err = connect(&addr, Some("wrong")).await.err().unwrap();
    assert_eq!(err.code(), Some(401));
}

#[tokio::test]
async fn auth_enabled_accepts_correct_token() {
    let (addr, _tmp) = start_server(Some("secret123".into())).await;
    let c = connect(&addr, Some("secret123")).await.unwrap();
    c.ping().await.unwrap();
}

#[tokio::test]
async fn auth_enabled_blocks_ops_before_connect() {
    let (addr, _tmp) = start_server(Some("secret123".into())).await;
    let (r, w) = TcpStream::connect(&addr).await.unwrap().into_split();
    let mut fr = FramedRead::new(r, exspeed_protocol::codec::ExspeedCodec::new());
    let mut fw = FramedWrite::new(w, exspeed_protocol::codec::ExspeedCodec::new());
    fw.send(Request::Ping.into_frame(7)).await.unwrap();
    let resp = timeout(Duration::from_secs(2), fr.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(matches!(
        Response::from_frame(&resp).unwrap(),
        Response::Error { code: 401, .. }
    ));
}

async fn start_server_with_api(auth_token: Option<String>) -> (String, u16, TempDir) {
    let port_l = exspeed_testkit::bind_local();
    let port = port_l.local_addr().unwrap().port();
    let api_port_l = exspeed_testkit::bind_local();
    let api_port = api_port_l.local_addr().unwrap().port();
    let bind = format!("127.0.0.1:{port}");
    let api_bind = format!("127.0.0.1:{api_port}");
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().to_path_buf();

    let args = exspeed::cli::server::ServerArgs {
        bind: bind.clone(),
        tcp_listener: Some(std::sync::Arc::new(port_l)),
        api_bind,
        api_listener: Some(std::sync::Arc::new(api_port_l)),
        data_dir,
        auth_token,
        credentials_file: None,
        tls_cert: None,
        tls_key: None,
        ..Default::default()
    };

    tokio::spawn(async move {
        exspeed::cli::server::run(args).await.unwrap();
    });

    tokio::time::sleep(Duration::from_millis(200)).await;
    (bind, api_port, tmp)
}

#[tokio::test]
async fn http_rejects_missing_bearer() {
    let (_, api_port, _tmp) = start_server_with_api(Some("secret123".into())).await;
    let resp = reqwest::get(format!("http://127.0.0.1:{api_port}/api/v1/streams"))
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn http_rejects_malformed_authorization() {
    let (_, api_port, _tmp) = start_server_with_api(Some("secret123".into())).await;
    let resp = reqwest::Client::new()
        .get(format!("http://127.0.0.1:{api_port}/api/v1/streams"))
        .header("Authorization", "Basic abc")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn http_rejects_wrong_bearer() {
    let (_, api_port, _tmp) = start_server_with_api(Some("secret123".into())).await;
    let resp = reqwest::Client::new()
        .get(format!("http://127.0.0.1:{api_port}/api/v1/streams"))
        .header("Authorization", "Bearer wrong-token")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn http_accepts_valid_bearer() {
    let (_, api_port, _tmp) = start_server_with_api(Some("secret123".into())).await;
    let resp = reqwest::Client::new()
        .get(format!("http://127.0.0.1:{api_port}/api/v1/streams"))
        .header("Authorization", "Bearer secret123")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn http_healthz_bypasses_auth() {
    let (_, api_port, _tmp) = start_server_with_api(Some("secret123".into())).await;
    let resp = reqwest::get(format!("http://127.0.0.1:{api_port}/healthz"))
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn http_readyz_bypasses_auth() {
    let (_, api_port, _tmp) = start_server_with_api(Some("secret123".into())).await;
    let resp = reqwest::get(format!("http://127.0.0.1:{api_port}/readyz"))
        .await
        .unwrap();
    // /readyz returns 200 when ready, 503 when not. Either is fine; it must
    // NOT return 401 — that's the auth check.
    assert_ne!(resp.status(), 401);
}

#[tokio::test]
async fn http_metrics_bypasses_auth() {
    let (_, api_port, _tmp) = start_server_with_api(Some("secret123".into())).await;
    let resp = reqwest::get(format!("http://127.0.0.1:{api_port}/metrics"))
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn http_webhooks_bypass_auth() {
    // Webhooks bypass the broker-wide bearer because they carry their own
    // per-webhook auth. No matching connector → 404, NOT 401.
    let (_, api_port, _tmp) = start_server_with_api(Some("secret123".into())).await;
    let resp = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{api_port}/webhooks/nonexistent"))
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_ne!(resp.status(), 401);
}

#[tokio::test]
async fn cli_client_sends_bearer_when_env_set() {
    // This test exercises the CliClient against an authenticated server.
    // If the Authorization header is correctly set, the request succeeds.
    let (_, api_port, _tmp) = start_server_with_api(Some("cli-secret".into())).await;

    std::env::set_var("EXSPEED_AUTH_TOKEN", "cli-secret");
    // Use the same CliClient the CLI binary uses.
    let client = exspeed::cli::client::CliClient::new(&format!("http://127.0.0.1:{api_port}"));
    std::env::remove_var("EXSPEED_AUTH_TOKEN");

    // GET /api/v1/streams is the standard list-streams call (see cli/stream.rs::list).
    let result = client.get("/api/v1/streams").await;
    assert!(
        result.is_ok(),
        "expected Authorization header to authenticate: {result:?}"
    );
}
