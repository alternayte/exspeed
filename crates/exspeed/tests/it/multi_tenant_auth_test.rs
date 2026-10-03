//! End-to-end integration tests for the multi-tenant auth (Plan C).
//!
//! Each test spins up a real in-process server with a per-test
//! credentials.toml (threaded via `ServerArgs.credentials_file` so no
//! env-var state is mutated). The tests exercise both the TCP and HTTP
//! authz paths against a matrix of identities (publish-only, subscribe-only,
//! scoped admin, global admin, legacy-admin env var).

use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;

use tempfile::NamedTempFile;
use tokio::net::TcpStream;
use tokio_util::sync::CancellationToken;

use exspeed_client::{Client, ConnectOptions, ConsumerSpec, PublishRecord, SeekTo, StreamSpec};

// ---------------------------------------------------------------------------
// Type aliases
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// File helpers
// ---------------------------------------------------------------------------

/// Write a TOML credentials file to a tempfile and return the handle.
/// Keep the returned `NamedTempFile` alive for the test's lifetime — its
/// `Drop` impl removes the file from disk.
fn write_creds(contents: &str) -> NamedTempFile {
    let mut f = NamedTempFile::new().unwrap();
    f.write_all(contents.as_bytes()).unwrap();
    f.flush().unwrap();
    f
}

/// Helper wrapping the publicly-exported `exspeed_common::auth::sha256_hex`.
fn sha256_hex(raw: &str) -> String {
    exspeed_common::auth::sha256_hex(raw.as_bytes())
}

// ---------------------------------------------------------------------------
// Server harness
// ---------------------------------------------------------------------------

/// Holds the resources a running test server needs to keep alive plus the
/// shutdown token. Dropping `_tmp` removes the data dir; `cancel` stops
/// the server.
struct TestServer {
    tcp_addr: String,
    http_addr: String,
    cancel: CancellationToken,
    _tmp: tempfile::TempDir,
}

/// Start a server with the given credentials file + env token.
/// Either or both may be `None`. Blocks until `/readyz` returns 200.
async fn start_server(credentials_file: Option<PathBuf>, auth_token: Option<String>) -> TestServer {
    let tcp_port_l = exspeed_testkit::bind_local();
    let tcp_port = tcp_port_l.local_addr().unwrap().port();
    let http_port_l = exspeed_testkit::bind_local();
    let http_port = http_port_l.local_addr().unwrap().port();
    let tcp_addr = format!("127.0.0.1:{tcp_port}");
    let http_addr = format!("127.0.0.1:{http_port}");
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().to_path_buf();

    let cancel = CancellationToken::new();
    let args = exspeed::cli::server::ServerArgs {
        bind: tcp_addr.clone(),
        tcp_listener: Some(std::sync::Arc::new(tcp_port_l)),
        api_bind: http_addr.clone(),
        api_listener: Some(std::sync::Arc::new(http_port_l)),
        data_dir,
        auth_token,
        credentials_file,
        tls_cert: None,
        tls_key: None,
        ..Default::default()
    };

    let cancel_for_server = cancel.clone();
    tokio::spawn(async move {
        let shutdown_fut = async move { cancel_for_server.cancelled().await };
        exspeed::cli::server::run_with_shutdown(args, shutdown_fut)
            .await
            .ok();
    });

    // Poll TCP first (cheap sanity check), then /readyz to ensure full init.
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if TcpStream::connect(&tcp_addr).await.is_ok() {
            break;
        }
    }
    let readyz = format!("http://{http_addr}/readyz");
    for _ in 0..50 {
        if let Ok(resp) = reqwest::get(&readyz).await {
            if resp.status().is_success() {
                return TestServer {
                    tcp_addr,
                    http_addr,
                    cancel,
                    _tmp: tmp,
                };
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("server /readyz did not become ready in 5s");
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

// ---------------------------------------------------------------------------
// TCP helpers
// ---------------------------------------------------------------------------

async fn try_client(addr: &str, token: Option<&str>) -> exspeed_client::Result<Client> {
    let opts = ConnectOptions {
        token: token.map(str::to_string),
        ..Default::default()
    };
    Client::connect(addr, opts).await
}

async fn client(addr: &str, token: &str) -> Client {
    try_client(addr, Some(token)).await.expect("connect")
}

fn assert_code<T: std::fmt::Debug>(r: exspeed_client::Result<T>, expected: u16) {
    match r {
        Err(e) => assert_eq!(e.code(), Some(expected), "unexpected error: {e}"),
        Ok(v) => panic!("expected error {expected}, got Ok({v:?})"),
    }
}

/// Admin creates a stream and a consumer on it.
async fn admin_setup_stream_and_consumer(
    addr: &str,
    admin_token: &str,
    stream: &str,
    consumer: &str,
) {
    let c = client(addr, admin_token).await;
    c.create_stream(StreamSpec::named(stream)).await.unwrap();
    c.create_consumer(ConsumerSpec::new(consumer, stream))
        .await
        .unwrap();
}

// ===========================================================================
// Core authentication tests — Sub-unit 6.2
// ===========================================================================

// 1 -------------------------------------------------------------------------
#[tokio::test]
async fn tcp_connect_unknown_token_returns_401_and_closes() {
    let creds = write_creds(&format!(
        r#"
[[credentials]]
name = "known"
token_sha256 = "{}"
permissions = [{{ streams = "*", actions = ["publish"] }}]
"#,
        sha256_hex("known")
    ));
    let srv = start_server(Some(creds.path().to_path_buf()), None).await;

    // The handshake fails with 401 and the server closes the connection.
    assert_code(
        try_client(&srv.tcp_addr, Some("unknown")).await.map(|_| ()),
        401,
    );
    assert_code(try_client(&srv.tcp_addr, None).await.map(|_| ()), 401);
}

// 2 -------------------------------------------------------------------------
#[tokio::test]
async fn tcp_connect_known_token_succeeds_and_identity_attached() {
    let creds = write_creds(&format!(
        r#"
[[credentials]]
name = "orders-service"
token_sha256 = "{}"
permissions = [{{ streams = "orders-*", actions = ["publish", "admin"] }}]
"#,
        sha256_hex("tok-orders")
    ));
    let srv = start_server(Some(creds.path().to_path_buf()), None).await;

    let c = client(&srv.tcp_addr, "tok-orders").await;
    c.create_stream(StreamSpec::named("orders-placed"))
        .await
        .unwrap();
    c.publish("orders-placed", PublishRecord::new("evt", "hello"))
        .await
        .unwrap();
}

// 3 -------------------------------------------------------------------------
#[tokio::test]
async fn http_unknown_bearer_returns_401() {
    let creds = write_creds(&format!(
        r#"
[[credentials]]
name = "alice"
token_sha256 = "{}"
permissions = [{{ streams = "*", actions = ["admin"] }}]
"#,
        sha256_hex("alice-token")
    ));
    let srv = start_server(Some(creds.path().to_path_buf()), None).await;

    let resp = reqwest::Client::new()
        .get(format!("http://{}/api/v1/streams", srv.http_addr))
        .header("Authorization", "Bearer totally-wrong")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
}

// 4 -------------------------------------------------------------------------
#[tokio::test]
async fn http_known_but_non_admin_bearer_returns_403() {
    let creds = write_creds(&format!(
        r#"
[[credentials]]
name = "pubber"
token_sha256 = "{}"
permissions = [{{ streams = "*", actions = ["publish"] }}]
"#,
        sha256_hex("pub-token")
    ));
    let srv = start_server(Some(creds.path().to_path_buf()), None).await;

    let resp = reqwest::Client::new()
        .get(format!("http://{}/api/v1/streams", srv.http_addr))
        .header("Authorization", "Bearer pub-token")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 403);
}

// 5 -------------------------------------------------------------------------
#[tokio::test]
async fn http_admin_bearer_allowed() {
    let creds = write_creds(&format!(
        r#"
[[credentials]]
name = "admin"
token_sha256 = "{}"
permissions = [{{ streams = "*", actions = ["admin"] }}]
"#,
        sha256_hex("admin-token")
    ));
    let srv = start_server(Some(creds.path().to_path_buf()), None).await;

    let resp = reqwest::Client::new()
        .get(format!("http://{}/api/v1/streams", srv.http_addr))
        .header("Authorization", "Bearer admin-token")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

// 6 -------------------------------------------------------------------------
#[tokio::test]
async fn publish_denied_when_credential_lacks_publish() {
    // Credential has subscribe only. Admin pre-creates a stream via HTTP so
    // the subscriber can target a real one.
    let creds = write_creds(&format!(
        r#"
[[credentials]]
name = "sub-only"
token_sha256 = "{sub_hash}"
permissions = [{{ streams = "*", actions = ["subscribe"] }}]

[[credentials]]
name = "admin"
token_sha256 = "{admin_hash}"
permissions = [{{ streams = "*", actions = ["admin"] }}]
"#,
        sub_hash = sha256_hex("sub-token"),
        admin_hash = sha256_hex("admin-token"),
    ));
    let srv = start_server(Some(creds.path().to_path_buf()), None).await;

    admin_setup_stream_and_consumer(&srv.tcp_addr, "admin-token", "events", "c1").await;

    let c = client(&srv.tcp_addr, "sub-token").await;
    assert_code(
        c.publish("events", PublishRecord::new("evt", "x")).await,
        403,
    );
    // The connection stays usable: subscribing is allowed.
    c.subscribe("c1", 8).await.expect("subscribe allowed");
}

// 7 -------------------------------------------------------------------------
#[tokio::test]
async fn publish_allowed_on_glob_match() {
    let creds = write_creds(&format!(
        r#"
[[credentials]]
name = "orders"
token_sha256 = "{}"
permissions = [{{ streams = "orders-*", actions = ["publish", "admin"] }}]
"#,
        sha256_hex("t")
    ));
    let srv = start_server(Some(creds.path().to_path_buf()), None).await;

    let c = client(&srv.tcp_addr, "t").await;
    c.create_stream(StreamSpec::named("orders-placed"))
        .await
        .unwrap();
    c.publish("orders-placed", PublishRecord::new("evt", "x"))
        .await
        .unwrap();
}

// 8 -------------------------------------------------------------------------
#[tokio::test]
async fn publish_denied_on_glob_miss() {
    // orders-scoped cred may also create streams under the orders glob, but
    // `payments-*` is out-of-scope for both publish and admin.
    let creds = write_creds(&format!(
        r#"
[[credentials]]
name = "orders"
token_sha256 = "{orders_hash}"
permissions = [{{ streams = "orders-*", actions = ["publish"] }}]

[[credentials]]
name = "admin"
token_sha256 = "{admin_hash}"
permissions = [{{ streams = "*", actions = ["admin"] }}]
"#,
        orders_hash = sha256_hex("orders-tok"),
        admin_hash = sha256_hex("admin-tok"),
    ));
    let srv = start_server(Some(creds.path().to_path_buf()), None).await;

    client(&srv.tcp_addr, "admin-tok")
        .await
        .create_stream(StreamSpec::named("payments-received"))
        .await
        .unwrap();
    let c = client(&srv.tcp_addr, "orders-tok").await;
    assert_code(
        c.publish("payments-received", PublishRecord::new("evt", "x"))
            .await,
        403,
    );
}

// 9 -------------------------------------------------------------------------
#[tokio::test]
async fn subscribe_denied_when_credential_lacks_subscribe() {
    let creds = write_creds(&format!(
        r#"
[[credentials]]
name = "pub-only"
token_sha256 = "{pub_hash}"
permissions = [{{ streams = "*", actions = ["publish"] }}]

[[credentials]]
name = "admin"
token_sha256 = "{admin_hash}"
permissions = [{{ streams = "*", actions = ["admin"] }}]
"#,
        pub_hash = sha256_hex("pub-tok"),
        admin_hash = sha256_hex("admin-tok"),
    ));
    let srv = start_server(Some(creds.path().to_path_buf()), None).await;

    admin_setup_stream_and_consumer(&srv.tcp_addr, "admin-tok", "events", "c1").await;
    let c = client(&srv.tcp_addr, "pub-tok").await;
    assert_code(c.subscribe("c1", 8).await.map(|s| s.id()), 403);
}

// 10 ------------------------------------------------------------------------
#[tokio::test]
async fn subscribe_allowed_delivers_records() {
    let creds = write_creds(&format!(
        r#"
[[credentials]]
name = "sub"
token_sha256 = "{sub_hash}"
permissions = [{{ streams = "*", actions = ["subscribe"] }}]

[[credentials]]
name = "admin"
token_sha256 = "{admin_hash}"
permissions = [{{ streams = "*", actions = ["admin", "publish"] }}]
"#,
        sub_hash = sha256_hex("sub-tok"),
        admin_hash = sha256_hex("admin-tok"),
    ));
    let srv = start_server(Some(creds.path().to_path_buf()), None).await;

    {
        let admin = client(&srv.tcp_addr, "admin-tok").await;
        admin
            .create_stream(StreamSpec::named("events"))
            .await
            .unwrap();
        admin
            .create_consumer(ConsumerSpec::new("c1", "events"))
            .await
            .unwrap();
        admin
            .publish("events", PublishRecord::new("evt", "payload-1"))
            .await
            .unwrap();
    }
    let c = client(&srv.tcp_addr, "sub-tok").await;
    let mut sub = c.subscribe("c1", 8).await.unwrap();
    let msg = sub
        .next_timeout(Duration::from_secs(5))
        .await
        .expect("record delivered");
    assert_eq!(msg.record.value.as_ref(), b"payload-1");
    msg.ack().await.unwrap();
}

// 11 ------------------------------------------------------------------------
#[tokio::test]
async fn read_pull_ack_nack_seek_all_require_subscribe_verb() {
    // pub-only cred — every subscribe-scoped op should 403.
    let creds = write_creds(&format!(
        r#"
[[credentials]]
name = "pub-only"
token_sha256 = "{pub_hash}"
permissions = [{{ streams = "*", actions = ["publish"] }}]

[[credentials]]
name = "admin"
token_sha256 = "{admin_hash}"
permissions = [{{ streams = "*", actions = ["admin"] }}]
"#,
        pub_hash = sha256_hex("pub-tok"),
        admin_hash = sha256_hex("admin-tok"),
    ));
    let srv = start_server(Some(creds.path().to_path_buf()), None).await;

    admin_setup_stream_and_consumer(&srv.tcp_addr, "admin-tok", "events", "c1").await;
    let c = client(&srv.tcp_addr, "pub-tok").await;
    assert_code(
        c.read("events", 0, 10, Duration::ZERO, "")
            .await
            .map(|r| r.records.len()),
        403,
    );
    assert_code(c.pull("c1", 10, Duration::ZERO).await, 403);
    assert_code(c.seek("c1", SeekTo::Earliest).await, 403);
    assert_code(c.ack("c1", vec![0]).await, 403);
    assert_code(c.nack("c1", 0, Duration::ZERO).await, 403);
    assert_code(c.consumer_info("c1").await, 403);
    // Creating a consumer needs subscribe on its stream too.
    assert_code(
        c.create_consumer(ConsumerSpec::new("c2", "events")).await,
        403,
    );
}

// 12 ------------------------------------------------------------------------
#[tokio::test]
async fn create_stream_requires_scoped_admin() {
    let creds = write_creds(&format!(
        r#"
[[credentials]]
name = "team-a"
token_sha256 = "{}"
permissions = [{{ streams = "team-a-*", actions = ["admin"] }}]
"#,
        sha256_hex("team-a-tok")
    ));
    let srv = start_server(Some(creds.path().to_path_buf()), None).await;

    let c = client(&srv.tcp_addr, "team-a-tok").await;
    c.create_stream(StreamSpec::named("team-a-orders"))
        .await
        .unwrap();
    assert_code(
        c.create_stream(StreamSpec::named("team-b-orders")).await,
        403,
    );
}

// 13 ------------------------------------------------------------------------
#[tokio::test]
async fn delete_stream_requires_scoped_admin() {
    let creds = write_creds(&format!(
        r#"
[[credentials]]
name = "team-a"
token_sha256 = "{a}"
permissions = [{{ streams = "team-a-*", actions = ["admin"] }}]

[[credentials]]
name = "admin"
token_sha256 = "{admin}"
permissions = [{{ streams = "*", actions = ["admin"] }}]
"#,
        a = sha256_hex("team-a-tok"),
        admin = sha256_hex("admin-tok"),
    ));
    let srv = start_server(Some(creds.path().to_path_buf()), None).await;
    let admin = client(&srv.tcp_addr, "admin-tok").await;
    admin
        .create_stream(StreamSpec::named("team-b-x"))
        .await
        .unwrap();

    let c = client(&srv.tcp_addr, "team-a-tok").await;
    c.create_stream(StreamSpec::named("team-a-x"))
        .await
        .unwrap();
    c.delete_stream("team-a-x").await.unwrap();
    assert_code(c.delete_stream("team-b-x").await, 403);
}

// 14 ------------------------------------------------------------------------
#[tokio::test]
async fn list_streams_only_shows_permitted_streams() {
    let creds = write_creds(&format!(
        r#"
[[credentials]]
name = "orders"
token_sha256 = "{o}"
permissions = [{{ streams = "orders-*", actions = ["publish"] }}]

[[credentials]]
name = "admin"
token_sha256 = "{admin}"
permissions = [{{ streams = "*", actions = ["admin"] }}]
"#,
        o = sha256_hex("orders-tok"),
        admin = sha256_hex("admin-tok"),
    ));
    let srv = start_server(Some(creds.path().to_path_buf()), None).await;
    let admin = client(&srv.tcp_addr, "admin-tok").await;
    admin
        .create_stream(StreamSpec::named("orders-a"))
        .await
        .unwrap();
    admin
        .create_stream(StreamSpec::named("payments-a"))
        .await
        .unwrap();

    let c = client(&srv.tcp_addr, "orders-tok").await;
    let names: Vec<String> = c
        .list_streams()
        .await
        .unwrap()
        .iter()
        .map(|s| s["name"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(names, vec!["orders-a".to_string()]);
}

// 15 ------------------------------------------------------------------------
#[tokio::test]
async fn http_delete_stream_scoped_admin_enforced() {
    let creds = write_creds(&format!(
        r#"
[[credentials]]
name = "team-a"
token_sha256 = "{a}"
permissions = [{{ streams = "team-a-*", actions = ["admin"] }}]

[[credentials]]
name = "admin"
token_sha256 = "{admin}"
permissions = [{{ streams = "*", actions = ["admin"] }}]
"#,
        a = sha256_hex("team-a-tok"),
        admin = sha256_hex("admin-tok"),
    ));
    let srv = start_server(Some(creds.path().to_path_buf()), None).await;
    client(&srv.tcp_addr, "admin-tok")
        .await
        .create_stream(StreamSpec::named("team-b-x"))
        .await
        .unwrap();
    let resp = reqwest::Client::new()
        .delete(format!("http://{}/api/v1/streams/team-b-x", srv.http_addr))
        .header("Authorization", "Bearer team-a-tok")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 403);
}

// 16 ------------------------------------------------------------------------
#[tokio::test]
async fn http_create_connector_requires_global_admin() {
    let creds = write_creds(&format!(
        r#"
[[credentials]]
name = "team-a"
token_sha256 = "{scoped_hash}"
permissions = [{{ streams = "team-a-*", actions = ["admin"] }}]

[[credentials]]
name = "global"
token_sha256 = "{global_hash}"
permissions = [{{ streams = "*", actions = ["admin"] }}]
"#,
        scoped_hash = sha256_hex("scoped-tok"),
        global_hash = sha256_hex("global-tok"),
    ));
    let srv = start_server(Some(creds.path().to_path_buf()), None).await;

    let http = reqwest::Client::new();
    // Minimum fields that satisfy the `ConnectorConfig` deserializer so the
    // request reaches the handler (and thus the authz gate) rather than
    // being rejected at the Json extractor with 422.
    let body = serde_json::json!({
        "name": "probe",
        "type": "source",
        "plugin": "nonexistent-plugin",
        "stream": "probe-stream",
    });

    // Scoped admin → 403 from the global-admin gate.
    let r = http
        .post(format!("http://{}/api/v1/connectors", srv.http_addr))
        .header("Authorization", "Bearer scoped-tok")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 403);

    // Global admin → authz passes; the handler may reject on the config's
    // merits. Accept anything that isn't 401/403 as "passed authz".
    let r = http
        .post(format!("http://{}/api/v1/connectors", srv.http_addr))
        .header("Authorization", "Bearer global-tok")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_ne!(r.status(), 401, "global admin should not be unauthorized");
    assert_ne!(r.status(), 403, "global admin should clear the authz gate");
}

// 17 ------------------------------------------------------------------------
#[tokio::test]
async fn http_create_query_requires_global_admin() {
    let creds = write_creds(&format!(
        r#"
[[credentials]]
name = "team-a"
token_sha256 = "{scoped_hash}"
permissions = [{{ streams = "team-a-*", actions = ["admin"] }}]

[[credentials]]
name = "global"
token_sha256 = "{global_hash}"
permissions = [{{ streams = "*", actions = ["admin"] }}]
"#,
        scoped_hash = sha256_hex("scoped-tok"),
        global_hash = sha256_hex("global-tok"),
    ));
    let srv = start_server(Some(creds.path().to_path_buf()), None).await;

    let http = reqwest::Client::new();
    // A deliberately invalid SQL payload: we only care that authz is
    // evaluated before the query parses.
    let body = serde_json::json!({ "sql": "SELECT 1 FROM no_such_stream" });

    let r = http
        .post(format!("http://{}/api/v1/queries", srv.http_addr))
        .header("Authorization", "Bearer scoped-tok")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 403);

    let r = http
        .post(format!("http://{}/api/v1/queries", srv.http_addr))
        .header("Authorization", "Bearer global-tok")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_ne!(r.status(), 401);
    assert_ne!(r.status(), 403);
}

// 18 ------------------------------------------------------------------------
#[tokio::test]
async fn http_whoami_returns_identity_for_any_authenticated_client() {
    let creds = write_creds(&format!(
        r#"
[[credentials]]
name = "pub-only"
token_sha256 = "{}"
permissions = [{{ streams = "orders-*", actions = ["publish"] }}]
"#,
        sha256_hex("pub-tok")
    ));
    let srv = start_server(Some(creds.path().to_path_buf()), None).await;

    let r = reqwest::Client::new()
        .get(format!("http://{}/api/v1/whoami", srv.http_addr))
        .header("Authorization", "Bearer pub-tok")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let body: serde_json::Value = r.json().await.unwrap();
    assert_eq!(body["name"], "pub-only");
    assert!(
        body["permissions"].is_array(),
        "permissions must be an array"
    );
    let perms = body["permissions"].as_array().unwrap();
    assert_eq!(perms.len(), 1);
    assert_eq!(perms[0]["streams"], "orders-*");
    assert_eq!(perms[0]["actions"], serde_json::json!(["publish"]));
}

// 19 ------------------------------------------------------------------------
#[tokio::test]
async fn env_var_only_injects_legacy_admin_with_full_permissions() {
    // No credentials file; just the env token. Server synthesizes a
    // `legacy-admin` credential with `*` + all verbs.
    let srv = start_server(None, Some("legacy-tok".into())).await;

    let c = client(&srv.tcp_addr, "legacy-tok").await;
    c.create_stream(StreamSpec::named("s")).await.unwrap();
    c.publish("s", PublishRecord::new("e", "v")).await.unwrap();

    // HTTP whoami shows `legacy-admin`.
    let r = reqwest::Client::new()
        .get(format!("http://{}/api/v1/whoami", srv.http_addr))
        .header("Authorization", "Bearer legacy-tok")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let body: serde_json::Value = r.json().await.unwrap();
    assert_eq!(body["name"], "legacy-admin");
}

// 20 ------------------------------------------------------------------------
#[tokio::test]
async fn env_var_plus_toml_both_work() {
    let creds = write_creds(&format!(
        r#"
[[credentials]]
name = "orders"
token_sha256 = "{}"
permissions = [{{ streams = "*", actions = ["publish", "admin"] }}]
"#,
        sha256_hex("orders-tok")
    ));
    let srv = start_server(Some(creds.path().to_path_buf()), Some("legacy-tok".into())).await;

    let a = client(&srv.tcp_addr, "legacy-tok").await;
    a.create_stream(StreamSpec::named("shared")).await.unwrap();
    a.publish("shared", PublishRecord::new("e", "A"))
        .await
        .unwrap();

    let b = client(&srv.tcp_addr, "orders-tok").await;
    b.publish("shared", PublishRecord::new("e", "B"))
        .await
        .unwrap();
}

// 21 ------------------------------------------------------------------------
#[tokio::test]
async fn env_var_plus_toml_with_name_collision_refuses_to_start() {
    // Library-level assertion per the plan — faster + more deterministic
    // than wrangling a server startup error.
    use exspeed_common::auth::{AuthError, CredentialStore};

    let creds = write_creds(&format!(
        r#"
[[credentials]]
name = "legacy-admin"
token_sha256 = "{}"
permissions = [{{ streams = "*", actions = ["admin"] }}]
"#,
        sha256_hex("x")
    ));
    let err = CredentialStore::build(Some(creds.path()), Some("legacy-tok")).unwrap_err();
    assert!(matches!(err, AuthError::LegacyAdminReserved));
}

// 22 ------------------------------------------------------------------------
#[tokio::test]
async fn no_auth_configured_is_open() {
    // No file, no env token → open broker (Plan B default).
    let srv = start_server(None, None).await;

    let c = try_client(&srv.tcp_addr, None).await.unwrap();
    c.create_stream(StreamSpec::named("open")).await.unwrap();
    c.publish("open", PublishRecord::new("e", "v"))
        .await
        .unwrap();

    // HTTP: no bearer → 200 on admin endpoints.
    let r = reqwest::Client::new()
        .get(format!("http://{}/api/v1/streams", srv.http_addr))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
}

// ===========================================================================
// File-loading regression tests — Sub-unit 6.3
// ===========================================================================

// 23 ------------------------------------------------------------------------
#[tokio::test]
async fn malformed_toml_refuses_to_start_with_line_number() {
    use exspeed_common::auth::{AuthError, CredentialStore};
    // Unterminated string → TOML parse error.
    let creds = write_creds(
        r#"
[[credentials]]
name = "a
token_sha256 = "abc"
"#,
    );
    let err = CredentialStore::build(Some(creds.path()), None).unwrap_err();
    assert!(
        matches!(err, AuthError::Toml(_)),
        "expected AuthError::Toml, got {err:?}"
    );
    let msg = format!("{err}");
    assert!(
        msg.to_lowercase().contains("toml") || msg.to_lowercase().contains("parse"),
        "error message missing TOML/parse context: {msg}"
    );
}

// 24 ------------------------------------------------------------------------
#[tokio::test]
async fn missing_required_field_refuses_to_start() {
    use exspeed_common::auth::CredentialStore;
    // Missing `token_sha256` → serde-driven parse error (surfaces as Toml).
    let creds = write_creds(
        r#"
[[credentials]]
name = "a"
"#,
    );
    let err = CredentialStore::build(Some(creds.path()), None).unwrap_err();
    // toml::de::Error is what serde returns for a missing required field.
    let msg = format!("{err}");
    assert!(
        msg.to_lowercase().contains("token_sha256") || msg.to_lowercase().contains("missing"),
        "error should name the missing field: {msg}"
    );
}

// 25 ------------------------------------------------------------------------
#[tokio::test]
async fn unknown_action_refuses_to_start() {
    use exspeed_common::auth::{AuthError, CredentialStore};
    let creds = write_creds(&format!(
        r#"
[[credentials]]
name = "a"
token_sha256 = "{}"
permissions = [{{ streams = "*", actions = ["manage"] }}]
"#,
        sha256_hex("x")
    ));
    let err = CredentialStore::build(Some(creds.path()), None).unwrap_err();
    match err {
        AuthError::UnknownAction { action, .. } => assert_eq!(action, "manage"),
        other => panic!("expected UnknownAction, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// `exspeed auth` CLI subcommand tests (Task 7).
//
// These spawn the `exspeed` binary via `CARGO_BIN_EXE_exspeed` — cargo
// rebuilds the binary on demand so the tests always run against the
// just-compiled code.
// ---------------------------------------------------------------------------

// 26 ------------------------------------------------------------------------
#[test]
fn exspeed_auth_hash_from_stdin_matches_sha256_reference() {
    use std::io::Write;
    use std::process::{Command, Stdio};

    let mut child = Command::new(env!("CARGO_BIN_EXE_exspeed"))
        .args(["auth", "hash"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.as_mut().unwrap().write_all(b"hello").unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "auth hash failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    // Known SHA-256 of "hello".
    assert_eq!(
        stdout.trim(),
        "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
    );
}

// 27 ------------------------------------------------------------------------
#[test]
fn exspeed_auth_gen_token_outputs_64_hex_and_matching_hash() {
    use std::process::Command;

    let output = Command::new(env!("CARGO_BIN_EXE_exspeed"))
        .args(["auth", "gen-token"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "auth gen-token failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let token = String::from_utf8(output.stdout).unwrap().trim().to_string();
    // stderr may carry unrelated tracing log lines (e.g. when
    // EXSPEED_INSECURE_SKIP_VERIFY is set). Only the last non-empty line
    // is the hash we emit.
    let stderr = String::from_utf8(output.stderr).unwrap();
    let hash = stderr
        .lines()
        .rfind(|l| !l.trim().is_empty())
        .unwrap_or("")
        .trim()
        .to_string();

    assert_eq!(token.len(), 64, "token should be 64 hex chars: {token}");
    assert!(
        token.chars().all(|c| c.is_ascii_hexdigit()),
        "token should be hex: {token}"
    );
    let expected = exspeed_common::auth::sha256_hex(token.as_bytes());
    assert_eq!(hash, expected, "stderr hash should be sha256(token)");
}

// 28 ------------------------------------------------------------------------
#[test]
fn exspeed_auth_lint_on_valid_file_exits_0() {
    use std::process::Command;

    let file = write_creds(&format!(
        r#"[[credentials]]
name = "a"
token_sha256 = "{}"
permissions = [{{ streams = "*", actions = ["admin"] }}]
"#,
        sha256_hex("t")
    ));
    let status = Command::new(env!("CARGO_BIN_EXE_exspeed"))
        .args(["auth", "lint"])
        .arg(file.path())
        .status()
        .unwrap();
    assert!(status.success(), "auth lint on valid file should exit 0");
}

// 29 ------------------------------------------------------------------------
#[test]
fn exspeed_auth_lint_on_duplicate_name_exits_nonzero() {
    use std::process::Command;

    let file = write_creds(&format!(
        r#"[[credentials]]
name = "dup"
token_sha256 = "{}"

[[credentials]]
name = "dup"
token_sha256 = "{}"
"#,
        sha256_hex("a"),
        sha256_hex("b"),
    ));
    let status = Command::new(env!("CARGO_BIN_EXE_exspeed"))
        .args(["auth", "lint"])
        .arg(file.path())
        .status()
        .unwrap();
    assert!(
        !status.success(),
        "auth lint on duplicate-name file should exit non-zero"
    );
}

// Regression (REVIEW blocker 10) ---------------------------------------------
#[tokio::test]
async fn tcp_query_requires_global_admin() {
    let creds = write_creds(&format!(
        r#"
[[credentials]]
name = "scoped"
token_sha256 = "{scoped_hash}"
permissions = [{{ streams = "orders-*", actions = ["publish", "subscribe", "admin"] }}]

[[credentials]]
name = "admin"
token_sha256 = "{admin_hash}"
permissions = [{{ streams = "*", actions = ["admin", "publish", "subscribe"] }}]
"#,
        scoped_hash = sha256_hex("scoped-tok"),
        admin_hash = sha256_hex("admin-tok"),
    ));
    let srv = start_server(Some(creds.path().to_path_buf()), None).await;

    admin_setup_stream_and_consumer(&srv.tcp_addr, "admin-tok", "payments", "pc").await;

    let scoped = client(&srv.tcp_addr, "scoped-tok").await;
    assert_code(scoped.query("SELECT * FROM payments").await, 403);
    let admin = client(&srv.tcp_addr, "admin-tok").await;
    admin.query("SELECT * FROM payments").await.unwrap();
}

// Regression (REVIEW blocker 10) ---------------------------------------------
#[tokio::test]
async fn consumer_ops_are_scoped_to_the_consumers_stream() {
    let creds = write_creds(&format!(
        r#"
[[credentials]]
name = "orders"
token_sha256 = "{orders_hash}"
permissions = [{{ streams = "orders-*", actions = ["publish", "subscribe"] }}]

[[credentials]]
name = "admin"
token_sha256 = "{admin_hash}"
permissions = [{{ streams = "*", actions = ["admin", "publish", "subscribe"] }}]
"#,
        orders_hash = sha256_hex("orders-tok"),
        admin_hash = sha256_hex("admin-tok"),
    ));
    let srv = start_server(Some(creds.path().to_path_buf()), None).await;
    admin_setup_stream_and_consumer(&srv.tcp_addr, "admin-tok", "orders-x", "mine").await;
    admin_setup_stream_and_consumer(&srv.tcp_addr, "admin-tok", "payments-x", "victim").await;

    let c = client(&srv.tcp_addr, "orders-tok").await;
    c.subscribe("mine", 8).await.unwrap();
    // Acting on a consumer of a stream outside the credential's scope is
    // forbidden, whatever the offset.
    assert_code(c.ack("victim", vec![1_000]).await, 403);
    assert_code(c.nack("victim", 0, Duration::ZERO).await, 403);
    assert_code(c.subscribe("victim", 8).await.map(|s| s.id()), 403);
    assert_code(c.delete_consumer("victim").await, 403);
    // In scope: fine.
    c.ack("mine", vec![0]).await.unwrap();
    // A dead-letter stream outside the publish scope is rejected.
    assert_code(
        c.create_consumer(ConsumerSpec {
            dlq_stream: Some("payments-x".into()),
            ..ConsumerSpec::new("sneaky", "orders-x")
        })
        .await,
        403,
    );
}

#[tokio::test]
async fn http_list_streams_is_scoped_to_the_caller() {
    let creds = write_creds(&format!(
        r#"
[[credentials]]
name = "root"
token_sha256 = "{}"
permissions = [{{ streams = "*", actions = ["admin"] }}]

[[credentials]]
name = "tenant-a"
token_sha256 = "{}"
permissions = [{{ streams = "a-*", actions = ["admin"] }}, {{ streams = "shared", actions = ["subscribe"] }}]
"#,
        sha256_hex("root-token"),
        sha256_hex("a-token"),
    ));
    let srv = start_server(Some(creds.path().to_path_buf()), None).await;
    let root = try_client(&srv.tcp_addr, Some("root-token")).await.unwrap();
    for s in ["a-1", "b-1", "shared"] {
        root.create_stream(StreamSpec::named(s)).await.unwrap();
    }
    // Creates the internal `__consumers` stream.
    root.create_consumer(ConsumerSpec::new("w", "b-1"))
        .await
        .unwrap();
    let list = |token: &'static str, query: &'static str| {
        let url = format!("http://{}/api/v1/streams{query}", srv.http_addr);
        async move {
            let v: serde_json::Value = reqwest::Client::new()
                .get(url)
                .bearer_auth(token)
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            v.as_array()
                .unwrap()
                .iter()
                .map(|s| s["name"].as_str().unwrap().to_string())
                .collect::<Vec<_>>()
        }
    };
    assert_eq!(list("a-token", "").await, vec!["a-1", "shared"]);
    assert_eq!(
        list("a-token", "?internal=true").await,
        vec!["a-1", "shared"],
        "internal streams are for global admins only"
    );
    assert_eq!(list("root-token", "").await, vec!["a-1", "b-1", "shared"]);
    let all = list("root-token", "?internal=true").await;
    assert!(all.iter().any(|n| n.starts_with("__")), "{all:?}");
}

#[tokio::test]
async fn http_records_long_poll_works_for_subscribe_only_credentials() {
    let creds = write_creds(&format!(
        r#"
[[credentials]]
name = "root"
token_sha256 = "{}"
permissions = [{{ streams = "*", actions = ["admin", "publish"] }}]

[[credentials]]
name = "reader"
token_sha256 = "{}"
permissions = [{{ streams = "logs", actions = ["subscribe"] }}]
"#,
        sha256_hex("root-token"),
        sha256_hex("reader-token"),
    ));
    let srv = start_server(Some(creds.path().to_path_buf()), None).await;
    let root = try_client(&srv.tcp_addr, Some("root-token")).await.unwrap();
    root.create_stream(StreamSpec::named("logs")).await.unwrap();
    root.create_stream(StreamSpec::named("secret"))
        .await
        .unwrap();
    let http = reqwest::Client::new();
    let url = |p: &str| format!("http://{}{p}", srv.http_addr);

    // No permission on `secret`; and the admin routes stay closed.
    let r = http
        .get(url("/api/v1/streams/secret/records"))
        .bearer_auth("reader-token")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 403);
    let r = http
        .get(url("/api/v1/streams/logs"))
        .bearer_auth("reader-token")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 403);

    // A caught-up read waits for the next record instead of returning empty.
    let started = std::time::Instant::now();
    let req = http
        .get(url("/api/v1/streams/logs/records?from=0&wait_ms=10000"))
        .bearer_auth("reader-token")
        .send();
    let publish = async {
        tokio::time::sleep(Duration::from_millis(300)).await;
        root.publish("logs", PublishRecord::new("logs.x", r#"{"n":1}"#))
            .await
            .unwrap();
    };
    let (resp, ()) = tokio::join!(req, publish);
    let resp = resp.unwrap();
    assert_eq!(resp.status(), 200);
    let page: serde_json::Value = resp.json().await.unwrap();
    let elapsed = started.elapsed();
    assert_eq!(page["records"].as_array().unwrap().len(), 1, "{page}");
    assert_eq!(page["next_offset"], 1);
    assert!(
        elapsed >= Duration::from_millis(250),
        "returned before the publish: {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_secs(5),
        "waited past the publish: {elapsed:?}"
    );

    // With nothing new, the wait ends at wait_ms with an empty page.
    let started = std::time::Instant::now();
    let page: serde_json::Value = http
        .get(url("/api/v1/streams/logs/records?from=1&wait_ms=300"))
        .bearer_auth("reader-token")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(page["records"].as_array().unwrap().is_empty());
    assert!(started.elapsed() >= Duration::from_millis(250));
}
