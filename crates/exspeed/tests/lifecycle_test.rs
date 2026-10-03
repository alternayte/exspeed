use std::path::PathBuf;
use tempfile::tempdir;

#[test]
fn second_server_open_on_same_data_dir_fails() {
    let tmp = tempdir().unwrap();
    let data_dir: PathBuf = tmp.path().to_path_buf();

    let _lock1 = exspeed::cli::server_lock::acquire_data_dir_lock(&data_dir)
        .expect("first lock should succeed");

    let err = exspeed::cli::server_lock::acquire_data_dir_lock(&data_dir)
        .expect_err("second lock should be rejected");

    let msg = format!("{err:#}");
    assert!(
        msg.contains("already in use") || msg.contains("locked"),
        "unexpected error: {msg}"
    );
}

use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;

async fn start_test_server(max_conns: u32) -> (String, tempfile::TempDir) {
    let port = exspeed_testkit::pick_unused_port().unwrap();
    let api_port = exspeed_testkit::pick_unused_port().unwrap();
    let bind = format!("127.0.0.1:{port}");
    let api_bind = format!("127.0.0.1:{api_port}");
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().to_path_buf();

    let bind_clone = bind.clone();
    let api_clone = api_bind.clone();
    let data_clone = data_dir.clone();
    tokio::spawn(async move {
        exspeed::cli::server::run(exspeed::cli::server::ServerArgs {
            bind: bind_clone,
            api_bind: api_clone,
            data_dir: data_clone,
            auth_token: None,
            credentials_file: None,
            tls_cert: None,
            tls_key: None,
            max_connections: max_conns as usize,
            ..Default::default()
        })
        .await
        .unwrap();
    });

    tokio::time::sleep(Duration::from_millis(200)).await;
    (bind, tmp)
}

#[tokio::test]
async fn connection_cap_rejects_overflow() {
    let (addr, _tmp) = start_test_server(2).await;

    let c1 = TcpStream::connect(&addr).await.unwrap();
    let c2 = TcpStream::connect(&addr).await.unwrap();

    // 3rd connection: server should drop it; we detect via EOF within a short window.
    let mut c3 = TcpStream::connect(&addr).await.unwrap();
    let mut buf = [0u8; 1];
    let read = tokio::time::timeout(Duration::from_secs(2), c3.read(&mut buf)).await;

    match read {
        Ok(Ok(0)) => { /* expected: server closed the socket */ }
        Ok(Ok(_n)) => panic!("expected EOF when over connection cap, got data"),
        Ok(Err(_)) => { /* connection reset, also acceptable */ }
        Err(_) => panic!("expected server to drop overflow connection within 2s"),
    }

    drop(c1);
    drop(c2);
}

use tokio::sync::oneshot;

#[tokio::test]
async fn sigterm_signal_token_stops_accept_loop() {
    let port = exspeed_testkit::pick_unused_port().unwrap();
    let api_port = exspeed_testkit::pick_unused_port().unwrap();
    let bind = format!("127.0.0.1:{port}");
    let api_bind = format!("127.0.0.1:{api_port}");
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().to_path_buf();

    let (tx, rx) = oneshot::channel::<()>();
    let server_fut = tokio::spawn(async move {
        exspeed::cli::server::run_with_shutdown(
            exspeed::cli::server::ServerArgs {
                bind,
                api_bind,
                data_dir,
                auth_token: None,
                credentials_file: None,
                tls_cert: None,
                tls_key: None,
                ..Default::default()
            },
            async {
                let _ = rx.await;
            },
        )
        .await
    });

    // Give the server a moment to bind the listener.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Open a connection (with a live subscription), then trigger shutdown:
    // the server must not wait on idle clients beyond the drain deadline.
    let client = exspeed_client::Client::connect(
        &format!("127.0.0.1:{port}"),
        exspeed_client::ConnectOptions::default(),
    )
    .await
    .unwrap();
    client.ping().await.unwrap();

    let _ = tx.send(());

    let result = tokio::time::timeout(Duration::from_secs(15), server_fut).await;
    let outer = result.expect("server should exit within 15s");
    let inner = outer.expect("server task should not panic");
    inner.expect("server should return Ok on graceful shutdown");
    // The client sees the connection close.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(client.ping().await.is_err());
}

#[tokio::test]
async fn readyz_returns_503_when_data_dir_unwritable() {
    let port = exspeed_testkit::pick_unused_port().unwrap();
    let api_port = exspeed_testkit::pick_unused_port().unwrap();
    let bind = format!("127.0.0.1:{port}");
    let api_bind = format!("127.0.0.1:{api_port}");
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().to_path_buf();
    let data_for_chmod = data_dir.clone();

    tokio::spawn(async move {
        exspeed::cli::server::run(exspeed::cli::server::ServerArgs {
            bind,
            api_bind,
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

    tokio::time::sleep(Duration::from_millis(300)).await;

    let url = format!("http://127.0.0.1:{api_port}/readyz");
    let ok = reqwest::get(&url).await.unwrap();
    assert_eq!(ok.status(), 200, "should be ready after startup");

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&data_for_chmod, std::fs::Permissions::from_mode(0o555)).unwrap();
        // Root ignores directory permissions; the probe can't be made to fail.
        if std::fs::write(data_for_chmod.join(".probe-as-root"), b"x").is_ok() {
            eprintln!("skipping: running as root, chmod does not block writes");
            return;
        }
    }

    let bad = reqwest::get(&url).await.unwrap();
    assert_eq!(
        bad.status(),
        503,
        "should be unready when data_dir is unwritable"
    );

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&data_for_chmod, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
}

// ---------------------------------------------------------------------------
// Startup failures are returned, not swallowed
// ---------------------------------------------------------------------------

fn bound() -> std::sync::Arc<std::net::TcpListener> {
    std::sync::Arc::new(std::net::TcpListener::bind("127.0.0.1:0").unwrap())
}

/// Run a server that is expected to fail at startup; returns the error.
/// Meanwhile polls `/readyz` on `api_addr` and asserts it never says ready.
async fn expect_startup_error(
    args: exspeed::cli::server::ServerArgs,
    api_addr: Option<std::net::SocketAddr>,
) -> String {
    let server = tokio::spawn(exspeed::cli::server::run_with_shutdown(
        args,
        std::future::pending(),
    ));
    let poll = async {
        if let Some(addr) = api_addr {
            let http = reqwest::Client::new();
            loop {
                if let Ok(r) = http.get(format!("http://{addr}/readyz")).send().await {
                    assert_ne!(
                        r.status(),
                        200,
                        "/readyz said ready during a failed startup"
                    );
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        } else {
            std::future::pending::<()>().await
        }
    };
    let res = tokio::select! {
        r = server => r,
        _ = poll => unreachable!(),
    };
    let err = res
        .expect("server task panicked")
        .expect_err("startup should fail");
    format!("{err:#}")
}

#[tokio::test]
async fn occupied_api_port_fails_startup() {
    let tmp = tempfile::tempdir().unwrap();
    let busy = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let args = exspeed::cli::server::ServerArgs {
        tcp_listener: Some(bound()),
        api_bind: busy.local_addr().unwrap().to_string(),
        ..exspeed::cli::server::ServerArgs::new(tmp.path())
    };
    let msg = tokio::time::timeout(Duration::from_secs(10), expect_startup_error(args, None))
        .await
        .expect("startup error is returned promptly");
    assert!(msg.contains("HTTP API"), "{msg}");

    // Nothing was left holding the data dir: a server with free ports starts.
    let api = bound();
    let api_addr = api.local_addr().unwrap();
    let (tx, rx) = oneshot::channel::<()>();
    let args = exspeed::cli::server::ServerArgs {
        tcp_listener: Some(bound()),
        api_listener: Some(api),
        ..exspeed::cli::server::ServerArgs::new(tmp.path())
    };
    let h = tokio::spawn(exspeed::cli::server::run_with_shutdown(args, async {
        let _ = rx.await;
    }));
    let url = format!("http://{api_addr}/readyz");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if matches!(reqwest::get(&url).await, Ok(r) if r.status() == 200) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "second start never got ready"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let _ = tx.send(());
    h.await.unwrap().unwrap();
}

#[tokio::test]
async fn occupied_tcp_port_fails_startup() {
    let tmp = tempfile::tempdir().unwrap();
    let busy = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let args = exspeed::cli::server::ServerArgs {
        bind: busy.local_addr().unwrap().to_string(),
        api_listener: Some(bound()),
        ..exspeed::cli::server::ServerArgs::new(tmp.path())
    };
    let msg = tokio::time::timeout(Duration::from_secs(10), expect_startup_error(args, None))
        .await
        .expect("startup error is returned promptly");
    assert!(msg.contains("client (TCP)"), "{msg}");
}

#[tokio::test]
async fn unreadable_connector_catalog_fails_startup_and_never_reports_ready() {
    let tmp = tempfile::tempdir().unwrap();
    // `connectors.d` is a file, so the catalog can't be read.
    std::fs::write(tmp.path().join("connectors.d"), b"not a directory").unwrap();
    let api = bound();
    let api_addr = api.local_addr().unwrap();
    let args = exspeed::cli::server::ServerArgs {
        tcp_listener: Some(bound()),
        api_listener: Some(api),
        ..exspeed::cli::server::ServerArgs::new(tmp.path())
    };
    let msg = tokio::time::timeout(
        Duration::from_secs(10),
        expect_startup_error(args, Some(api_addr)),
    )
    .await
    .expect("startup error is returned promptly");
    assert!(msg.contains("connector"), "{msg}");
}

#[tokio::test]
async fn bad_tls_files_fail_startup_and_never_report_ready() {
    let tmp = tempfile::tempdir().unwrap();
    let cert = tmp.path().join("tls.crt");
    let key = tmp.path().join("tls.key");
    std::fs::write(&cert, b"not a certificate").unwrap();
    std::fs::write(&key, b"not a key").unwrap();
    let api = bound();
    let api_addr = api.local_addr().unwrap();
    let args = exspeed::cli::server::ServerArgs {
        tcp_listener: Some(bound()),
        api_listener: Some(api),
        tls_cert: Some(cert),
        tls_key: Some(key),
        ..exspeed::cli::server::ServerArgs::new(tmp.path().join("data"))
    };
    tokio::time::timeout(
        Duration::from_secs(10),
        expect_startup_error(args, Some(api_addr)),
    )
    .await
    .expect("startup error is returned promptly");
}

#[tokio::test]
async fn shutdown_waits_for_the_http_api() {
    let tmp = tempfile::tempdir().unwrap();
    let api = bound();
    let api_addr = api.local_addr().unwrap();
    let (tx, rx) = oneshot::channel::<()>();
    let args = exspeed::cli::server::ServerArgs {
        tcp_listener: Some(bound()),
        api_listener: Some(api),
        drain_timeout_secs: 2,
        ..exspeed::cli::server::ServerArgs::new(tmp.path())
    };
    let h = tokio::spawn(exspeed::cli::server::run_with_shutdown(args, async {
        let _ = rx.await;
    }));
    let url = format!("http://{api_addr}/readyz");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !matches!(reqwest::get(&url).await, Ok(r) if r.status() == 200) {
        assert!(tokio::time::Instant::now() < deadline, "never ready");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let _ = tx.send(());
    tokio::time::timeout(Duration::from_secs(15), h)
        .await
        .expect("server stops")
        .unwrap()
        .unwrap();
    // Once run_with_shutdown has returned, the HTTP API is gone too.
    assert!(
        tokio::net::TcpStream::connect(api_addr).await.is_err(),
        "the HTTP listener is closed when the server returns"
    );
}
