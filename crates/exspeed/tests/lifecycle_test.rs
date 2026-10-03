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
