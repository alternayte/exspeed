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
    let port = portpicker::pick_unused_port().unwrap();
    let api_port = portpicker::pick_unused_port().unwrap();
    let bind = format!("127.0.0.1:{port}");
    let api_bind = format!("127.0.0.1:{api_port}");
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().to_path_buf();

    // NOTE: env var is process-global and not serialized — only one consumer (this test) exists today.
    // If a sibling test reads EXSPEED_MAX_CONNS, add Mutex serialization (see log_format.rs tests).
    std::env::set_var("EXSPEED_MAX_CONNS", max_conns.to_string());

    let bind_clone = bind.clone();
    let api_clone = api_bind.clone();
    let data_clone = data_dir.clone();
    tokio::spawn(async move {
        exspeed::cli::server::run(exspeed::cli::server::ServerArgs {
            bind: bind_clone,
            api_bind: api_clone,
            data_dir: data_clone,
            auth_token: None,
            tls_cert: None,
            tls_key: None,
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
