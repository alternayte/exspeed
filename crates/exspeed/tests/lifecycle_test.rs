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

use bytes::{Bytes, BytesMut};
use exspeed_protocol::codec::ExspeedCodec;
use exspeed_protocol::frame::Frame;
use exspeed_protocol::messages::connect::{AuthType, ConnectRequest};
use exspeed_protocol::opcodes::OpCode;
use futures_util::{SinkExt, StreamExt};
use tokio::sync::oneshot;
use tokio_util::codec::{FramedRead, FramedWrite};

#[tokio::test]
async fn sigterm_signal_token_stops_accept_loop() {
    let port = portpicker::pick_unused_port().unwrap();
    let api_port = portpicker::pick_unused_port().unwrap();
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
                tls_cert: None,
                tls_key: None,
            },
            async {
                let _ = rx.await;
            },
        )
        .await
    });

    // Give the server a moment to bind the listener.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Open a connection, send CONNECT, then trigger shutdown.
    let stream = TcpStream::connect(format!("127.0.0.1:{port}")).await.unwrap();
    let (reader, writer) = stream.into_split();
    let mut framed_read = FramedRead::new(reader, ExspeedCodec::new());
    let mut framed_write = FramedWrite::new(writer, ExspeedCodec::new());
    let mut payload = BytesMut::new();
    ConnectRequest {
        client_id: "shutdown-test".into(),
        auth_type: AuthType::None,
        auth_payload: Bytes::new(),
    }
    .encode(&mut payload);
    framed_write
        .send(Frame::new(OpCode::Connect, 1, payload.freeze()))
        .await
        .unwrap();
    let _ = framed_read.next().await.unwrap().unwrap(); // OK

    let _ = tx.send(());

    let result = tokio::time::timeout(Duration::from_secs(15), server_fut).await;
    let outer = result.expect("server should exit within 15s");
    let inner = outer.expect("server task should not panic");
    inner.expect("server should return Ok on graceful shutdown");
}

#[tokio::test]
async fn readyz_returns_503_when_data_dir_unwritable() {
    let port = portpicker::pick_unused_port().unwrap();
    let api_port = portpicker::pick_unused_port().unwrap();
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
            tls_cert: None,
            tls_key: None,
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
    }

    let bad = reqwest::get(&url).await.unwrap();
    assert_eq!(bad.status(), 503, "should be unready when data_dir is unwritable");

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&data_for_chmod, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
}
