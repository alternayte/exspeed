#![allow(dead_code)]
pub mod db;

use std::path::{Path, PathBuf};
use std::time::Duration;

use exspeed::cli::server::ServerArgs;
use exspeed_client::{Client, ConnectOptions, PublishRecord, StreamSpec};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

/// An in-process server on random ports with its own data directory.
/// Dropping it shuts the server down.
pub struct TestServer {
    pub addr: String,
    pub api_addr: String,
    pub data_dir: PathBuf,
    _tmp: Option<tempfile::TempDir>,
    stop: Option<oneshot::Sender<()>>,
    handle: Option<JoinHandle<anyhow::Result<()>>>,
}

pub struct Builder {
    data_dir: Option<PathBuf>,
    configure: Vec<Box<dyn FnOnce(&mut ServerArgs) + Send>>,
}

impl Builder {
    /// Reuse a data directory (restart tests). The caller keeps it alive.
    pub fn data_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.data_dir = Some(dir.into());
        self
    }

    pub fn auth_token(self, token: &str) -> Self {
        let t = token.to_string();
        self.with(move |a| a.auth_token = Some(t))
    }

    pub fn credentials_file(self, path: impl Into<PathBuf>) -> Self {
        let p = path.into();
        self.with(move |a| a.credentials_file = Some(p))
    }

    pub fn tls(self, cert: impl Into<PathBuf>, key: impl Into<PathBuf>) -> Self {
        let (c, k) = (cert.into(), key.into());
        self.with(move |a| {
            a.tls_cert = Some(c);
            a.tls_key = Some(k);
        })
    }

    pub fn with(mut self, f: impl FnOnce(&mut ServerArgs) + Send + 'static) -> Self {
        self.configure.push(Box::new(f));
        self
    }

    pub async fn start(self) -> TestServer {
        let (tmp, data_dir) = match self.data_dir {
            Some(d) => (None, d),
            None => {
                let t = tempfile::tempdir().unwrap();
                let p = t.path().to_path_buf();
                (Some(t), p)
            }
        };
        let port = exspeed_testkit::pick_unused_port().unwrap();
        let api_port = exspeed_testkit::pick_unused_port().unwrap();
        let addr = format!("127.0.0.1:{port}");
        let api_addr = format!("127.0.0.1:{api_port}");
        let mut args = ServerArgs::new(&data_dir);
        args.bind = addr.clone();
        args.api_bind = api_addr.clone();
        let tls = {
            for f in self.configure {
                f(&mut args);
            }
            args.tls_cert.is_some()
        };
        let (stop_tx, stop_rx) = oneshot::channel::<()>();
        let handle = tokio::spawn(exspeed::cli::server::run_with_shutdown(args, async move {
            let _ = stop_rx.await;
        }));
        let server = TestServer {
            addr,
            api_addr,
            data_dir,
            _tmp: tmp,
            stop: Some(stop_tx),
            handle: Some(handle),
        };
        server.wait_ready(tls).await;
        server
    }
}

impl TestServer {
    pub fn builder() -> Builder {
        Builder {
            data_dir: None,
            configure: Vec::new(),
        }
    }

    pub async fn start() -> TestServer {
        Self::builder().start().await
    }

    async fn wait_ready(&self, tls: bool) {
        let scheme = if tls { "https" } else { "http" };
        let url = format!("{scheme}://{}/readyz", self.api_addr);
        let http = reqwest::Client::builder()
            .danger_accept_invalid_certs(true)
            .build()
            .unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            if let Some(h) = &self.handle {
                assert!(!h.is_finished(), "server exited during startup");
            }
            let tcp_up = tokio::net::TcpStream::connect(&self.addr).await.is_ok();
            let http_up = matches!(http.get(&url).send().await, Ok(r) if r.status().is_success());
            if tcp_up && http_up {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "server did not become ready"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    pub fn api_url(&self, path: &str) -> String {
        format!("http://{}{}", self.api_addr, path)
    }

    pub fn data_path(&self) -> &Path {
        &self.data_dir
    }

    /// A connected client without credentials.
    pub async fn client(&self) -> Client {
        Client::connect(&self.addr, ConnectOptions::default())
            .await
            .expect("connect")
    }

    pub async fn client_with_token(&self, token: &str) -> Client {
        Client::connect(&self.addr, ConnectOptions::default().token(token))
            .await
            .expect("connect")
    }

    /// Stop the server and wait for it to exit (it releases the data-dir
    /// lock). The temp dir, if owned, stays until `self` drops.
    pub async fn stop(&mut self) {
        if let Some(tx) = self.stop.take() {
            let _ = tx.send(());
        }
        if let Some(h) = self.handle.take() {
            let _ = tokio::time::timeout(Duration::from_secs(20), h).await;
        }
    }

    /// Stop, then start again on the same data dir and ports-agnostic.
    pub async fn restart(mut self) -> TestServer {
        self.stop().await;
        let tmp = self._tmp.take();
        let mut next = TestServer::builder()
            .data_dir(self.data_dir.clone())
            .start()
            .await;
        next._tmp = tmp;
        next
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        if let Some(tx) = self.stop.take() {
            let _ = tx.send(());
        }
    }
}

/// Create `stream` with default settings.
pub async fn create_stream(client: &Client, stream: &str) {
    client
        .create_stream(StreamSpec::named(stream))
        .await
        .expect("create stream");
}

/// Publish `n` records `{"i": <n>}` on `subject`; returns their offsets.
pub async fn publish_n(client: &Client, stream: &str, subject: &str, n: usize) -> Vec<u64> {
    let mut offsets = Vec::with_capacity(n);
    for i in 0..n {
        let ack = client
            .publish(
                stream,
                PublishRecord::new(subject, format!(r#"{{"i":{i}}}"#)),
            )
            .await
            .expect("publish");
        offsets.push(ack.offset);
    }
    offsets
}

/// Poll `f` until it returns `Some`, or panic after `timeout`.
pub async fn eventually<T, F, Fut>(timeout: Duration, mut f: F) -> T
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Option<T>>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(v) = f().await {
            return v;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "condition not met within {timeout:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}
