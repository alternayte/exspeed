//! The core NATS protocol listener, driven over raw sockets (any NATS
//! client library speaks exactly this), plus interop with Exspeed's own
//! core messaging.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

use crate::common::TestServer;

const WAIT: Duration = Duration::from_secs(5);

/// A server-sent operation.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Op {
    Info(Value),
    Msg {
        subject: String,
        sid: String,
        reply: Option<String>,
        /// The raw header block (HMSG), if any.
        headers: Option<String>,
        payload: Bytes,
    },
    Ping,
    Pong,
    Ok,
    Err(String),
}

trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}

/// A minimal NATS client over raw protocol lines.
pub(crate) struct Nats {
    io: BufReader<Box<dyn Io>>,
}

impl Nats {
    /// Connect, read INFO, send CONNECT (merged over sane defaults) + PING
    /// and wait for PONG.
    pub(crate) async fn connect(addr: &str, connect: Value) -> Nats {
        let (mut n, _info) = Self::open(addr).await;
        n.handshake(connect).await;
        n
    }

    pub(crate) async fn open(addr: &str) -> (Nats, Value) {
        let tcp = TcpStream::connect(addr).await.unwrap();
        let mut n = Nats {
            io: BufReader::new(Box::new(tcp)),
        };
        let info = match n.next().await {
            Some(Op::Info(v)) => v,
            other => panic!("expected INFO, got {other:?}"),
        };
        (n, info)
    }

    pub(crate) async fn handshake(&mut self, connect: Value) {
        let verbose = connect["verbose"] == true;
        self.send_connect(connect).await;
        self.send("PING").await;
        if verbose {
            assert_eq!(self.next().await, Some(Op::Ok), "+OK for CONNECT");
        }
        match self.next().await {
            Some(Op::Pong) => {}
            other => panic!("expected PONG after CONNECT, got {other:?}"),
        }
    }

    pub(crate) async fn send_connect(&mut self, connect: Value) {
        let mut opts = json!({"verbose": false, "pedantic": false, "headers": true,
            "no_responders": true, "protocol": 1, "lang": "test"});
        for (k, v) in connect.as_object().unwrap() {
            opts[k] = v.clone();
        }
        self.send(&format!("CONNECT {opts}")).await;
    }

    pub(crate) async fn send(&mut self, line: &str) {
        let w = self.io.get_mut();
        w.write_all(line.as_bytes()).await.unwrap();
        w.write_all(b"\r\n").await.unwrap();
        w.flush().await.unwrap();
    }

    pub(crate) async fn publish(&mut self, subject: &str, payload: &str) {
        self.send(&format!("PUB {subject} {}\r\n{payload}", payload.len()))
            .await;
    }

    pub(crate) async fn subscribe(&mut self, subject: &str, sid: &str) {
        self.send(&format!("SUB {subject} {sid}")).await;
    }

    /// Make sure every earlier command was processed.
    pub(crate) async fn flush(&mut self) {
        self.send("PING").await;
        loop {
            match self.next().await {
                Some(Op::Pong) => return,
                Some(Op::Ok) => continue,
                other => panic!("expected PONG, got {other:?}"),
            }
        }
    }

    /// The next operation; `None` on EOF.
    pub(crate) async fn next(&mut self) -> Option<Op> {
        let mut line = String::new();
        if self.io.read_line(&mut line).await.ok()? == 0 {
            return None;
        }
        let line = line.trim_end_matches(['\r', '\n']).to_string();
        let mut parts = line.split(' ');
        let op = parts.next().unwrap().to_string();
        let args: Vec<String> = parts.map(str::to_string).collect();
        Some(match op.as_str() {
            "INFO" => Op::Info(serde_json::from_str(&line[5..]).unwrap()),
            "PING" => Op::Ping,
            "PONG" => Op::Pong,
            "+OK" => Op::Ok,
            "-ERR" => Op::Err(line[5..].trim_matches('\'').to_string()),
            "MSG" | "HMSG" => {
                let n = |s: &str| s.parse::<usize>().unwrap();
                let (reply, hdr, total) = match (op.as_str(), args.len()) {
                    ("MSG", 3) => (None, 0, n(&args[2])),
                    ("MSG", 4) => (Some(args[2].clone()), 0, n(&args[3])),
                    ("HMSG", 4) => (None, n(&args[2]), n(&args[3])),
                    ("HMSG", 5) => (Some(args[2].clone()), n(&args[3]), n(&args[4])),
                    _ => panic!("bad {line}"),
                };
                let mut body = vec![0u8; total + 2];
                self.io.read_exact(&mut body).await.ok()?;
                body.truncate(total);
                let payload = Bytes::from(body.split_off(hdr));
                Op::Msg {
                    subject: args[0].clone(),
                    sid: args[1].clone(),
                    reply,
                    headers: (op == "HMSG").then(|| String::from_utf8(body).unwrap()),
                    payload,
                }
            }
            other => panic!("unexpected op {other}: {line}"),
        })
    }

    pub(crate) async fn next_timeout(&mut self, d: Duration) -> Option<Op> {
        tokio::time::timeout(d, self.next()).await.ok().flatten()
    }

    pub(crate) async fn next_msg(
        &mut self,
    ) -> (String, String, Option<String>, Option<String>, String) {
        match tokio::time::timeout(WAIT, self.next()).await {
            Ok(Some(Op::Msg {
                subject,
                sid,
                reply,
                headers,
                payload,
            })) => (
                subject,
                sid,
                reply,
                headers,
                String::from_utf8(payload.to_vec()).unwrap(),
            ),
            other => panic!("expected MSG, got {other:?}"),
        }
    }
}

/// Count the messages that arrive until the connection goes quiet.
async fn drain(n: &mut Nats) -> usize {
    let mut k = 0;
    while let Some(Op::Msg { .. }) = n.next_timeout(Duration::from_millis(300)).await {
        k += 1;
    }
    k
}

fn nats_addr(s: &TestServer) -> String {
    s.nats_addr.clone().expect("server started with .nats()")
}

#[tokio::test]
async fn info_describes_the_server() {
    let server = TestServer::builder().nats().start().await;
    let (_n, info) = Nats::open(&nats_addr(&server)).await;
    assert_eq!(info["headers"], true);
    assert_eq!(info["proto"], 1);
    assert_eq!(info["auth_required"], false);
    assert_eq!(info["tls_required"], false);
    assert_eq!(info["max_payload"], 8 * 1024 * 1024);
    assert!(info["client_id"].as_u64().unwrap() > 0);
}

#[tokio::test]
async fn publish_subscribe_with_wildcards_queue_groups_and_headers() {
    let server = TestServer::builder().nats().start().await;
    let addr = nats_addr(&server);
    let mut a = Nats::connect(&addr, json!({})).await;
    let mut b = Nats::connect(&addr, json!({})).await;
    b.subscribe("orders.*", "1").await;
    b.subscribe("orders.>", "2").await;
    b.flush().await;
    a.publish("orders.eu", "x").await;
    a.publish("orders.eu.created", "y").await;
    a.publish("other", "z").await;
    let mut got = vec![b.next_msg().await, b.next_msg().await, b.next_msg().await];
    got.sort_by_key(|m| (m.1.clone(), m.4.clone()));
    let got: Vec<(String, String, String)> = got.into_iter().map(|m| (m.0, m.1, m.4)).collect();
    assert_eq!(
        got,
        vec![
            ("orders.eu".into(), "1".into(), "x".into()),
            ("orders.eu".into(), "2".into(), "x".into()),
            ("orders.eu.created".into(), "2".into(), "y".into()),
        ]
    );
    assert!(b.next_timeout(Duration::from_millis(200)).await.is_none());

    // Headers round-trip through HPUB/HMSG.
    b.subscribe("h", "3").await;
    b.flush().await;
    let hdr = "NATS/1.0\r\nTrace-Id: 42\r\n\r\n";
    a.send(&format!(
        "HPUB h {} {}\r\n{hdr}hi",
        hdr.len(),
        hdr.len() + 2
    ))
    .await;
    let (_, sid, _, headers, payload) = b.next_msg().await;
    assert_eq!((sid.as_str(), payload.as_str()), ("3", "hi"));
    assert_eq!(headers.as_deref(), Some(hdr));

    // Queue group: 20 messages split across two members, none duplicated.
    let mut c = Nats::connect(&addr, json!({})).await;
    b.send("SUB jobs workers 10").await;
    c.send("SUB jobs workers 11").await;
    b.flush().await;
    c.flush().await;
    for i in 0..20 {
        a.publish("jobs", &i.to_string()).await;
    }
    a.flush().await;
    let (kb, kc) = (drain(&mut b).await, drain(&mut c).await);
    assert_eq!(kb + kc, 20);
    assert!(kb > 0 && kc > 0, "{kb} / {kc}");
}

#[tokio::test]
async fn unsubscribe_echo_verbose_and_ping() {
    let server = TestServer::builder().nats().start().await;
    let addr = nats_addr(&server);
    let mut n = Nats::connect(&addr, json!({})).await;
    n.send("PING").await;
    assert_eq!(n.next().await, Some(Op::Pong));

    // UNSUB <sid> <max>: delivered twice, then gone.
    n.subscribe("auto", "1").await;
    n.send("UNSUB 1 2").await;
    n.flush().await;
    for _ in 0..4 {
        n.publish("auto", "x").await;
    }
    assert!(matches!(n.next_msg().await, (_, s, ..) if s == "1"));
    assert!(matches!(n.next_msg().await, (_, s, ..) if s == "1"));
    n.flush().await;

    // Plain UNSUB stops at once.
    n.subscribe("gone", "2").await;
    n.send("UNSUB 2").await;
    n.publish("gone", "x").await;
    n.flush().await;

    // echo: false skips the connection's own messages.
    let mut quiet = Nats::connect(&addr, json!({"echo": false})).await;
    quiet.subscribe("e", "1").await;
    quiet.publish("e", "mine").await;
    quiet.flush().await;
    n.publish("e", "theirs").await;
    assert_eq!(quiet.next_msg().await.4, "theirs");

    // Verbose mode acknowledges every command.
    let mut v = Nats::connect(&addr, json!({"verbose": true})).await;
    v.subscribe("v", "1").await;
    assert_eq!(v.next().await, Some(Op::Ok));
    v.publish("v", "1").await;
    let mut seen = vec![v.next().await.unwrap(), v.next().await.unwrap()];
    seen.sort_by_key(|o| matches!(o, Op::Ok));
    assert!(
        matches!(seen[0], Op::Msg { .. }) && seen[1] == Op::Ok,
        "{seen:?}"
    );
}

#[tokio::test]
async fn requests_get_a_503_status_when_nobody_listens() {
    let server = TestServer::builder().nats().start().await;
    let addr = nats_addr(&server);
    let mut n = Nats::connect(&addr, json!({})).await;
    n.subscribe("_INBOX.abc.*", "9").await;
    n.send("PUB svc.none _INBOX.abc.1 2\r\nhi").await;
    let (subject, sid, _, headers, payload) = n.next_msg().await;
    assert_eq!((subject.as_str(), sid.as_str()), ("_INBOX.abc.1", "9"));
    assert_eq!(headers.as_deref(), Some("NATS/1.0 503\r\n\r\n"));
    assert!(payload.is_empty());

    // Without no_responders the request simply goes unanswered.
    let mut old = Nats::connect(&addr, json!({"no_responders": false})).await;
    old.subscribe("_INBOX.def.*", "1").await;
    old.send("PUB svc.none _INBOX.def.1 2\r\nhi").await;
    assert!(old.next_timeout(Duration::from_millis(300)).await.is_none());

    // With a responder, the reply arrives.
    let mut svc = Nats::connect(&addr, json!({})).await;
    svc.send("SUB svc.echo q 1").await;
    svc.flush().await;
    n.send("PUB svc.echo _INBOX.abc.2 4\r\nping").await;
    let (_, _, reply, _, body) = svc.next_msg().await;
    assert_eq!(reply.as_deref(), Some("_INBOX.abc.2"));
    svc.send(&format!("PUB {} 4\r\npong", reply.unwrap())).await;
    let (subject, _, _, _, body2) = n.next_msg().await;
    assert_eq!(
        (subject.as_str(), body.as_str(), body2.as_str()),
        ("_INBOX.abc.2", "ping", "pong")
    );
}

#[tokio::test]
async fn interoperates_with_exspeed_core_messaging() {
    let server = TestServer::builder().nats().start().await;
    let addr = nats_addr(&server);
    let ex = server.client().await;
    let mut n = Nats::connect(&addr, json!({})).await;

    // NATS -> Exspeed, headers included.
    let mut sub = ex.subscribe_core("x.>", None).await.unwrap();
    let hdr = "NATS/1.0\r\nk: v\r\n\r\n";
    n.send(&format!(
        "HPUB x.1 {} {}\r\n{hdr}from-nats",
        hdr.len(),
        hdr.len() + 9
    ))
    .await;
    let m = tokio::time::timeout(WAIT, sub.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.subject, "x.1");
    assert_eq!(&m.value[..], b"from-nats");
    assert_eq!(m.headers, vec![("k".to_string(), "v".to_string())]);

    // Exspeed -> NATS.
    n.subscribe("y.*", "5").await;
    n.flush().await;
    ex.publish_core_with("y.1", "from-exspeed", vec![("a".into(), "b".into())])
        .await
        .unwrap();
    let (subject, sid, _, headers, payload) = n.next_msg().await;
    assert_eq!(
        (subject.as_str(), sid.as_str(), payload.as_str()),
        ("y.1", "5", "from-exspeed")
    );
    assert_eq!(headers.as_deref(), Some("NATS/1.0\r\na: b\r\n\r\n"));

    // A NATS request answered by an Exspeed responder.
    let mut svc = ex.subscribe_core("svc.add", Some("adders")).await.unwrap();
    let responder = {
        let ex = server.client().await;
        tokio::spawn(async move {
            let req = svc.next().await.unwrap();
            ex.respond(
                &req,
                Bytes::from(format!("sum:{}", String::from_utf8_lossy(&req.value))),
            )
            .await
            .unwrap();
        })
    };
    n.subscribe("_INBOX.r.*", "6").await;
    n.flush().await;
    n.send("PUB svc.add _INBOX.r.1 3\r\n1+2").await;
    let (subject, _, _, _, body) = n.next_msg().await;
    assert_eq!((subject.as_str(), body.as_str()), ("_INBOX.r.1", "sum:1+2"));
    responder.await.unwrap();

    // An Exspeed request answered by a NATS responder.
    n.subscribe("svc.mul", "7").await;
    n.flush().await;
    let req = tokio::spawn({
        let ex = server.client().await;
        async move { ex.request_core("svc.mul", "2*3", WAIT).await }
    });
    let (_, _, reply, _, body) = n.next_msg().await;
    assert_eq!(body, "2*3");
    n.publish(&reply.unwrap(), "6").await;
    let resp = req.await.unwrap().unwrap();
    assert_eq!(&resp.value[..], b"6");
}

#[tokio::test]
async fn authentication_and_subject_permissions() {
    let dir = tempfile::tempdir().unwrap();
    let creds = dir.path().join("credentials.toml");
    let sha = |t: &str| {
        use sha2::{Digest, Sha256};
        Sha256::digest(t.as_bytes())
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>()
    };
    std::fs::write(
        &creds,
        format!(
            r#"
[[credentials]]
name = "svc"
token_sha256 = "{}"
permissions = [{{ subjects = "svc.>", actions = ["publish", "subscribe"] }}]
"#,
            sha("svc-token")
        ),
    )
    .unwrap();
    let server = TestServer::builder()
        .nats()
        .credentials_file(&creds)
        .start()
        .await;
    let addr = nats_addr(&server);

    // No credentials: Authorization Violation, connection closed.
    let (mut n, info) = Nats::open(&addr).await;
    assert_eq!(info["auth_required"], true);
    n.send_connect(json!({})).await;
    assert_eq!(
        n.next().await,
        Some(Op::Err("Authorization Violation".into()))
    );
    assert_eq!(n.next().await, None);

    // A wrong token likewise; a command before CONNECT too.
    let (mut n, _) = Nats::open(&addr).await;
    n.send_connect(json!({"auth_token": "nope"})).await;
    assert_eq!(
        n.next().await,
        Some(Op::Err("Authorization Violation".into()))
    );
    let (mut n, _) = Nats::open(&addr).await;
    n.send("PING").await;
    assert_eq!(
        n.next().await,
        Some(Op::Err("Authorization Violation".into()))
    );

    // Token as auth_token or as a password.
    let mut ok = Nats::connect(&addr, json!({"auth_token": "svc-token"})).await;
    let mut ok2 = Nats::connect(&addr, json!({"user": "any", "pass": "svc-token"})).await;
    ok2.subscribe("svc.x", "1").await;
    ok2.flush().await;
    ok.publish("svc.x", "allowed").await;
    assert_eq!(ok2.next_msg().await.4, "allowed");

    // Outside its subjects: a permissions error, the connection stays up.
    ok.publish("secret.x", "no").await;
    assert_eq!(
        ok.next().await,
        Some(Op::Err(
            "Permissions Violation for Publish to \"secret.x\"".into()
        ))
    );
    ok.subscribe("secret.>", "2").await;
    assert_eq!(
        ok.next().await,
        Some(Op::Err(
            "Permissions Violation for Subscription to \"secret.>\"".into()
        ))
    );
    ok.flush().await;

    // Its own inbox and replies to inboxes are always allowed.
    ok.subscribe("_INBOX.me.*", "3").await;
    ok.flush().await;
    ok2.publish("_INBOX.me.1", "reply").await;
    assert_eq!(ok.next_msg().await.4, "reply");
}

#[tokio::test]
async fn protocol_errors() {
    let server = TestServer::builder().nats().start().await;
    let addr = nats_addr(&server);

    let mut n = Nats::connect(&addr, json!({})).await;
    n.publish("bad.*", "x").await;
    assert_eq!(
        n.next().await,
        Some(Op::Err("Invalid Publish Subject".into()))
    );
    n.subscribe("bad..subject", "1").await;
    assert_eq!(n.next().await, Some(Op::Err("Invalid Subject".into())));
    n.flush().await;

    let mut n = Nats::connect(&addr, json!({})).await;
    n.send("FOO").await;
    assert_eq!(
        n.next().await,
        Some(Op::Err("Unknown Protocol Operation".into()))
    );
    assert_eq!(n.next().await, None);

    let mut n = Nats::connect(&addr, json!({})).await;
    n.send(&format!("PUB big {}", 64 * 1024 * 1024)).await;
    assert_eq!(
        n.next().await,
        Some(Op::Err("Maximum Payload Violation".into()))
    );
    assert_eq!(n.next().await, None);
}

#[tokio::test]
async fn tls_upgrade_after_info() {
    let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let (cert_path, key_path) = (dir.path().join("c.pem"), dir.path().join("k.pem"));
    std::fs::write(&cert_path, cert.cert.pem()).unwrap();
    std::fs::write(&key_path, cert.key_pair.serialize_pem()).unwrap();
    let server = TestServer::builder()
        .nats()
        .tls(&cert_path, &key_path)
        .start()
        .await;

    let mut tcp = BufReader::new(TcpStream::connect(nats_addr(&server)).await.unwrap());
    let mut line = String::new();
    tcp.read_line(&mut line).await.unwrap();
    let info: Value = serde_json::from_str(line.trim_end()[5..].trim()).unwrap();
    assert_eq!(info["tls_required"], true);

    let mut roots = tokio_rustls::rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let cfg = tokio_rustls::rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = tokio_rustls::TlsConnector::from(Arc::new(cfg));
    let name = tokio_rustls::rustls::pki_types::ServerName::try_from("localhost").unwrap();
    let tls = connector.connect(name, tcp.into_inner()).await.unwrap();
    let mut n = Nats {
        io: BufReader::new(Box::new(tls)),
    };
    n.handshake(json!({})).await;
    n.subscribe("t", "1").await;
    n.publish("t", "secure").await;
    assert_eq!(n.next_msg().await.4, "secure");
}

#[tokio::test]
async fn shutdown_sends_lame_duck_info() {
    let mut server = TestServer::builder().nats().start().await;
    let mut n = Nats::connect(&nats_addr(&server), json!({})).await;
    server.stop().await;
    match n.next_timeout(WAIT).await {
        Some(Op::Info(v)) => assert_eq!(v["ldm"], true),
        other => panic!("expected lame-duck INFO, got {other:?}"),
    }
}

/// A stream that captures `subjects`, created over the HTTP API.
async fn capturing_stream(
    server: &TestServer,
    name: &str,
    subjects: &[&str],
) -> reqwest::StatusCode {
    reqwest::Client::new()
        .post(server.api_url("/api/v1/streams"))
        .json(&json!({"name": name, "capture_subjects": subjects}))
        .send()
        .await
        .unwrap()
        .status()
}

#[tokio::test]
async fn streams_capture_nats_publishes_with_pub_acks_and_dedup() {
    let server = TestServer::builder().nats().start().await;
    assert!(capturing_stream(&server, "orders", &["orders.>"])
        .await
        .is_success());
    // Overlapping capture subjects are refused.
    assert_eq!(
        capturing_stream(&server, "dup", &["orders.eu.*"]).await,
        reqwest::StatusCode::BAD_REQUEST
    );
    let addr = nats_addr(&server);
    let mut n = Nats::connect(&addr, json!({})).await;
    let mut watcher = Nats::connect(&addr, json!({})).await;
    watcher.subscribe("orders.>", "1").await;
    watcher.flush().await;

    // Fire-and-forget: stored and still delivered to core subscribers.
    n.publish("orders.eu.created", "a").await;
    assert_eq!(watcher.next_msg().await.4, "a");

    // A request gets a PubAck from the stream, not a no-responders 503.
    n.subscribe("_INBOX.ack.*", "9").await;
    let hdr = "NATS/1.0\r\nNats-Msg-Id: m-1\r\n\r\n";
    let hpub = format!(
        "HPUB orders.us _INBOX.ack.1 {} {}\r\n{hdr}b",
        hdr.len(),
        hdr.len() + 1
    );
    n.send(&hpub).await;
    let ack: Value = serde_json::from_str(&n.next_msg().await.4).unwrap();
    assert_eq!(ack, json!({"stream": "orders", "seq": 2}));
    // The same Nats-Msg-Id again: deduplicated.
    n.send(&hpub.replace("_INBOX.ack.1", "_INBOX.ack.2")).await;
    let ack: Value = serde_json::from_str(&n.next_msg().await.4).unwrap();
    assert_eq!(
        ack,
        json!({"stream": "orders", "seq": 2, "duplicate": true})
    );
    // Other subjects aren't captured.
    n.publish("other", "c").await;
    n.flush().await;

    // An Exspeed CorePublish to a captured subject is stored too.
    let ex = server.client().await;
    ex.publish_core("orders.ex", "d").await.unwrap();

    let records = crate::common::eventually(WAIT, || async {
        let r = ex.read("orders", 0, 10, Duration::ZERO, "").await.ok()?;
        (r.records.len() == 3).then_some(r.records)
    })
    .await;
    let got: Vec<(String, String)> = records
        .iter()
        .map(|r| {
            (
                r.subject.clone(),
                String::from_utf8_lossy(&r.value).into_owned(),
            )
        })
        .collect();
    assert_eq!(
        got,
        vec![
            ("orders.eu.created".to_string(), "a".to_string()),
            ("orders.us".to_string(), "b".to_string()),
            ("orders.ex".to_string(), "d".to_string()),
        ]
    );
    assert!(records[1]
        .headers
        .contains(&("Nats-Msg-Id".to_string(), "m-1".to_string())));
}

#[tokio::test]
async fn capture_errors_are_reported_to_the_requester() {
    let server = TestServer::builder().nats().start().await;
    let status = reqwest::Client::new()
        .post(server.api_url("/api/v1/streams"))
        .json(&json!({"name": "bounded", "capture_subjects": ["jobs.*"],
            "max_msgs": 1, "discard": "new"}))
        .send()
        .await
        .unwrap()
        .status();
    assert!(status.is_success());
    let mut n = Nats::connect(&nats_addr(&server), json!({})).await;
    n.subscribe("_INBOX.e.*", "1").await;
    n.send("PUB jobs.a _INBOX.e.1 1\r\nx").await;
    let ack: Value = serde_json::from_str(&n.next_msg().await.4).unwrap();
    assert_eq!(ack["seq"], 1);
    n.send("PUB jobs.a _INBOX.e.2 1\r\ny").await;
    let ack: Value = serde_json::from_str(&n.next_msg().await.4).unwrap();
    assert_eq!(ack["error"]["code"], 429, "{ack}");
}
