//! Core (non-persistent) messaging: fan-out, queue groups, request-reply,
//! "no responders", and subject permissions.

use std::time::Duration;

use exspeed_client::code;

use crate::common::TestServer;

const WAIT: Duration = Duration::from_secs(5);

#[tokio::test]
async fn messages_fan_out_to_matching_subscribers() {
    let server = TestServer::start().await;
    let (a, b, p) = (
        server.client().await,
        server.client().await,
        server.client().await,
    );
    let mut all = a.subscribe_core("orders.>", None).await.unwrap();
    let mut eu = b.subscribe_core("orders.eu.*", None).await.unwrap();

    p.publish_core("orders.eu.created", "1").await.unwrap();
    p.publish_core("orders.us.created", "2").await.unwrap();
    p.publish_core("billing.x", "ignored").await.unwrap();

    let m = all.next_timeout(WAIT).await.unwrap();
    assert_eq!(
        (m.subject.as_str(), &m.value[..]),
        ("orders.eu.created", &b"1"[..])
    );
    let m = all.next_timeout(WAIT).await.unwrap();
    assert_eq!(m.subject, "orders.us.created");
    let m = eu.next_timeout(WAIT).await.unwrap();
    assert_eq!(m.subject, "orders.eu.created");
    assert!(eu.next_timeout(Duration::from_millis(200)).await.is_none());
    assert!(all.next_timeout(Duration::from_millis(200)).await.is_none());

    // Nothing is stored: a late subscriber sees only what comes next.
    let mut late = a.subscribe_core("orders.>", None).await.unwrap();
    assert!(late
        .next_timeout(Duration::from_millis(200))
        .await
        .is_none());
}

#[tokio::test]
async fn a_queue_group_splits_messages() {
    let server = TestServer::start().await;
    let (w1, w2, p) = (
        server.client().await,
        server.client().await,
        server.client().await,
    );
    let mut s1 = w1.subscribe_core("jobs", Some("workers")).await.unwrap();
    let mut s2 = w2.subscribe_core("jobs", Some("workers")).await.unwrap();
    for i in 0..20 {
        p.publish_core("jobs", i.to_string()).await.unwrap();
    }
    let mut n = (0, 0);
    while s1.next_timeout(Duration::from_millis(300)).await.is_some() {
        n.0 += 1;
    }
    while s2.next_timeout(Duration::from_millis(300)).await.is_some() {
        n.1 += 1;
    }
    assert_eq!(n.0 + n.1, 20, "every job reaches exactly one worker");
    assert!(n.0 > 0 && n.1 > 0, "{n:?}");
}

#[tokio::test]
async fn request_reply() {
    let server = TestServer::start().await;
    let (svc, caller) = (server.client().await, server.client().await);
    let mut reqs = svc.subscribe_core("svc.upper", Some("svc")).await.unwrap();
    let responder = tokio::spawn(async move {
        while let Some(m) = reqs.next().await {
            let up = String::from_utf8_lossy(&m.value).to_uppercase();
            svc.respond(&m, up).await.unwrap();
        }
    });
    for word in ["hello", "world"] {
        let r = caller.request_core("svc.upper", word, WAIT).await.unwrap();
        assert_eq!(&r.value[..], word.to_uppercase().as_bytes());
    }
    responder.abort();
}

#[tokio::test]
async fn a_request_nobody_hears_fails_fast() {
    let server = TestServer::start().await;
    let c = server.client().await;
    let started = std::time::Instant::now();
    let err = c
        .request_core("svc.missing", "x", Duration::from_secs(30))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some(code::NOT_FOUND), "{err}");
    assert!(started.elapsed() < Duration::from_secs(5));
}

#[tokio::test]
async fn subjects_must_be_concrete_to_publish() {
    let server = TestServer::start().await;
    let c = server.client().await;
    for bad in ["orders.*", "orders.>", "", "a..b"] {
        let err = c.publish_core(bad, "x").await.unwrap_err();
        assert_eq!(err.code(), Some(code::BAD_REQUEST), "{bad:?}: {err}");
    }
    let err = c.subscribe_core("", None).await.unwrap_err();
    assert_eq!(err.code(), Some(code::BAD_REQUEST));
}

#[tokio::test]
async fn subject_permissions_are_enforced() {
    let dir = tempfile::tempdir().unwrap();
    let creds = dir.path().join("credentials.toml");
    let hash = |t: &str| exspeed_common::auth::sha256_hex(t.as_bytes());
    std::fs::write(
        &creds,
        format!(
            r#"
[[credentials]]
name = "svc"
token_sha256 = "{}"
permissions = [{{ subjects = "svc.>", actions = ["subscribe"] }}]

[[credentials]]
name = "caller"
token_sha256 = "{}"
permissions = [{{ subjects = "svc.*", actions = ["publish"] }}]
"#,
            hash("svc-token"),
            hash("caller-token")
        ),
    )
    .unwrap();
    let server = TestServer::builder().credentials_file(&creds).start().await;
    let svc = server.client_with_token("svc-token").await;
    let caller = server.client_with_token("caller-token").await;

    // Each may only do what its permissions say.
    let err = svc.publish_core("svc.echo", "x").await.unwrap_err();
    assert_eq!(err.code(), Some(code::FORBIDDEN));
    let err = caller.subscribe_core("svc.>", None).await.unwrap_err();
    assert_eq!(err.code(), Some(code::FORBIDDEN));
    let err = svc.subscribe_core(">", None).await.unwrap_err();
    assert_eq!(err.code(), Some(code::FORBIDDEN));
    // Nobody may listen to every inbox.
    let err = caller.subscribe_core("_INBOX.>", None).await.unwrap_err();
    assert_eq!(err.code(), Some(code::FORBIDDEN));

    // Request-reply works with just these: replies go to an inbox, which
    // anyone may publish to and its owner may subscribe to.
    let mut reqs = svc.subscribe_core("svc.echo", None).await.unwrap();
    let responder = tokio::spawn(async move {
        if let Some(m) = reqs.next().await {
            svc.respond(&m, m.value.clone()).await.unwrap();
        }
    });
    let r = caller.request_core("svc.echo", "ping", WAIT).await.unwrap();
    assert_eq!(&r.value[..], b"ping");
    responder.await.unwrap();
}
