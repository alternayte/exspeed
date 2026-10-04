//! Key-value buckets: put/get, history, delete/purge, compare-and-set under
//! concurrency, keys, TTLs, watch, restart and the HTTP API.

use std::sync::Arc;
use std::time::Duration;

use exspeed_client::{code, BucketOptions, KvOp};

use crate::common::TestServer;

const WAIT: Duration = Duration::from_secs(5);

#[tokio::test]
async fn put_get_delete() {
    let server = TestServer::start().await;
    let c = server.client().await;
    let kv = c.kv("config");
    kv.create(BucketOptions::default()).await.unwrap();

    assert!(kv.get("app.mode").await.unwrap().is_none());
    let r1 = kv.put("app.mode", "dev").await.unwrap();
    let r2 = kv.put("app.mode", "prod").await.unwrap();
    assert!(r2 > r1);
    let e = kv.get("app.mode").await.unwrap().unwrap();
    assert_eq!(
        (&e.value[..], e.revision, e.op),
        (&b"prod"[..], r2, KvOp::Put)
    );
    // History 1: the older value is gone.
    assert!(kv.get_revision("app.mode", r1).await.unwrap().is_none());

    kv.put("app.port", "8080").await.unwrap();
    kv.put("db.url", "postgres://").await.unwrap();
    assert_eq!(
        kv.keys("").await.unwrap(),
        ["app.mode", "app.port", "db.url"]
    );
    assert_eq!(kv.keys("app.*").await.unwrap(), ["app.mode", "app.port"]);

    kv.delete("app.port").await.unwrap();
    assert!(kv.get("app.port").await.unwrap().is_none());
    assert_eq!(kv.keys("app.*").await.unwrap(), ["app.mode"]);

    let err = c.kv("missing").get("x").await.unwrap_err();
    assert_eq!(err.code(), Some(code::NOT_FOUND));
    let err = kv.put("bad key", "x").await.unwrap_err();
    assert_eq!(err.code(), Some(code::BAD_REQUEST));
}

#[tokio::test]
async fn history_and_purge() {
    let server = TestServer::start().await;
    let c = server.client().await;
    let kv = c.kv("h");
    kv.create(BucketOptions {
        history: 3,
        ..Default::default()
    })
    .await
    .unwrap();
    for v in ["1", "2", "3", "4"] {
        kv.put("k", v).await.unwrap();
    }
    let h = kv.history("k").await.unwrap();
    let values: Vec<_> = h
        .iter()
        .map(|e| String::from_utf8_lossy(&e.value).into_owned())
        .collect();
    assert_eq!(values, ["2", "3", "4"], "the newest 3 are kept");
    assert_eq!(
        &kv.get_revision("k", h[0].revision)
            .await
            .unwrap()
            .unwrap()
            .value[..],
        b"2"
    );

    kv.delete("k").await.unwrap();
    let h = kv.history("k").await.unwrap();
    assert_eq!(h.last().unwrap().op, KvOp::Delete);
    kv.put("k", "5").await.unwrap();
    kv.purge("k").await.unwrap();
    let h = kv.history("k").await.unwrap();
    assert_eq!(h.len(), 1, "a purge hides everything before it");
    assert_eq!(h[0].op, KvOp::Purge);
    assert!(kv.get("k").await.unwrap().is_none());
}

#[tokio::test]
async fn compare_and_set() {
    let server = TestServer::start().await;
    let c = server.client().await;
    let kv = c.kv("cas");
    kv.create(BucketOptions::default()).await.unwrap();

    let r1 = kv.create_key("lock", "a").await.unwrap();
    let err = kv.create_key("lock", "b").await.unwrap_err();
    assert_eq!(err.code(), Some(code::CONFLICT));
    let err = kv.update("lock", "b", r1 + 100).await.unwrap_err();
    assert_eq!(err.code(), Some(code::CONFLICT));
    let r2 = kv.update("lock", "b", r1).await.unwrap();
    assert!(r2 > r1);
    // A deleted key can be created again.
    kv.delete("lock").await.unwrap();
    kv.create_key("lock", "c").await.unwrap();
}

#[tokio::test]
async fn concurrent_increments_never_lose_an_update() {
    let server = TestServer::start().await;
    let kv = server.client().await.kv("counter");
    kv.create(BucketOptions::default()).await.unwrap();
    kv.put("n", "0").await.unwrap();

    let mut tasks = Vec::new();
    for _ in 0..8 {
        let c = server.client().await;
        tasks.push(tokio::spawn(async move {
            let kv = c.kv("counter");
            for _ in 0..10 {
                loop {
                    let e = kv.get("n").await.unwrap().unwrap();
                    let n: u64 = String::from_utf8_lossy(&e.value).parse().unwrap();
                    match kv.update("n", (n + 1).to_string(), e.revision).await {
                        Ok(_) => break,
                        Err(err) if err.code() == Some(code::CONFLICT) => continue,
                        Err(err) => panic!("{err}"),
                    }
                }
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    let e = kv.get("n").await.unwrap().unwrap();
    assert_eq!(&e.value[..], b"80", "every increment counted exactly once");
}

#[tokio::test]
async fn keys_and_values_expire() {
    let server = TestServer::start().await;
    let c = server.client().await;
    let kv = c.kv("sessions");
    kv.create(BucketOptions {
        ttl: Some(Duration::from_millis(300)),
        ..Default::default()
    })
    .await
    .unwrap();
    kv.put("s1", "x").await.unwrap();
    kv.put_with("s2", "y", None, Some(Duration::from_millis(100)))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(180)).await;
    assert!(kv.get("s2").await.unwrap().is_none(), "own TTL");
    assert!(kv.get("s1").await.unwrap().is_some());
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert!(kv.get("s1").await.unwrap().is_none(), "bucket TTL");
    assert!(kv.keys("").await.unwrap().is_empty());
}

#[tokio::test]
async fn watch_sees_current_values_then_changes() {
    let server = TestServer::start().await;
    let c = server.client().await;
    let kv = c.kv("w");
    kv.create(BucketOptions::default()).await.unwrap();
    kv.put("a", "1").await.unwrap();
    kv.put("a", "2").await.unwrap();
    kv.put("b", "1").await.unwrap();
    kv.put("gone", "x").await.unwrap();
    kv.delete("gone").await.unwrap();

    let mut w = kv.watch("");
    let first = w.next_timeout(WAIT).await.unwrap().unwrap();
    let second = w.next_timeout(WAIT).await.unwrap().unwrap();
    assert_eq!(
        [
            (first.key.as_str(), &first.value[..]),
            (second.key.as_str(), &second.value[..])
        ],
        [("a", &b"2"[..]), ("b", &b"1"[..])],
        "the current value of each live key"
    );
    let writer = Arc::new(kv.clone());
    let w2 = writer.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        w2.put("c", "new").await.unwrap();
        w2.delete("a").await.unwrap();
    });
    let e = w.next_timeout(WAIT).await.unwrap().unwrap();
    assert_eq!((e.key.as_str(), e.op), ("c", KvOp::Put));
    let e = w.next_timeout(WAIT).await.unwrap().unwrap();
    assert_eq!((e.key.as_str(), e.op), ("a", KvOp::Delete));
}

#[tokio::test]
async fn values_survive_a_restart() {
    let server = TestServer::start().await;
    {
        let kv = server.client().await.kv("durable");
        kv.create(BucketOptions::default()).await.unwrap();
        kv.put("k", "v1").await.unwrap();
        kv.put("k", "v2").await.unwrap();
    }
    let server = server.restart().await;
    let kv = server.client().await.kv("durable");
    let e = kv.get("k").await.unwrap().unwrap();
    assert_eq!(&e.value[..], b"v2");
    // Compare-and-set continues from the stored revision.
    kv.update("k", "v3", e.revision).await.unwrap();
}

#[tokio::test]
async fn kv_over_http() {
    let server = TestServer::start().await;
    let http = reqwest::Client::new();
    let url = |p: &str| server.api_url(p);
    let r = http
        .post(url("/api/v1/kv"))
        .json(&serde_json::json!({"bucket": "web", "history": 2}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 201);

    let r = http
        .put(url("/api/v1/kv/web/site.title"))
        .header("If-None-Match", "*")
        .body("Hello")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let rev = r.json::<serde_json::Value>().await.unwrap()["revision"]
        .as_u64()
        .unwrap();
    let r = http
        .put(url("/api/v1/kv/web/site.title"))
        .header("If-None-Match", "*")
        .body("again")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 409);

    let r = http
        .get(url("/api/v1/kv/web/site.title"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(r.headers()["x-exspeed-revision"], rev.to_string().as_str());
    assert_eq!(r.text().await.unwrap(), "Hello");

    let r = http
        .put(url("/api/v1/kv/web/site.title"))
        .header("If-Match", rev.to_string())
        .body("Hello, world")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let keys: Vec<String> = http
        .get(url("/api/v1/kv/web"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(keys, ["site.title"]);
    let hist: Vec<serde_json::Value> = http
        .get(url("/api/v1/kv/web/site.title/history"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(hist.len(), 2);
    let r = http
        .delete(url("/api/v1/kv/web/site.title"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let r = http
        .get(url("/api/v1/kv/web/site.title"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404);
}
