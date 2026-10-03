//! `http_poll` and `http_sink` against an in-process HTTP server.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{json, Value};

use exspeed_connectors::offset_store::OffsetStore;
use exspeed_connectors::status::Status;
use exspeed_connectors::ConnectorType::{Sink, Source};
use exspeed_connectors::Registry;

use crate::common::*;

#[derive(Clone, Default)]
struct Mock {
    /// Status codes to answer with before succeeding (front first).
    script: Arc<Mutex<VecDeque<u16>>>,
    /// Bodies received by the sink endpoint, with their Idempotency-Key.
    received: Arc<Mutex<Vec<(String, String)>>>,
    page_hits: Arc<Mutex<u32>>,
}

async fn serve(router: Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    format!("http://{addr}")
}

/// 250 items in one response; a second page via `?page=2`.
async fn items(
    State(m): State<Mock>,
    Query(q): Query<std::collections::HashMap<String, String>>,
) -> Json<Value> {
    *m.page_hits.lock().unwrap() += 1;
    let page2 = q.get("page").map(String::as_str) == Some("2");
    let (range, next) = if page2 {
        (250..260, Value::Null)
    } else {
        (0..250, json!("2"))
    };
    let items: Vec<Value> = range.map(|i| json!({"id": i, "kind": "thing"})).collect();
    Json(json!({"data": {"items": items}, "next": next}))
}

async fn hook(State(m): State<Mock>, headers: HeaderMap, body: String) -> Response {
    if let Some(code) = m.script.lock().unwrap().pop_front() {
        let mut r = (StatusCode::from_u16(code).unwrap(), "scripted").into_response();
        if code == 429 {
            r.headers_mut().insert("retry-after", "0".parse().unwrap());
        }
        return r;
    }
    let key = headers
        .get("idempotency-key")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    m.received.lock().unwrap().push((body, key));
    StatusCode::NO_CONTENT.into_response()
}

fn router(m: &Mock) -> Router {
    Router::new()
        .route("/items", get(items))
        .route("/hook", post(hook))
        .with_state(m.clone())
}

#[tokio::test]
async fn http_poll_emits_every_item_and_follows_pages() {
    let m = Mock::default();
    let base = serve(router(&m)).await;
    let env = Env::new();
    let mut cfg = fast_config("poller", Source, "http_poll", "things");
    cfg.batch_size = 100; // smaller than one response: must not truncate
    cfg.subject_template = "things.{$.kind}".into();
    cfg.settings = json!({
        "url": format!("{base}/items"),
        "interval_secs": 3600,
        "items_path": "data.items",
        "item_key": "id",
        "idempotent_items": true,
        "next_page_path": "next",
        "page_param": "page",
    })
    .as_object()
    .unwrap()
    .clone();
    let (h, _) = env.run(&Registry::builtin(), cfg, Arc::new(MemOffsets::default()));
    eventually(10, "260 items", || async {
        env.read_all("things").await.len() >= 260
    })
    .await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    h.stop(Duration::from_secs(5)).await;

    let recs = env.read_all("things").await;
    assert_eq!(recs.len(), 260);
    let ids: Vec<i64> = recs
        .iter()
        .map(|r| {
            serde_json::from_slice::<Value>(&r.value).unwrap()["id"]
                .as_i64()
                .unwrap()
        })
        .collect();
    assert_eq!(ids, (0..260).collect::<Vec<_>>());
    assert_eq!(recs[0].subject, "things.thing");
    assert_eq!(recs[7].key.as_deref(), Some(&b"7"[..]));
    assert_eq!(
        *m.page_hits.lock().unwrap(),
        2,
        "one interval = both pages, then wait"
    );
}

fn sink_cfg(name: &str, base: &str) -> exspeed_connectors::ConnectorConfig {
    let mut c = fast_config(name, Sink, "http_sink", "in");
    c.settings = json!({"url": format!("{base}/hook"), "timeout_secs": 5})
        .as_object()
        .unwrap()
        .clone();
    c
}

#[tokio::test]
async fn http_sink_retries_transient_and_commits() {
    let m = Mock::default();
    m.script.lock().unwrap().extend([503, 429, 500]);
    let base = serve(router(&m)).await;
    let env = Env::new();
    env.publish("in", &["a", "b"]).await;
    let offsets: Arc<dyn OffsetStore> = Arc::new(MemOffsets::default());
    let (h, state) = env.run(
        &Registry::builtin(),
        sink_cfg("hs1", &base),
        offsets.clone(),
    );
    eventually(10, "committed", || async {
        offsets.load_sink("hs1").await.unwrap() == Some(2)
    })
    .await;
    h.stop(Duration::from_secs(5)).await;
    let got = m.received.lock().unwrap().clone();
    assert_eq!(
        got,
        vec![
            ("a".to_string(), "in:0".to_string()),
            ("b".to_string(), "in:1".to_string())
        ]
    );
    assert_eq!(state.snapshot().restart_count, 0, "retried in place");
}

#[tokio::test]
async fn http_sink_401_fails_the_connector() {
    let m = Mock::default();
    m.script.lock().unwrap().extend([401; 100]);
    let base = serve(router(&m)).await;
    let env = Env::new();
    env.publish("in", &["a"]).await;
    let offsets: Arc<dyn OffsetStore> = Arc::new(MemOffsets::default());
    let (_h, state) = env.run(
        &Registry::builtin(),
        sink_cfg("hs2", &base),
        offsets.clone(),
    );
    eventually(10, "failed", || async { state.status() == Status::Failed }).await;
    assert!(state.snapshot().last_error.unwrap().contains("401"));
    assert_eq!(offsets.load_sink("hs2").await.unwrap(), None);
}

#[tokio::test]
async fn http_sink_4xx_is_poison_and_goes_to_dlq() {
    let m = Mock::default();
    m.script.lock().unwrap().extend([422]);
    let base = serve(router(&m)).await;
    let env = Env::new();
    env.publish("in", &["bad", "good"]).await;
    let mut cfg = sink_cfg("hs3", &base);
    cfg.dlq_stream = Some("in-dlq".into());
    let offsets: Arc<dyn OffsetStore> = Arc::new(MemOffsets::default());
    let (h, _) = env.run(&Registry::builtin(), cfg, offsets.clone());
    eventually(10, "committed", || async {
        offsets.load_sink("hs3").await.unwrap() == Some(2)
    })
    .await;
    h.stop(Duration::from_secs(5)).await;
    assert_eq!(values(&env.read_all("in-dlq").await), vec!["bad"]);
    assert_eq!(m.received.lock().unwrap().len(), 1);
}
