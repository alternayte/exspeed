//! `GET /api/v1/openapi.json`, and the spec kept in sync with the router.

use std::collections::BTreeSet;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::Value;
use tower::ServiceExt;

use crate::common::make_state_with_leader;

const METHODS: &[&str] = &["get", "post", "put", "patch", "delete"];

async fn fetch_spec() -> Value {
    let app = exspeed_api::handlers::build_router(make_state_with_leader(true).await);
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/openapi.json")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.headers()["content-type"], "application/json");
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

/// (method, path) of every operation in the spec.
fn spec_routes(spec: &Value) -> BTreeSet<(String, String)> {
    let mut out = BTreeSet::new();
    for (path, item) in spec["paths"].as_object().unwrap() {
        for method in item.as_object().unwrap().keys() {
            if METHODS.contains(&method.as_str()) {
                out.insert((method.clone(), path.clone()));
            }
        }
    }
    out
}

/// (method, path) of every `.route(...)` call in `build_router`, read from
/// its source so a new route can't be added without this test seeing it.
/// Axum's `{*rest}` wildcard is written `{rest}` in OpenAPI.
fn router_routes() -> BTreeSet<(String, String)> {
    let src = include_str!("../../src/handlers/mod.rs");
    let body = &src[src.find("pub fn build_router").expect("build_router")..];
    let mut out = BTreeSet::new();
    let mut rest = body;
    while let Some(i) = rest.find(".route(") {
        rest = &rest[i + ".route(".len()..];
        // The call's argument list, up to the matching parenthesis.
        let mut depth = 1usize;
        let mut end = 0;
        for (j, c) in rest.char_indices() {
            match c {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        end = j;
                        break;
                    }
                }
                _ => {}
            }
        }
        let args = &rest[..end];
        let q1 = args.find('"').expect("route path literal");
        let q2 = q1 + 1 + args[q1 + 1..].find('"').unwrap();
        let path = args[q1 + 1..q2].replace("{*", "{");
        let handlers = &args[q2 + 1..];
        let mut found = false;
        for m in METHODS {
            // `get(handler)` or `.get(handler)`.
            let needle = format!("{m}(");
            let mut from = 0;
            while let Some(k) = handlers[from..].find(&needle) {
                let at = from + k;
                let prev = handlers[..at].chars().last();
                if prev.is_none_or(|c| !(c.is_alphanumeric() || c == '_')) {
                    out.insert((m.to_string(), path.clone()));
                    found = true;
                }
                from = at + needle.len();
            }
        }
        assert!(found, "no method found for route {path}");
    }
    out
}

#[tokio::test]
async fn spec_is_served_and_lists_the_main_paths() {
    let spec = fetch_spec().await;
    assert!(spec["openapi"].as_str().unwrap().starts_with("3.1"));
    let routes = spec_routes(&spec);
    for (m, p) in [
        ("get", "/api/v1/streams"),
        ("post", "/api/v1/streams"),
        ("post", "/api/v1/streams/{name}/publish"),
        ("get", "/api/v1/streams/{name}/records"),
        ("post", "/api/v1/consumers/{name}/seek"),
        ("post", "/api/v1/queries"),
        ("get", "/api/v1/backup"),
        ("get", "/api/v1/openapi.json"),
        ("get", "/healthz"),
    ] {
        assert!(
            routes.contains(&(m.to_string(), p.to_string())),
            "{m} {p} missing from the spec"
        );
    }
    for schema in [
        "StreamInfo",
        "PublishResponse",
        "ConsumerSpec",
        "ConsumerInfo",
        "ErrorBody",
    ] {
        assert!(
            spec["components"]["schemas"][schema].is_object(),
            "schema {schema} missing"
        );
    }
    // The backup is documented as a binary tar download.
    assert!(
        spec["paths"]["/api/v1/backup"]["get"]["responses"]["200"]["content"]["application/x-tar"]
            .is_object()
    );
    // The publish body references the schema with its fields.
    let publish = &spec["paths"]["/api/v1/streams/{name}/publish"]["post"];
    assert_eq!(
        publish["requestBody"]["content"]["application/json"]["schema"]["$ref"],
        "#/components/schemas/PublishBody"
    );
}

#[tokio::test]
async fn every_route_in_the_router_is_in_the_spec_and_vice_versa() {
    let spec = spec_routes(&fetch_spec().await);
    let router = router_routes();
    assert!(
        router.len() >= 35,
        "parser found only {} routes",
        router.len()
    );
    let undocumented: Vec<_> = router.difference(&spec).collect();
    assert!(
        undocumented.is_empty(),
        "routes missing from the OpenAPI spec: {undocumented:?}"
    );
    let unrouted: Vec<_> = spec.difference(&router).collect();
    assert!(
        unrouted.is_empty(),
        "spec operations with no route: {unrouted:?}"
    );
}

/// Every documented operation is actually answered by the router (axum's
/// own "no route" answers are an empty 404 or a 405).
#[tokio::test]
async fn every_spec_operation_reaches_a_handler() {
    let spec = fetch_spec().await;
    let app = exspeed_api::handlers::build_router(make_state_with_leader(true).await);
    for (method, path) in spec_routes(&spec) {
        let uri = path
            .split('/')
            .map(|seg| {
                if seg.starts_with('{') {
                    "no-such-thing"
                } else {
                    seg
                }
            })
            .collect::<Vec<_>>()
            .join("/");
        let m = Method::from_bytes(method.to_uppercase().as_bytes()).unwrap();
        let req = Request::builder()
            .method(m)
            .uri(&uri)
            .header("content-type", "application/json")
            .body(Body::from("{}"))
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        let status = resp.status();
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        assert_ne!(status, StatusCode::METHOD_NOT_ALLOWED, "{method} {uri}");
        if status == StatusCode::NOT_FOUND {
            assert!(!body.is_empty(), "{method} {uri}: unrouted 404");
        }
    }
}
