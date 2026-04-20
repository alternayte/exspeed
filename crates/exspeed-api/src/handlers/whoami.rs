use std::sync::Arc;

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use exspeed_common::auth::Identity;
use serde::Serialize;

#[derive(Serialize)]
struct WhoamiResponse {
    name: String,
    permissions: Vec<PermissionJson>,
}

#[derive(Serialize)]
struct PermissionJson {
    streams: String,
    actions: Vec<&'static str>,
}

/// Returns the authenticated identity's name + permissions. When auth is
/// globally disabled, reports a synthetic `anonymous` identity with full
/// permissions so client-side debugging works consistently regardless of
/// broker config.
pub async fn whoami(identity: Option<Extension<Arc<Identity>>>) -> Response {
    let Some(Extension(id)) = identity else {
        let body = WhoamiResponse {
            name: "anonymous".to_string(),
            permissions: vec![PermissionJson {
                streams: "*".to_string(),
                actions: vec!["publish", "subscribe", "admin"],
            }],
        };
        return (StatusCode::OK, Json(body)).into_response();
    };
    let perms = id
        .permissions
        .iter()
        .map(|p| PermissionJson {
            streams: p.streams.as_str().to_string(),
            actions: {
                use exspeed_common::auth::Action;
                let mut v = Vec::new();
                if p.actions.contains(Action::Publish) {
                    v.push("publish");
                }
                if p.actions.contains(Action::Subscribe) {
                    v.push("subscribe");
                }
                if p.actions.contains(Action::Admin) {
                    v.push("admin");
                }
                v
            },
        })
        .collect();
    (
        StatusCode::OK,
        Json(WhoamiResponse {
            name: id.name.clone(),
            permissions: perms,
        }),
    )
        .into_response()
}
