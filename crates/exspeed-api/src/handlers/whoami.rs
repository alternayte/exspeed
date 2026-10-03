use std::sync::Arc;

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use exspeed_common::auth::Identity;
use serde::Serialize;

/// The caller's identity.
#[derive(Serialize, utoipa::ToSchema)]
pub struct WhoamiResponse {
    name: String,
    permissions: Vec<PermissionJson>,
}

#[derive(Serialize, utoipa::ToSchema)]
pub struct PermissionJson {
    /// Stream glob (`*`, `orders-*`).
    streams: String,
    /// `publish`, `subscribe`, `admin`.
    #[schema(value_type = Vec<String>)]
    actions: Vec<&'static str>,
}

/// Returns the authenticated identity's name + permissions. When auth is
/// globally disabled, reports a synthetic `anonymous` identity with full
/// permissions so client-side debugging works consistently regardless of
/// broker config.
#[utoipa::path(
    get,
    path = "/api/v1/whoami",
    tag = "cluster",
    security(("bearer" = [])),
    responses(
        (status = 200, description = "Name and permissions (anonymous with full permissions when auth is off)", body = WhoamiResponse),
        (status = 401, description = "Missing or unknown token", body = crate::openapi::ErrorBody),
    )
)]
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
