//! Materialized tables (`CREATE TABLE … AS SELECT`), served under
//! `/api/v1/views` for compatibility.

use std::sync::Arc;

use axum::extract::{Extension, Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use exspeed_common::auth::Identity;
use serde::Deserialize;
use serde_json::json;

use super::queries::exql_error;
use crate::state::AppState;

#[derive(Deserialize, utoipa::ToSchema)]
pub struct CreateViewRequest {
    /// `CREATE TABLE … AS SELECT … GROUP BY …`
    pub sql: String,
}

#[derive(Deserialize, utoipa::IntoParams)]
#[into_params(parameter_in = Query)]
pub struct GetViewParams {
    /// Group key; a JSON array for several GROUP BY columns.
    pub key: Option<String>,
}

fn deny(identity: Option<Extension<Arc<Identity>>>) -> Option<Response> {
    identity.and_then(|Extension(id)| super::require_global_admin(&id))
}

/// GET /api/v1/views
#[utoipa::path(
    get,
    path = "/api/v1/views",
    tag = "views",
    security(("bearer" = [])),
    responses(
        (status = 200, description = "Tables", body = Vec<crate::openapi::TableInfoDoc>),
    )
)]
pub async fn list_views(
    State(state): State<Arc<AppState>>,
    identity: Option<Extension<Arc<Identity>>>,
) -> Response {
    if let Some(r) = deny(identity) {
        return r;
    }
    (StatusCode::OK, Json(json!(state.exql.list_tables()))).into_response()
}

/// GET /api/v1/views/{name}[?key=<k>]
///
/// All rows (`{columns, rows, row_count}`), or with `?key=` the row of one
/// group (`{columns, row}`). The key is the group value; for several
/// GROUP BY columns it is a JSON array (`["eu",3]`), and empty for a
/// global aggregate.
#[utoipa::path(
    get,
    path = "/api/v1/views/{name}",
    tag = "views",
    security(("bearer" = [])),
    params(("name" = String, Path, description = "Table name"), GetViewParams),
    responses(
        (status = 200, description = "`{columns, rows, row_count}`, or `{columns, row}` with `key`", body = Object),
        (status = 404, description = "No such table, or no row for the key", body = crate::openapi::ExqlErrorBody),
    )
)]
pub async fn get_view(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    identity: Option<Extension<Arc<Identity>>>,
    Query(params): Query<GetViewParams>,
) -> Response {
    if let Some(r) = deny(identity) {
        return r;
    }
    let res = match params.key {
        Some(key) => state.exql.table_row(&name, &key),
        None => state.exql.table_rows(&name),
    };
    match res {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => exql_error(&e),
    }
}

/// POST /api/v1/views
///
/// Create a materialized table (`CREATE TABLE … AS SELECT … GROUP BY …`;
/// `CREATE MATERIALIZED VIEW` is an alias).
#[utoipa::path(
    post,
    path = "/api/v1/views",
    tag = "views",
    security(("bearer" = [])),
    request_body = CreateViewRequest,
    responses(
        (status = 201, description = "The table's query", body = crate::openapi::QueryInfoDoc),
        (status = 400, description = "Invalid statement", body = crate::openapi::ExqlErrorBody),
        (status = 503, description = "Not the leader", body = crate::openapi::ExqlErrorBody),
    )
)]
pub async fn create_view(
    State(state): State<Arc<AppState>>,
    identity: Option<Extension<Arc<Identity>>>,
    Json(body): Json<CreateViewRequest>,
) -> Response {
    if let Some(r) = deny(identity) {
        return r;
    }
    match state.exql.create_continuous(&body.sql).await {
        Ok(info) => {
            let mut v = json!(info);
            v["query_id"] = json!(info.id);
            (StatusCode::CREATED, Json(v)).into_response()
        }
        Err(e) => exql_error(&e),
    }
}
