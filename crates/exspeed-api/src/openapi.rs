//! OpenAPI 3.1 description of the HTTP API, served at
//! `GET /api/v1/openapi.json`.
//!
//! Handlers carry `#[utoipa::path]` annotations next to their code. Types
//! owned by other crates (consumer specs, query and connector info) are
//! described here by mirror schemas; tests check that each mirror lists
//! the same fields as the real type serializes, so they can't drift
//! silently. The lease and follower routes belong to the HA layer and are
//! documented by stubs at the bottom of this file.

use std::sync::OnceLock;

use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use utoipa::openapi::security::{HttpAuthScheme, HttpBuilder, SecurityScheme};
use utoipa::{Modify, OpenApi, ToSchema};

use crate::handlers;

/// Error body returned by most endpoints.
#[derive(Serialize, ToSchema)]
pub struct ErrorBody {
    /// Human-readable message.
    pub error: String,
}

/// Raw bytes (`type: string, format: binary`).
#[derive(ToSchema)]
#[schema(value_type = String, format = Binary)]
#[allow(dead_code)]
pub struct BinaryBody(Vec<u8>);

/// Error body of ExQL endpoints (queries, tables, connections).
#[derive(Serialize, ToSchema)]
pub struct ExqlErrorBody {
    pub error: String,
    /// `PARSE_ERROR`, `PLAN_ERROR`, `UNSUPPORTED`, `NOT_FOUND`, `TIMEOUT`,
    /// `CONFLICT`, `MEMORY_LIMIT`, `NOT_LEADER`, ...
    pub code: String,
    /// 1-based position of a parse error.
    pub line: Option<u64>,
    pub column: Option<u64>,
}

// ---------------------------------------------------------------------------
// Mirrors of types owned by other crates
// ---------------------------------------------------------------------------

/// Where a new consumer starts reading.
#[derive(ToSchema)]
#[schema(as = DeliverPolicy)]
#[allow(dead_code)]
#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliverPolicyDoc {
    /// From the first retained record (default).
    All,
    /// Only records appended after creation.
    New,
    /// From this offset.
    FromOffset(u64),
    /// From the first record at or after this time (Unix ms).
    FromTime(u64),
}

/// Whether delivered records must be acknowledged.
#[derive(ToSchema, Serialize)]
#[schema(as = AckPolicy)]
#[serde(rename_all = "snake_case")]
#[allow(dead_code)]
pub enum AckPolicyDoc {
    /// Ack each record; unacked records are redelivered (default).
    Explicit,
    /// At-most-once: records count as acked when delivered.
    None,
}

/// A durable consumer's definition (`exspeed_protocol::client::ConsumerSpec`).
#[derive(ToSchema, Serialize)]
#[schema(as = ConsumerSpec)]
#[allow(dead_code)]
pub struct ConsumerSpecDoc {
    pub name: String,
    pub stream: String,
    /// NATS-style subject filters (`orders.*`, `orders.>`); empty = all.
    #[schema(default = json!([]))]
    pub filter_subjects: Vec<String>,
    #[schema(value_type = Option<DeliverPolicyDoc>)]
    pub deliver: DeliverPolicyDoc,
    #[schema(value_type = Option<AckPolicyDoc>)]
    pub ack: AckPolicyDoc,
    /// Redeliver a record not acked within this time.
    #[schema(default = 30000)]
    pub ack_wait_ms: Option<u64>,
    /// Dead-letter after this many deliveries; 0 = never.
    #[schema(default = 5)]
    pub max_deliver: Option<u32>,
    /// Redelivery delays by delivery count (the last repeats).
    pub backoff_ms: Option<Vec<u64>>,
    /// Pause delivery while this many records await an ack.
    #[schema(default = 1000)]
    pub max_ack_pending: Option<u32>,
    /// Where records go after `max_deliver` attempts or a term.
    pub dlq_stream: Option<String>,
    /// Must be false over HTTP (ephemeral consumers are tied to a TCP
    /// connection).
    pub ephemeral: Option<bool>,
    /// Dead-letter records whose TTL expires before they are acked (reason
    /// `expired`) instead of dropping them.
    pub dead_letter_expired: Option<bool>,
    /// Only records whose headers have these values (omitted when empty).
    pub filter_headers: Option<std::collections::BTreeMap<String, String>>,
    /// `all` (default) or `any` of `filter_headers` must match.
    #[schema(value_type = Option<String>)]
    pub header_match: Option<String>,
    /// Deliver to one subscription at a time (the oldest connected).
    pub single_active: Option<bool>,
    /// Look this many records ahead and deliver higher `exspeed-priority`
    /// first; 0 = in order.
    pub priority_window: Option<u32>,
}

/// Consumer counters (`exspeed_broker::consumer::ConsumerStats`).
#[derive(ToSchema, Serialize)]
#[schema(as = ConsumerStats)]
#[allow(dead_code)]
pub struct ConsumerStatsDoc {
    pub delivered: u64,
    pub redelivered: u64,
    pub acked: u64,
    pub dead_lettered: u64,
    /// Unacked records removed by retention or compaction before redelivery.
    pub gone: u64,
    /// Records skipped because the consumer fell behind retention.
    pub skipped: u64,
}

/// A consumer with its live state (`exspeed_broker::consumer::ConsumerInfo`).
#[derive(ToSchema, Serialize)]
#[schema(as = ConsumerInfo)]
#[allow(dead_code)]
pub struct ConsumerInfoDoc {
    #[schema(value_type = ConsumerSpecDoc)]
    pub spec: ConsumerSpecDoc,
    /// Next offset to be delivered for the first time.
    pub next_offset: u64,
    /// Everything below this offset is acked (or filtered out).
    pub ack_floor: u64,
    pub num_unacked: u64,
    pub num_in_flight: u64,
    /// Records held back until their delivery time.
    pub num_delayed: u64,
    /// Records not yet delivered (approximate).
    pub num_waiting: u64,
    /// `high_watermark - ack_floor`.
    pub lag: u64,
    pub subscribers: u64,
    pub pull_waiters: u64,
    #[schema(value_type = ConsumerStatsDoc)]
    pub stats: ConsumerStatsDoc,
}

/// A continuous query (`exspeed_processing::QueryInfo`).
#[derive(ToSchema, Serialize)]
#[schema(as = QueryInfo)]
#[allow(dead_code)]
pub struct QueryInfoDoc {
    pub id: String,
    /// Same as `id`; present on `CREATE` responses.
    pub query_id: Option<String>,
    pub sql: String,
    /// `stream` or `table`.
    pub kind: String,
    pub name: String,
    /// Stream the query writes (a table's changelog for tables).
    pub target_stream: String,
    /// `running`, `paused`, `pending` or `failed`.
    pub status: String,
    /// `running`, `paused` or `stopped`.
    pub desired_state: String,
    pub error: Option<String>,
    pub created_at: String,
    /// `records_in`, `records_out`, `late_records_dropped`, `checkpoints`,
    /// `watermark`, `last_checkpoint`.
    #[schema(value_type = Object)]
    pub stats: serde_json::Value,
}

/// A materialized table (`exspeed_processing::TableInfo`).
#[derive(ToSchema, Serialize)]
#[schema(as = TableInfo)]
#[allow(dead_code)]
pub struct TableInfoDoc {
    pub name: String,
    pub query_id: String,
    pub columns: Vec<String>,
    pub row_count: u64,
}

/// A connector with its live status (`exspeed_connectors::ConnectorInfo`).
/// The status fields (`status`, `last_error`, `restart_count`, `lag`,
/// `last_success_ms`, `checkpoint`, ...) are listed in docs/connectors.md.
#[derive(ToSchema, Serialize)]
#[schema(as = ConnectorInfo)]
#[allow(dead_code)]
pub struct ConnectorInfoDoc {
    pub name: String,
    /// `source` or `sink`.
    pub connector_type: String,
    pub plugin: String,
    pub stream: String,
    /// `api` or `file`.
    pub origin: String,
    /// Defining file under `connectors.d/` for file connectors.
    pub file: Option<String>,
    pub status: String,
    /// `GET /api/v1/connectors/{name}` only: the config with `${VAR}`
    /// references unresolved.
    #[schema(value_type = Option<Object>)]
    pub config: Option<serde_json::Value>,
}

/// Lease record (`GET /api/v1/leases`).
#[derive(ToSchema, Serialize)]
#[allow(dead_code)]
pub struct LeaseRecord {
    pub name: String,
    /// Node id of the holder.
    pub holder: String,
    /// Fencing token, incremented on every acquisition.
    pub epoch: u64,
    /// RFC 3339.
    pub expires_at: String,
    /// `host:port` followers replicate from, or null.
    pub replication_endpoint: Option<String>,
    /// `host:port` clients are redirected to, or null.
    pub client_endpoint: Option<String>,
    /// In-sync replicas (node ids, the holder included).
    pub isr: Vec<String>,
}

// ---------------------------------------------------------------------------
// The document
// ---------------------------------------------------------------------------

struct BearerAuth;

impl Modify for BearerAuth {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        let components = openapi.components.get_or_insert_with(Default::default);
        components.add_security_scheme(
            "bearer",
            SecurityScheme::Http(
                HttpBuilder::new()
                    .scheme(HttpAuthScheme::Bearer)
                    .description(Some(
                        "Required on /api/v1/* when auth is enabled (credentials file or \
                         auth token). Every route except whoami and openapi.json needs an \
                         admin permission.",
                    ))
                    .build(),
            ),
        );
    }
}

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Exspeed HTTP API",
        description = "Management API of the Exspeed stream-processing broker. Applications publish and consume over the binary TCP protocol (docs/protocol.md); this API manages streams, consumers, queries, connectors and operations. In a cluster, followers answer 503 (with the leader's address) on /api/v1/* except cluster, leases, whoami and openapi.json.",
        license(name = "MIT")
    ),
    modifiers(&BearerAuth),
    paths(
        handlers::health::healthz,
        handlers::health::readyz,
        handlers::metrics::prometheus_metrics,
        handlers::webhooks::handle_webhook,
        handlers::streams::list_streams,
        handlers::streams::create_stream,
        handlers::streams::get_stream,
        handlers::streams::patch_stream,
        handlers::streams::delete_stream,
        handlers::streams::publish_to_stream,
        handlers::streams::read_records,
        handlers::consumers::list_consumers,
        handlers::consumers::create_consumer,
        handlers::consumers::get_consumer,
        handlers::consumers::delete_consumer,
        handlers::consumers::seek_consumer,
        handlers::connectors::list_connectors,
        handlers::connectors::create_connector,
        handlers::connectors::get_connector,
        handlers::connectors::update_connector,
        handlers::connectors::delete_connector,
        handlers::connectors::restart_connector,
        handlers::views::list_views,
        handlers::views::create_view,
        handlers::views::get_view,
        handlers::queries::execute_query,
        handlers::queries::create_continuous,
        handlers::queries::list_queries,
        handlers::queries::get_query,
        handlers::queries::delete_query,
        handlers::queries::pause_query,
        handlers::queries::resume_query,
        handlers::connections::list_connections,
        handlers::connections::create_connection,
        handlers::connections::delete_connection,
        handlers::whoami::whoami,
        handlers::kv::create_bucket,
        handlers::kv::list_keys,
        handlers::kv::get_key,
        handlers::kv::put_key,
        handlers::kv::delete_key,
        handlers::kv::key_history,
        handlers::backup::backup,
        openapi_json,
        handlers::leases::list_leases,
        handlers::cluster::status,
    ),
    tags(
        (name = "health", description = "Probes and metrics (no auth)"),
        (name = "streams", description = "Streams, HTTP publish and record browsing"),
        (name = "consumers", description = "Durable consumers (delivery itself is TCP-only)"),
        (name = "queries", description = "ExQL statements and continuous queries"),
        (name = "views", description = "Materialized tables"),
        (name = "kv", description = "Key-value buckets"),
        (name = "connectors", description = "Source and sink connectors"),
        (name = "connections", description = "External databases for bounded queries"),
        (name = "webhooks", description = "Ingest through http_webhook connectors"),
        (name = "cluster", description = "Identity, leases and replication"),
        (name = "operations", description = "Backup and API description"),
    )
)]
pub struct ApiDoc;

/// The OpenAPI document, with the server's version.
pub fn spec() -> utoipa::openapi::OpenApi {
    let mut doc = ApiDoc::openapi();
    doc.info.version = env!("CARGO_PKG_VERSION").to_string();
    doc
}

/// `GET /api/v1/openapi.json` — this document. No auth required.
#[utoipa::path(
    get,
    path = "/api/v1/openapi.json",
    tag = "operations",
    responses((status = 200, description = "OpenAPI 3.1 document", content_type = "application/json", body = Object))
)]
pub async fn openapi_json() -> Response {
    static JSON: OnceLock<String> = OnceLock::new();
    let body = JSON.get_or_init(|| spec().to_pretty_json().expect("serialize OpenAPI document"));
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        body.as_str(),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use serde_json::{json, Value};

    use super::*;

    fn doc() -> Value {
        serde_json::to_value(spec()).unwrap()
    }

    fn schema_props(doc: &Value, name: &str) -> BTreeSet<String> {
        doc["components"]["schemas"][name]["properties"]
            .as_object()
            .unwrap_or_else(|| panic!("schema {name} has no properties"))
            .keys()
            .cloned()
            .collect()
    }

    fn keys(v: Value) -> BTreeSet<String> {
        v.as_object().unwrap().keys().cloned().collect()
    }

    /// Each mirror schema lists exactly the fields the real type
    /// serializes (plus documented extras), so a field added to the real
    /// type fails here until the schema is updated.
    #[test]
    fn mirror_schemas_match_the_real_types() {
        let doc = doc();
        let mut spec = exspeed_protocol::client::ConsumerSpec::new("c", "s");
        // Serialized only when set.
        spec.filter_headers.insert("k".into(), "v".into());
        assert_eq!(
            schema_props(&doc, "ConsumerSpec"),
            keys(serde_json::to_value(&spec).unwrap())
        );
        let stats = exspeed_broker::consumer::core::ConsumerStats::default();
        assert_eq!(
            schema_props(&doc, "ConsumerStats"),
            keys(serde_json::to_value(stats).unwrap())
        );
        let info = exspeed_broker::consumer::ConsumerInfo {
            spec,
            next_offset: 0,
            ack_floor: 0,
            num_unacked: 0,
            num_in_flight: 0,
            num_delayed: 0,
            num_waiting: 0,
            lag: 0,
            subscribers: 0,
            pull_waiters: 0,
            stats,
        };
        assert_eq!(
            schema_props(&doc, "ConsumerInfo"),
            keys(serde_json::to_value(&info).unwrap())
        );
        let q = exspeed_processing::engine::QueryInfo {
            id: "q".into(),
            sql: "".into(),
            kind: exspeed_processing::engine::QueryKind::Stream,
            name: "n".into(),
            target_stream: "t".into(),
            status: "running".into(),
            desired_state: exspeed_processing::engine::DesiredState::Running,
            error: None,
            created_at: "".into(),
            stats: json!({}),
        };
        let mut q = keys(serde_json::to_value(&q).unwrap());
        q.insert("query_id".into()); // added by CREATE responses
        assert_eq!(schema_props(&doc, "QueryInfo"), q);
        let t = exspeed_processing::engine::TableInfo {
            name: "t".into(),
            query_id: "q".into(),
            columns: vec![],
            row_count: 0,
        };
        assert_eq!(
            schema_props(&doc, "TableInfo"),
            keys(serde_json::to_value(&t).unwrap())
        );
    }

    #[test]
    fn document_is_openapi_3_1_with_bearer_auth() {
        let doc = doc();
        assert!(doc["openapi"].as_str().unwrap().starts_with("3.1"));
        assert_eq!(doc["info"]["version"], env!("CARGO_PKG_VERSION"));
        assert_eq!(
            doc["components"]["securitySchemes"]["bearer"]["scheme"],
            "bearer"
        );
        // Every operation is tagged and has at least one response.
        for (path, item) in doc["paths"].as_object().unwrap() {
            for (method, op) in item.as_object().unwrap() {
                assert!(
                    op["tags"].as_array().is_some_and(|t| !t.is_empty()),
                    "{method} {path}"
                );
                assert!(
                    op["responses"].as_object().is_some_and(|r| !r.is_empty()),
                    "{method} {path}"
                );
            }
        }
    }
}
