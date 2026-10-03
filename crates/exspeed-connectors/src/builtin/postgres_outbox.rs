//! `postgres_outbox`: the transactional-outbox pattern.
//!
//! Applications insert events into an outbox table in the same transaction
//! as their state change; this source publishes them.
//!
//! - `mode = "poll"` (default) reads the table; `mode = "cdc"` streams its
//!   inserts through a replication slot.
//! - Column types: the id may be int4/int8/uuid/text, the payload text,
//!   json or jsonb — everything is cast to text in SQL.
//! - Every record carries `x-idempotency-key = pgoutbox:<schema.table>:<id>`
//!   (namespaced so two outbox tables with overlapping ids never collide in
//!   one stream's dedup window), so a replay
//!   after a crash is dropped by the broker (effectively-once within the
//!   stream's dedup window).
//! - `cleanup = "delete"` (default) deletes published rows in `ack()`, i.e.
//!   only after the records are durable.
//!
//! Commit-order caveat (poll mode with `cleanup = "none"`): the cursor is
//! `id > last_id`, so a row from a transaction that commits after a row
//! with a higher id was already polled is skipped. With `cleanup =
//! "delete"` there is no cursor (every remaining row is unpublished), so
//! late commits are picked up on the next poll; CDC mode delivers in commit
//! order. Prefer either for strict completeness.

use async_trait::async_trait;
use bytes::Bytes;
use serde::Deserialize;
use serde_json::Value;
use tokio::time::Instant;
use tokio_postgres::Client;
use tracing::{info, warn};

use crate::builtin::pg::{self, quote_ident, TableRef};
use crate::builtin::postgres_cdc::{tuple_object, CdcStream, ChangeKind, Step};
use crate::registry::PluginInit;
use crate::settings::{self, de};
use crate::traits::{ConnectorError, Lag, SourceBatch, SourceConnector, SourceRecord};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutboxMode {
    Poll,
    Cdc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Cleanup {
    Delete,
    None,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutboxSettings {
    pub connection: String,
    #[serde(default = "default_mode")]
    pub mode: OutboxMode,
    #[serde(default = "default_table", alias = "outbox_table")]
    pub table: String,
    #[serde(default = "default_id")]
    pub id_column: String,
    #[serde(default = "default_key")]
    pub key_column: String,
    #[serde(default = "default_agg")]
    pub aggregate_type_column: String,
    #[serde(default = "default_evt")]
    pub event_type_column: String,
    #[serde(default = "default_payload")]
    pub payload_column: String,
    /// Poll mode with `cleanup = "delete"`: delivery order (default: the id).
    #[serde(default)]
    pub order_column: Option<String>,
    #[serde(default = "default_cleanup", alias = "cleanup_mode")]
    pub cleanup: Cleanup,
    #[serde(default)]
    pub slot_name: Option<String>,
    #[serde(default)]
    pub publication_name: Option<String>,
    #[serde(default, deserialize_with = "de::bool")]
    pub drop_slot_on_delete: bool,
}

fn default_mode() -> OutboxMode {
    OutboxMode::Poll
}
fn default_table() -> String {
    "outbox_events".into()
}
fn default_id() -> String {
    "id".into()
}
fn default_key() -> String {
    "aggregate_id".into()
}
fn default_agg() -> String {
    "aggregate_type".into()
}
fn default_evt() -> String {
    "event_type".into()
}
fn default_payload() -> String {
    "payload".into()
}
fn default_cleanup() -> Cleanup {
    Cleanup::Delete
}

/// One outbox row, decoded.
struct OutboxRow {
    id: String,
    key: Option<String>,
    aggregate_type: String,
    event_type: String,
    payload: String,
}

pub struct PostgresOutboxSource {
    s: OutboxSettings,
    table: TableRef,
    subject_template: String,
    client: Option<Client>,
    /// SQL type of the id column (`bigint`, `uuid`, …).
    id_type: String,
    select_sql: String,
    cursor: Option<String>,
    pending: Vec<String>,
    cdc: Option<CdcStream>,
    publication_is_derived: bool,
}

impl PostgresOutboxSource {
    pub fn new(init: &PluginInit) -> Result<Self, ConnectorError> {
        let s: OutboxSettings = settings::parse("postgres_outbox", &init.settings)?;
        pg::validate_connection_string(&s.connection)?;
        if s.mode == OutboxMode::Poll {
            // These only mean something with a replication slot; accepting
            // them in poll mode hides a config that silently doesn't do
            // what it says (e.g. no logical replication at all).
            let cdc_only = [
                ("slot_name", s.slot_name.is_some()),
                ("publication_name", s.publication_name.is_some()),
                ("drop_slot_on_delete", s.drop_slot_on_delete),
            ];
            if let Some((name, _)) = cdc_only.iter().find(|(_, set)| *set) {
                return Err(ConnectorError::config(format!(
                    "postgres_outbox: '{name}' only applies to mode = \"cdc\" \
                     (this connector polls the table; set mode = \"cdc\" or remove it)"
                )));
            }
        }
        let table = TableRef::parse(&s.table)?;
        let slot = s
            .slot_name
            .clone()
            .unwrap_or_else(|| pg::default_slot_name(&init.config.name));
        pg::validate_slot_name(&slot)?;
        let publication_is_derived = s.publication_name.is_none();
        let publication = s
            .publication_name
            .clone()
            .unwrap_or_else(|| pg::default_publication_name(&init.config.name));
        let cdc = (s.mode == OutboxMode::Cdc)
            .then(|| CdcStream::new(s.connection.clone(), slot, publication, vec![table.clone()]));
        Ok(Self {
            subject_template: if init.config.subject_template.is_empty() {
                "{aggregate_type}.{event_type}".into()
            } else {
                init.config.subject_template.clone()
            },
            table,
            s,
            client: None,
            id_type: String::new(),
            select_sql: String::new(),
            cursor: None,
            pending: Vec::new(),
            cdc,
            publication_is_derived,
        })
    }

    fn record(&self, row: OutboxRow) -> SourceRecord {
        let payload_json: Option<Value> = serde_json::from_str(&row.payload).ok();
        let subject = crate::subject::render(
            &self.subject_template,
            &[
                ("aggregate_type", &row.aggregate_type),
                ("event_type", &row.event_type),
                ("table", &self.table.table),
            ],
            payload_json.as_ref(),
        );
        SourceRecord {
            key: row.key.map(|k| Bytes::from(k.into_bytes())),
            value: Bytes::from(row.payload.into_bytes()),
            subject,
            headers: vec![
                (
                    "x-idempotency-key".into(),
                    format!("pgoutbox:{}:{}", self.table, row.id),
                ),
                ("x-aggregate-type".into(), row.aggregate_type),
                ("x-event-type".into(), row.event_type),
                ("x-exspeed-source".into(), "postgres_outbox".into()),
            ],
        }
    }

    async fn column_type(&self, client: &Client, column: &str) -> Result<String, ConnectorError> {
        let row = client
            .query_opt(
                "SELECT format_type(a.atttypid, a.atttypmod) FROM pg_attribute a \
                 WHERE a.attrelid = ($1::text)::regclass AND a.attname = $2 AND NOT a.attisdropped",
                &[&self.table.quoted(), &column],
            )
            .await
            .map_err(|e| pg::classify(&e, "read outbox columns"))?;
        row.map(|r| r.get::<_, String>(0)).ok_or_else(|| {
            ConnectorError::fatal(format!(
                "postgres_outbox: column '{column}' not found in {}",
                self.table
            ))
        })
    }

    async fn poll_table(&mut self, max_batch: usize) -> Result<SourceBatch, ConnectorError> {
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| ConnectorError::connection("postgres_outbox: not started"))?;
        let limit = max_batch.max(1) as i64;
        let rows = match (&self.cursor, self.s.cleanup) {
            (Some(c), Cleanup::None) => client
                .query(&self.select_sql, &[c, &limit])
                .await
                .map_err(|e| pg::classify(&e, "poll outbox"))?,
            (None, Cleanup::None) => client
                .query(&self.select_sql, &[&None::<String>, &limit])
                .await
                .map_err(|e| pg::classify(&e, "poll outbox"))?,
            (_, Cleanup::Delete) => client
                .query(&self.select_sql, &[&limit])
                .await
                .map_err(|e| pg::classify(&e, "poll outbox"))?,
        };
        let mut records = Vec::with_capacity(rows.len());
        self.pending.clear();
        for r in rows {
            let row = OutboxRow {
                id: r.get::<_, Option<String>>(0).unwrap_or_default(),
                key: r.get(1),
                aggregate_type: r.get::<_, Option<String>>(2).unwrap_or_default(),
                event_type: r.get::<_, Option<String>>(3).unwrap_or_default(),
                payload: r
                    .get::<_, Option<String>>(4)
                    .unwrap_or_else(|| "null".into()),
            };
            self.pending.push(row.id.clone());
            records.push(self.record(row));
        }
        let checkpoint = match (self.s.cleanup, self.pending.last()) {
            (Cleanup::None, Some(last)) => {
                self.cursor = Some(last.clone());
                Some(last.clone())
            }
            _ => None,
        };
        Ok(SourceBatch {
            records,
            checkpoint,
        })
    }

    fn cdc_row(&self, c: &crate::builtin::postgres_cdc::Change) -> Option<OutboxRow> {
        let ChangeKind::Insert { new } = &c.kind else {
            return None; // the outbox only publishes inserts
        };
        let (obj, _) = tuple_object(&c.relation, new, false);
        let text = |col: &str| -> Option<String> {
            obj.get(col).and_then(|v| match v {
                Value::Null => None,
                Value::String(s) => Some(s.clone()),
                other => Some(other.to_string()),
            })
        };
        Some(OutboxRow {
            id: text(&self.s.id_column)?,
            key: text(&self.s.key_column),
            aggregate_type: text(&self.s.aggregate_type_column).unwrap_or_default(),
            event_type: text(&self.s.event_type_column).unwrap_or_default(),
            payload: text(&self.s.payload_column).unwrap_or_else(|| "null".into()),
        })
    }
}

#[async_trait]
impl SourceConnector for PostgresOutboxSource {
    async fn start(&mut self, checkpoint: Option<String>) -> Result<(), ConnectorError> {
        let client = pg::connect(&self.s.connection).await?;
        self.id_type = self.column_type(&client, &self.s.id_column).await?;
        for col in [
            &self.s.key_column,
            &self.s.aggregate_type_column,
            &self.s.event_type_column,
            &self.s.payload_column,
        ] {
            self.column_type(&client, col).await?;
        }
        let t = self.table.quoted();
        let id = quote_ident(&self.s.id_column);
        let cols = format!(
            "{id}::text, {}::text, {}::text, {}::text, {}::text",
            quote_ident(&self.s.key_column),
            quote_ident(&self.s.aggregate_type_column),
            quote_ident(&self.s.event_type_column),
            quote_ident(&self.s.payload_column),
        );
        self.select_sql = match self.s.cleanup {
            Cleanup::Delete => {
                let order =
                    quote_ident(self.s.order_column.as_deref().unwrap_or(&self.s.id_column));
                format!("SELECT {cols} FROM {t} ORDER BY {order} LIMIT $1")
            }
            Cleanup::None => {
                if !matches!(self.id_type.as_str(), "integer" | "bigint" | "smallint")
                    && self.s.mode == OutboxMode::Poll
                {
                    return Err(ConnectorError::fatal(format!(
                        "postgres_outbox: cleanup = \"none\" polls by `id > last_id`, which needs an \
                         increasing integer id, but '{}' is {}; use cleanup = \"delete\" or mode = \"cdc\"",
                        self.s.id_column, self.id_type
                    )));
                }
                format!(
                    "SELECT {cols} FROM {t} WHERE ($1::text IS NULL OR {id} > ($1::text)::{}) \
                     ORDER BY {id} LIMIT $2",
                    self.id_type
                )
            }
        };
        self.client = Some(client);
        self.pending.clear();
        match &mut self.cdc {
            Some(cdc) => cdc.start(checkpoint.as_deref()).await?,
            None => self.cursor = checkpoint,
        }
        info!(table = %self.table, mode = ?self.s.mode, "postgres_outbox started");
        Ok(())
    }

    async fn poll(&mut self, max_batch: usize) -> Result<SourceBatch, ConnectorError> {
        if self.cdc.is_none() {
            return self.poll_table(max_batch).await;
        }
        let started = Instant::now();
        let mut records = Vec::new();
        let mut checkpoint = None;
        loop {
            if records.len() >= max_batch {
                break;
            }
            let deadline = if records.is_empty() && checkpoint.is_none() {
                started + std::time::Duration::from_secs(1)
            } else {
                (Instant::now() + std::time::Duration::from_millis(20))
                    .min(started + std::time::Duration::from_secs(1))
            };
            let step = self.cdc.as_mut().unwrap().next(deadline).await?;
            match step {
                Step::Change(c) => {
                    if let Some(row) = self.cdc_row(&c) {
                        self.pending.push(row.id.clone());
                        records.push(self.record(row));
                    }
                }
                Step::Commit(lsn) | Step::Idle(lsn) => checkpoint = Some(lsn.to_string()),
                Step::Timeout => break,
            }
        }
        Ok(SourceBatch {
            records,
            checkpoint,
        })
    }

    async fn ack(&mut self, checkpoint: Option<&str>) -> Result<(), ConnectorError> {
        if self.s.cleanup == Cleanup::Delete && !self.pending.is_empty() {
            let client = self
                .client
                .as_ref()
                .ok_or_else(|| ConnectorError::connection("postgres_outbox: not started"))?;
            let sql = format!(
                "DELETE FROM {} WHERE {} = ANY(($1::text[])::{}[])",
                self.table.quoted(),
                quote_ident(&self.s.id_column),
                self.id_type
            );
            client.execute(&sql, &[&self.pending]).await.map_err(|e| {
                match pg::classify(&e, "delete published outbox rows") {
                    ConnectorError::Poison(_) => ConnectorError::fatal(e.to_string()),
                    other => other,
                }
            })?;
            self.pending.clear();
        } else {
            self.pending.clear();
        }
        if let Some(cdc) = &mut self.cdc {
            cdc.ack(checkpoint)?;
        }
        Ok(())
    }

    async fn stop(&mut self) -> Result<(), ConnectorError> {
        if let Some(cdc) = &mut self.cdc {
            cdc.stop().await;
        }
        self.client = None;
        Ok(())
    }

    fn lag(&self) -> Option<Lag> {
        self.cdc.as_ref().and_then(|c| c.lag())
    }

    async fn dry_run(&mut self, max: usize) -> Result<Vec<SourceRecord>, ConnectorError> {
        if let Some(cdc) = &self.cdc {
            let changes = cdc.peek(max.max(1) * 4).await?;
            return Ok(changes
                .iter()
                .filter_map(|c| self.cdc_row(c))
                .map(|r| self.record(r))
                .take(max)
                .collect());
        }
        // Poll mode is read-only until ack(); never ack here.
        self.start(None).await?;
        let batch = self.poll_table(max).await;
        self.pending.clear();
        let _ = self.stop().await;
        Ok(batch?.records.into_iter().take(max).collect())
    }

    async fn cleanup(&mut self) -> Result<(), ConnectorError> {
        if let (Some(cdc), true) = (&self.cdc, self.s.drop_slot_on_delete) {
            if let Err(e) = cdc.drop_slot(self.publication_is_derived).await {
                warn!(error = %e, "postgres_outbox: dropping the slot failed");
                return Err(e);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ConnectorConfig, ConnectorType};
    use serde_json::json;

    fn init(settings: Value) -> PluginInit {
        let (m, _) = exspeed_common::Metrics::new();
        PluginInit {
            config: ConnectorConfig::new("o", ConnectorType::Source, "postgres_outbox", "s"),
            settings: settings.as_object().unwrap().clone(),
            metrics: std::sync::Arc::new(m),
        }
    }

    #[test]
    fn settings() {
        let s = PostgresOutboxSource::new(&init(json!({"connection": "postgres://h/db", "outbox_table": "app.outbox", "cleanup_mode": "none"}))).unwrap();
        assert_eq!(s.table.to_string(), "app.outbox");
        assert_eq!(s.s.cleanup, Cleanup::None);
        assert!(s.cdc.is_none());
        let c = PostgresOutboxSource::new(&init(
            json!({"connection": "postgres://h/db", "mode": "cdc"}),
        ))
        .unwrap();
        assert!(c.cdc.is_some());
        assert!(PostgresOutboxSource::new(&init(
            json!({"connection": "postgres://h/db", "mode": "stream"})
        ))
        .is_err());
        assert!(PostgresOutboxSource::new(&init(
            json!({"connection": "postgres://h/db", "outbx_table": "x"})
        ))
        .is_err());
    }

    /// N4: slot/publication settings in poll mode are a config error, not
    /// silently ignored; in CDC mode they are accepted.
    #[test]
    fn cdc_only_settings_are_rejected_in_poll_mode() {
        for (k, v) in [
            ("slot_name", json!("my_slot")),
            ("publication_name", json!("my_pub")),
            ("drop_slot_on_delete", json!(true)),
        ] {
            let mut poll = json!({"connection": "postgres://h/db"});
            poll[k] = v.clone();
            let err = PostgresOutboxSource::new(&init(poll)).err().unwrap();
            assert!(err.to_string().contains(k), "{err}");
            assert_eq!(err.kind(), crate::traits::ErrorKind::Fatal);

            let mut cdc = json!({"connection": "postgres://h/db", "mode": "cdc"});
            cdc[k] = v;
            PostgresOutboxSource::new(&init(cdc)).unwrap();
        }
        // `drop_slot_on_delete = false` is the default and fine anywhere.
        PostgresOutboxSource::new(&init(
            json!({"connection": "postgres://h/db", "drop_slot_on_delete": false}),
        ))
        .unwrap();
    }

    #[test]
    fn record_shape() {
        let s = PostgresOutboxSource::new(&init(json!({"connection": "postgres://h/db"}))).unwrap();
        let r = s.record(OutboxRow {
            id: "7".into(),
            key: Some("order-1".into()),
            aggregate_type: "order".into(),
            event_type: "created".into(),
            payload: r#"{"total": 5}"#.into(),
        });
        assert_eq!(r.subject, "order.created");
        assert_eq!(r.key.as_deref(), Some(&b"order-1"[..]));
        // Namespaced by table: two outbox tables (or two connectors writing
        // one stream) with overlapping ids must not dedupe each other.
        assert!(r.headers.contains(&(
            "x-idempotency-key".into(),
            "pgoutbox:public.outbox_events:7".into()
        )));
    }
}
