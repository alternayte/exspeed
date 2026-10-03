//! `postgres_poll`: poll tables by a tracking column (e.g. `updated_at`).
//!
//! Each table keeps its own cursor `(tracking_column, primary key…)`, so rows
//! that share a tracking value are never skipped:
//!
//! ```sql
//! SELECT row_to_json(t)::text, t.tc::text, t.pk::text FROM tbl t
//! WHERE (t.tc, t.pk) > ($1::text::<type>, $2::text::<type>)
//! ORDER BY t.tc, t.pk LIMIT n
//! ```
//!
//! Rows are emitted as JSON (`row_to_json`, so timestamps, numerics, uuids
//! and json decode correctly). The checkpoint is a JSON map of table →
//! cursor values (as text).
//!
//! Guarantee: at-least-once. Caveat: a row whose tracking value is lower
//! than rows already polled (a transaction that committed late, or a clock
//! going backwards) is missed. The tracking and key columns must be NOT
//! NULL.

use std::collections::BTreeMap;

use async_trait::async_trait;
use bytes::Bytes;
use serde::Deserialize;
use serde_json::Value;
use tokio_postgres::Client;
use tracing::info;

use crate::builtin::pg::{self, quote_ident, TableRef};
use crate::registry::PluginInit;
use crate::settings::{self, de};
use crate::traits::{ConnectorError, SourceBatch, SourceConnector, SourceRecord};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresPollSettings {
    pub connection: String,
    #[serde(deserialize_with = "de::string_list")]
    pub tables: Vec<String>,
    #[serde(default = "default_tracking")]
    pub tracking_column: String,
    /// Tie-breaker columns. Default: the table's primary key.
    #[serde(default, deserialize_with = "de::string_list")]
    pub key_columns: Vec<String>,
}

fn default_tracking() -> String {
    "updated_at".into()
}

#[derive(Debug, Clone)]
struct TablePlan {
    table: TableRef,
    /// Cursor columns: tracking column, then key columns.
    cursor_cols: Vec<String>,
    key_cols: Vec<String>,
    sql_first: String,
    sql_next: String,
}

pub struct PostgresPollSource {
    s: PostgresPollSettings,
    tables: Vec<TableRef>,
    subject_template: String,
    client: Option<Client>,
    plans: Vec<TablePlan>,
    /// table → cursor values (text), advanced as rows are polled.
    cursors: BTreeMap<String, Vec<String>>,
}

impl PostgresPollSource {
    pub fn new(init: &PluginInit) -> Result<Self, ConnectorError> {
        let s: PostgresPollSettings = settings::parse("postgres_poll", &init.settings)?;
        pg::validate_connection_string(&s.connection)?;
        if s.tables.is_empty() {
            return Err(ConnectorError::config(
                "postgres_poll: 'tables' must list at least one table",
            ));
        }
        let tables = s
            .tables
            .iter()
            .map(|t| TableRef::parse(t))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            subject_template: if init.config.subject_template.is_empty() {
                "{schema}.{table}".into()
            } else {
                init.config.subject_template.clone()
            },
            s,
            tables,
            client: None,
            plans: Vec::new(),
            cursors: BTreeMap::new(),
        })
    }

    async fn plan(&self, client: &Client, table: &TableRef) -> Result<TablePlan, ConnectorError> {
        let types = client
            .query(
                "SELECT a.attname::text, format_type(a.atttypid, a.atttypmod) \
                 FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid \
                 JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE n.nspname = $1 AND c.relname = $2 AND a.attnum > 0 AND NOT a.attisdropped",
                &[&table.schema, &table.table],
            )
            .await
            .map_err(|e| pg::classify(&e, "read table columns"))?;
        if types.is_empty() {
            return Err(ConnectorError::fatal(format!(
                "postgres_poll: table {table} not found"
            )));
        }
        let type_of: BTreeMap<String, String> =
            types.iter().map(|r| (r.get(0), r.get(1))).collect();

        let key_cols = if self.s.key_columns.is_empty() {
            let rows = client
                .query(
                    "SELECT a.attname::text FROM pg_index i \
                     JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) \
                     WHERE i.indrelid = ($1::text)::regclass AND i.indisprimary \
                     ORDER BY array_position(i.indkey, a.attnum)",
                    &[&table.quoted()],
                )
                .await
                .map_err(|e| pg::classify(&e, "read primary key"))?;
            let cols: Vec<String> = rows.iter().map(|r| r.get(0)).collect();
            if cols.is_empty() {
                return Err(ConnectorError::fatal(format!(
                    "postgres_poll: table {table} has no primary key; set key_columns"
                )));
            }
            cols
        } else {
            self.s.key_columns.clone()
        };
        let mut cursor_cols = vec![self.s.tracking_column.clone()];
        cursor_cols.extend(
            key_cols
                .iter()
                .filter(|k| **k != self.s.tracking_column)
                .cloned(),
        );
        for c in &cursor_cols {
            if !type_of.contains_key(c) {
                return Err(ConnectorError::fatal(format!(
                    "postgres_poll: column '{c}' not found in {table}"
                )));
            }
        }
        let cols_q: Vec<String> = cursor_cols
            .iter()
            .map(|c| format!("t.{}", quote_ident(c)))
            .collect();
        let select = format!(
            "SELECT row_to_json(t)::text, {} FROM {} t",
            cols_q
                .iter()
                .map(|c| format!("{c}::text"))
                .collect::<Vec<_>>()
                .join(", "),
            table.quoted()
        );
        let order = cols_q.join(", ");
        let params: Vec<String> = cursor_cols
            .iter()
            .enumerate()
            .map(|(i, c)| format!("(${}::text)::{}", i + 1, type_of[c]))
            .collect();
        let limit_param = cursor_cols.len() + 1;
        Ok(TablePlan {
            table: table.clone(),
            sql_first: format!("{select} ORDER BY {order} LIMIT $1"),
            sql_next: format!(
                "{select} WHERE ({order}) > ({}) ORDER BY {order} LIMIT ${limit_param}",
                params.join(", ")
            ),
            cursor_cols,
            key_cols,
        })
    }

    fn checkpoint(&self) -> String {
        serde_json::to_string(&self.cursors).unwrap_or_default()
    }
}

fn key_bytes(row: &Value, key_cols: &[String]) -> Option<Bytes> {
    let vals: Vec<&Value> = key_cols.iter().filter_map(|k| row.get(k)).collect();
    match vals.as_slice() {
        [] => None,
        [one] => Some(Bytes::from(match one {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        })),
        many => Some(Bytes::from(
            Value::Array(many.iter().map(|v| (*v).clone()).collect()).to_string(),
        )),
    }
}

#[async_trait]
impl SourceConnector for PostgresPollSource {
    async fn start(&mut self, checkpoint: Option<String>) -> Result<(), ConnectorError> {
        let client = pg::connect(&self.s.connection).await?;
        let mut plans = Vec::new();
        for t in &self.tables {
            plans.push(self.plan(&client, t).await?);
        }
        self.cursors = match &checkpoint {
            Some(cp) => serde_json::from_str(cp).map_err(|e| {
                ConnectorError::fatal(format!("postgres_poll: stored checkpoint is invalid: {e}"))
            })?,
            None => BTreeMap::new(),
        };
        for p in &plans {
            if let Some(c) = self.cursors.get(&p.table.to_string()) {
                if c.len() != p.cursor_cols.len() {
                    return Err(ConnectorError::fatal(format!(
                        "postgres_poll: stored cursor for {} has {} values but the cursor has {} columns; \
                         delete the connector's offset to start over",
                        p.table,
                        c.len(),
                        p.cursor_cols.len()
                    )));
                }
            }
        }
        info!(tables = ?self.tables, "postgres_poll started");
        self.plans = plans;
        self.client = Some(client);
        Ok(())
    }

    async fn poll(&mut self, max_batch: usize) -> Result<SourceBatch, ConnectorError> {
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| ConnectorError::connection("postgres_poll: not started"))?;
        let mut records = Vec::new();
        let limit = max_batch.max(1) as i64;
        for plan in &self.plans {
            let tkey = plan.table.to_string();
            let rows = match self.cursors.get(&tkey) {
                None => client
                    .query(&plan.sql_first, &[&limit])
                    .await
                    .map_err(|e| pg::classify(&e, "poll query"))?,
                Some(cursor) => {
                    let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = cursor
                        .iter()
                        .map(|v| v as &(dyn tokio_postgres::types::ToSql + Sync))
                        .collect();
                    params.push(&limit);
                    client
                        .query(&plan.sql_next, &params)
                        .await
                        .map_err(|e| pg::classify(&e, "poll query"))?
                }
            };
            for row in rows {
                let json_text: String = row.get(0);
                let cursor: Vec<String> = (1..=plan.cursor_cols.len())
                    .map(|i| row.get::<_, Option<String>>(i).unwrap_or_default())
                    .collect();
                let value: Value = serde_json::from_str(&json_text).unwrap_or(Value::Null);
                let subject = crate::subject::render(
                    &self.subject_template,
                    &[("schema", &plan.table.schema), ("table", &plan.table.table)],
                    Some(&value),
                );
                records.push(SourceRecord {
                    key: key_bytes(&value, &plan.key_cols),
                    value: Bytes::from(json_text),
                    subject,
                    headers: vec![
                        (
                            "x-idempotency-key".into(),
                            format!("pgpoll:{tkey}:{}", cursor.join("\u{1f}")),
                        ),
                        ("x-exspeed-source".into(), "postgres_poll".into()),
                        ("x-table".into(), tkey.clone()),
                    ],
                });
                self.cursors.insert(tkey.clone(), cursor);
            }
        }
        let checkpoint = (!records.is_empty()).then(|| self.checkpoint());
        Ok(SourceBatch {
            records,
            checkpoint,
        })
    }

    async fn ack(&mut self, _checkpoint: Option<&str>) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn stop(&mut self) -> Result<(), ConnectorError> {
        self.client = None;
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
            config: ConnectorConfig::new("p", ConnectorType::Source, "postgres_poll", "s"),
            settings: settings.as_object().unwrap().clone(),
            metrics: std::sync::Arc::new(m),
        }
    }

    #[test]
    fn settings() {
        assert!(PostgresPollSource::new(&init(
            json!({"connection": "postgres://h/db", "tables": ["a", "b.c"]})
        ))
        .is_ok());
        assert!(PostgresPollSource::new(&init(
            json!({"connection": "postgres://h/db", "tables": "a", "timestamp_column": "x"})
        ))
        .is_err());
        assert!(PostgresPollSource::new(&init(json!({"connection": "postgres://h/db"}))).is_err());
    }

    #[test]
    fn keys() {
        let row = json!({"id": 5, "tenant": "a", "x": 1});
        assert_eq!(key_bytes(&row, &["id".into()]).as_deref(), Some(&b"5"[..]));
        assert_eq!(
            key_bytes(&row, &["tenant".into(), "id".into()]).as_deref(),
            Some(&br#"["a",5]"#[..])
        );
    }
}
