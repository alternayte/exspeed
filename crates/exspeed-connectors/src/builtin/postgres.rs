use std::collections::HashMap;

use async_trait::async_trait;
use bytes::Bytes;
use pgwire_replication::{Lsn, ReplicationClient, ReplicationConfig, ReplicationEvent};
use tokio_postgres::NoTls;
use tracing::{debug, error, info, warn};

use crate::builtin::pgoutput;
use crate::config::ConnectorConfig;
use crate::traits::{ConnectorError, HealthStatus, SourceBatch, SourceConnector, SourceRecord};

use super::postgres_outbox::sanitize_pg_name;

#[derive(Debug, Clone, PartialEq)]
enum PgMode {
    Poll,
    Cdc,
}

pub struct PostgresSource {
    // Note: tokio_postgres::Client does not implement Debug, so we impl Debug manually below.
    connection_string: String,
    tables: Vec<String>,
    timestamp_column: String,
    subject_template: String,
    mode: PgMode,
    slot_name: String,
    publication_name: String,
    operations: Vec<String>,
    client: Option<tokio_postgres::Client>,
    last_timestamp: Option<String>,
    /// pgwire-replication CDC streaming client (CDC mode only).
    repl_client: Option<ReplicationClient>,
    /// Relation schemas received from the WAL (CDC mode only).
    relations: HashMap<u32, pgoutput::Relation>,
    /// Last committed LSN position (CDC mode only).
    last_lsn: Option<Lsn>,
}

impl std::fmt::Debug for PostgresSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresSource")
            .field("connection_string", &"[redacted]")
            .field("tables", &self.tables)
            .field("timestamp_column", &self.timestamp_column)
            .field("subject_template", &self.subject_template)
            .field("mode", &self.mode)
            .field("connected", &self.client.is_some())
            .field("last_timestamp", &self.last_timestamp)
            .field("repl_connected", &self.repl_client.is_some())
            .field("last_lsn", &self.last_lsn)
            .finish()
    }
}

/// Extract (schema, bare_table) from a possibly-qualified table name like "public.orders".
fn split_table_name(table: &str) -> (&str, &str) {
    if let Some(dot) = table.find('.') {
        (&table[..dot], &table[dot + 1..])
    } else {
        ("public", table)
    }
}

/// Attempt to read a column value as a String, trying several common Postgres types.
fn col_to_string(row: &tokio_postgres::Row, idx: usize) -> String {
    if let Ok(v) = row.try_get::<_, String>(idx) {
        return v;
    }
    if let Ok(v) = row.try_get::<_, i64>(idx) {
        return v.to_string();
    }
    if let Ok(v) = row.try_get::<_, i32>(idx) {
        return v.to_string();
    }
    if let Ok(v) = row.try_get::<_, f64>(idx) {
        return v.to_string();
    }
    if let Ok(v) = row.try_get::<_, bool>(idx) {
        return v.to_string();
    }
    "<null>".to_string()
}

/// Parse a postgres connection string into components for ReplicationConfig.
///
/// Supports the URI format: `postgres://user:password@host:port/database`
fn parse_connection_string(
    conn_str: &str,
) -> Result<(String, u16, String, String, String), ConnectorError> {
    let pg_config: tokio_postgres::Config = conn_str
        .parse()
        .map_err(|e| ConnectorError::Config(format!("invalid connection string: {e}")))?;

    let host = pg_config
        .get_hosts()
        .first()
        .map(|h| match h {
            tokio_postgres::config::Host::Tcp(s) => s.clone(),
            #[cfg(unix)]
            tokio_postgres::config::Host::Unix(p) => p.to_string_lossy().into_owned(),
        })
        .unwrap_or_else(|| "127.0.0.1".to_string());

    let port = pg_config.get_ports().first().copied().unwrap_or(5432);

    let user = pg_config
        .get_user()
        .unwrap_or("postgres")
        .to_string();

    let password = pg_config
        .get_password()
        .map(|p| String::from_utf8_lossy(p).into_owned())
        .unwrap_or_default();

    let database = pg_config
        .get_dbname()
        .unwrap_or("postgres")
        .to_string();

    Ok((host, port, user, password, database))
}

impl PostgresSource {
    pub fn new(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let connection_string = config
            .setting("connection")
            .map_err(ConnectorError::Config)?
            .to_string();

        let tables_raw = config
            .setting("tables")
            .map_err(ConnectorError::Config)?
            .to_string();

        let tables: Vec<String> = tables_raw
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        if tables.is_empty() {
            return Err(ConnectorError::Config(
                "setting 'tables' must contain at least one table name".to_string(),
            ));
        }

        let timestamp_column = config.setting_or("timestamp_column", "updated_at");
        let subject_template = config.subject_template.clone();

        let mode = match config.setting_or("mode", "poll").as_str() {
            "cdc" => PgMode::Cdc,
            _ => PgMode::Poll,
        };

        let sanitized = sanitize_pg_name(&config.name);
        let slot_name = {
            let configured = config.setting_or("slot_name", "");
            if configured.is_empty() {
                format!("exspeed_{sanitized}_slot")
            } else {
                configured
            }
        };
        let publication_name = {
            let configured = config.setting_or("publication_name", "");
            if configured.is_empty() {
                format!("exspeed_{sanitized}_pub")
            } else {
                configured
            }
        };

        let operations: Vec<String> = config
            .setting_or("operations", "INSERT,UPDATE,DELETE")
            .split(',')
            .map(|s| s.trim().to_uppercase())
            .collect();

        Ok(Self {
            connection_string,
            tables,
            timestamp_column,
            subject_template,
            mode,
            slot_name,
            publication_name,
            operations,
            client: None,
            last_timestamp: None,
            repl_client: None,
            relations: HashMap::new(),
            last_lsn: None,
        })
    }

    // -----------------------------------------------------------------------
    // CDC mode: start / poll / commit / stop
    // -----------------------------------------------------------------------

    async fn start_cdc(&mut self, last_position: Option<String>) -> Result<(), ConnectorError> {
        // 1. Open a regular client for publication/slot management
        let (client, connection) = tokio_postgres::connect(&self.connection_string, NoTls)
            .await
            .map_err(|e| ConnectorError::Connection(e.to_string()))?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("postgres management connection error: {}", e);
            }
        });

        // 2. Ensure publication and replication slot exist
        pgoutput::ensure_publication(&client, &self.publication_name, &self.tables).await?;
        pgoutput::ensure_replication_slot(&client, &self.slot_name).await?;

        self.client = Some(client);

        // 3. Parse the last_position as an LSN to resume from
        let start_lsn = match &last_position {
            Some(pos) => Lsn::parse(pos).map_err(|e| {
                ConnectorError::Data(format!("invalid LSN position '{pos}': {e}"))
            })?,
            None => Lsn::ZERO,
        };

        // 4. Parse connection string components for ReplicationConfig
        let (host, port, user, password, database) =
            parse_connection_string(&self.connection_string)?;

        let repl_config = ReplicationConfig {
            host,
            port,
            user,
            password,
            database,
            slot: self.slot_name.clone(),
            publication: self.publication_name.clone(),
            start_lsn,
            ..Default::default()
        };

        info!(
            slot = self.slot_name.as_str(),
            publication = self.publication_name.as_str(),
            start_lsn = %start_lsn,
            tables = ?self.tables,
            "starting CDC replication for postgres source"
        );

        let repl_client = ReplicationClient::connect(repl_config)
            .await
            .map_err(|e| ConnectorError::Connection(format!("replication connect failed: {e}")))?;

        self.repl_client = Some(repl_client);
        self.last_lsn = if start_lsn.is_zero() {
            None
        } else {
            Some(start_lsn)
        };

        Ok(())
    }

    async fn poll_cdc(&mut self, max_batch: usize) -> Result<SourceBatch, ConnectorError> {
        let repl_client = match &mut self.repl_client {
            Some(c) => c,
            None => {
                return Err(ConnectorError::Connection(
                    "replication client not connected".to_string(),
                ))
            }
        };

        let mut records = Vec::new();
        let mut commit_lsn: Option<Lsn> = None;

        // Collect events until we hit a Commit, reach max_batch, or timeout
        loop {
            if records.len() >= max_batch {
                break;
            }

            let event = match tokio::time::timeout(
                std::time::Duration::from_secs(1),
                repl_client.recv(),
            )
            .await
            {
                Ok(Ok(Some(ev))) => ev,
                Ok(Ok(None)) => {
                    debug!("CDC replication stream ended");
                    break;
                }
                Ok(Err(e)) => {
                    return Err(ConnectorError::Connection(format!(
                        "replication error: {e}"
                    )));
                }
                Err(_) => {
                    // Timeout — return what we have so far
                    break;
                }
            };

            match event {
                ReplicationEvent::XLogData {
                    data, wal_end, ..
                } => {
                    let wal_event = match pgoutput::parse_pgoutput_message(&data) {
                        Ok(ev) => ev,
                        Err(e) => {
                            warn!(error = %e, "failed to parse pgoutput message, skipping");
                            continue;
                        }
                    };

                    match wal_event {
                        pgoutput::WalEvent::Relation(rel) => {
                            debug!(
                                relation_id = rel.id,
                                table = %rel.table,
                                "received relation schema"
                            );
                            self.relations.insert(rel.id, rel);
                        }
                        pgoutput::WalEvent::Insert {
                            relation_id,
                            new_tuple,
                        } => {
                            if !self.operations.contains(&"INSERT".to_string()) {
                                continue;
                            }
                            let relation = match self.relations.get(&relation_id) {
                                Some(r) => r,
                                None => {
                                    warn!(
                                        relation_id,
                                        "received insert for unknown relation, skipping"
                                    );
                                    continue;
                                }
                            };

                            let record = build_cdc_record(
                                relation, &new_tuple, "insert", wal_end,
                            )?;
                            records.push(record);
                            repl_client.update_applied_lsn(wal_end);
                        }
                        pgoutput::WalEvent::Update {
                            relation_id,
                            new_tuple,
                            ..
                        } => {
                            if !self.operations.contains(&"UPDATE".to_string()) {
                                continue;
                            }
                            let relation = match self.relations.get(&relation_id) {
                                Some(r) => r,
                                None => {
                                    warn!(
                                        relation_id,
                                        "received update for unknown relation, skipping"
                                    );
                                    continue;
                                }
                            };

                            let record = build_cdc_record(
                                relation, &new_tuple, "update", wal_end,
                            )?;
                            records.push(record);
                            repl_client.update_applied_lsn(wal_end);
                        }
                        pgoutput::WalEvent::Delete {
                            relation_id,
                            old_tuple,
                        } => {
                            if !self.operations.contains(&"DELETE".to_string()) {
                                continue;
                            }
                            let relation = match self.relations.get(&relation_id) {
                                Some(r) => r,
                                None => {
                                    warn!(
                                        relation_id,
                                        "received delete for unknown relation, skipping"
                                    );
                                    continue;
                                }
                            };

                            let record = build_cdc_record(
                                relation, &old_tuple, "delete", wal_end,
                            )?;
                            records.push(record);
                            repl_client.update_applied_lsn(wal_end);
                        }
                        pgoutput::WalEvent::Begin { .. } => {
                            // Transaction boundary — continue collecting
                        }
                        pgoutput::WalEvent::Commit { end_lsn, .. } => {
                            commit_lsn = Some(Lsn::from_u64(end_lsn));
                            repl_client.update_applied_lsn(Lsn::from_u64(end_lsn));
                            break;
                        }
                        pgoutput::WalEvent::Unknown(tag) => {
                            debug!(tag, "unknown pgoutput message type, skipping");
                        }
                    }
                }
                ReplicationEvent::Commit { end_lsn, .. } => {
                    commit_lsn = Some(end_lsn);
                    repl_client.update_applied_lsn(end_lsn);
                    break;
                }
                ReplicationEvent::Begin { .. } => {
                    // Transaction start — continue
                }
                ReplicationEvent::KeepAlive { .. } => {
                    if !records.is_empty() {
                        break;
                    }
                }
                ReplicationEvent::StoppedAt { .. } => {
                    debug!("CDC replication reached stop LSN");
                    break;
                }
                ReplicationEvent::Message { .. } => {
                    // Logical decoding messages — not relevant
                }
            }
        }

        let position = commit_lsn
            .map(|lsn| lsn.to_string())
            .or_else(|| self.last_lsn.map(|lsn| lsn.to_string()));

        Ok(SourceBatch { records, position })
    }

    async fn commit_cdc(&mut self, position: String) -> Result<(), ConnectorError> {
        let lsn = Lsn::parse(&position).map_err(|e| {
            ConnectorError::Data(format!("invalid LSN in commit position '{position}': {e}"))
        })?;
        self.last_lsn = Some(lsn);
        Ok(())
    }

    async fn stop_cdc(&mut self) -> Result<(), ConnectorError> {
        if let Some(repl_client) = self.repl_client.take() {
            repl_client.stop();
        }
        self.client = None;
        self.relations.clear();
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Poll mode: start / poll / commit / stop (original implementation)
    // -----------------------------------------------------------------------

    async fn start_poll(&mut self, last_position: Option<String>) -> Result<(), ConnectorError> {
        let (client, connection) = tokio_postgres::connect(&self.connection_string, NoTls)
            .await
            .map_err(|e| ConnectorError::Connection(e.to_string()))?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("postgres source connection error: {}", e);
            }
        });

        self.client = Some(client);
        self.last_timestamp = last_position;
        Ok(())
    }

    async fn poll_poll(&mut self, max_batch: usize) -> Result<SourceBatch, ConnectorError> {
        let client = match &self.client {
            Some(c) => c,
            None => return Err(ConnectorError::Connection("not connected".to_string())),
        };

        let mut all_records: Vec<SourceRecord> = Vec::new();
        let mut max_ts: Option<String> = self.last_timestamp.clone();

        for table in &self.tables.clone() {
            let (schema, bare_table) = split_table_name(table);

            let rows = match &self.last_timestamp {
                Some(ts) => {
                    let query = format!(
                        "SELECT * FROM {table} WHERE {ts_col}::text > $1 ORDER BY {ts_col} LIMIT $2",
                        table = table,
                        ts_col = self.timestamp_column,
                    );
                    client
                        .query(&query, &[ts, &(max_batch as i64)])
                        .await
                        .map_err(|e| ConnectorError::Data(e.to_string()))?
                }
                None => {
                    let query = format!(
                        "SELECT * FROM {table} ORDER BY {ts_col} LIMIT $1",
                        table = table,
                        ts_col = self.timestamp_column,
                    );
                    client
                        .query(&query, &[&(max_batch as i64)])
                        .await
                        .map_err(|e| ConnectorError::Data(e.to_string()))?
                }
            };

            for row in &rows {
                let columns = row.columns();

                let mut map = serde_json::Map::new();
                let mut row_ts: Option<String> = None;
                let mut pk_value = String::new();

                for (idx, col) in columns.iter().enumerate() {
                    let val_str = col_to_string(row, idx);
                    map.insert(
                        col.name().to_string(),
                        serde_json::Value::String(val_str.clone()),
                    );

                    if col.name() == self.timestamp_column.as_str() {
                        row_ts = Some(val_str.clone());
                        let update = match &max_ts {
                            None => true,
                            Some(prev) => val_str > *prev,
                        };
                        if update {
                            max_ts = Some(val_str.clone());
                        }
                    }

                    if idx == 0 {
                        pk_value = val_str;
                    }
                }

                let value_json = serde_json::Value::Object(map);
                let value_bytes = Bytes::from(
                    serde_json::to_vec(&value_json)
                        .map_err(|e| ConnectorError::Data(format!("json serialization: {e}")))?,
                );

                let subject = if !self.subject_template.is_empty() {
                    self.subject_template
                        .replace("{schema}", schema)
                        .replace("{table}", bare_table)
                } else {
                    format!("{schema}.{bare_table}.change")
                };

                let key = if pk_value.is_empty() {
                    None
                } else {
                    Some(Bytes::from(pk_value.clone().into_bytes()))
                };

                let idemp_key = format!(
                    "{}:{}:{}",
                    table,
                    pk_value,
                    row_ts.as_deref().unwrap_or("")
                );

                let record = SourceRecord {
                    key,
                    value: value_bytes,
                    subject,
                    headers: vec![
                        ("x-idempotency-key".to_string(), idemp_key),
                        ("x-exspeed-source".to_string(), "postgres".to_string()),
                        ("x-table".to_string(), table.clone()),
                    ],
                };

                all_records.push(record);
            }
        }

        if max_ts != self.last_timestamp {
            self.last_timestamp = max_ts.clone();
        }

        Ok(SourceBatch {
            records: all_records,
            position: max_ts,
        })
    }

    async fn commit_poll(&mut self, position: String) -> Result<(), ConnectorError> {
        self.last_timestamp = Some(position);
        Ok(())
    }

    async fn stop_poll(&mut self) -> Result<(), ConnectorError> {
        self.client = None;
        Ok(())
    }
}

/// Build a SourceRecord from a CDC WAL event (insert, update, or delete).
fn build_cdc_record(
    relation: &pgoutput::Relation,
    tuple: &[pgoutput::ColValue],
    operation: &str,
    wal_end: Lsn,
) -> Result<SourceRecord, ConnectorError> {
    let col_map = pgoutput::tuple_to_map(relation, tuple);

    // Build JSON value from all columns
    let json_map: serde_json::Map<String, serde_json::Value> = col_map
        .iter()
        .map(|(k, v)| {
            (
                k.clone(),
                match v {
                    Some(s) => serde_json::Value::String(s.clone()),
                    None => serde_json::Value::Null,
                },
            )
        })
        .collect();
    let value = serde_json::to_vec(&serde_json::Value::Object(json_map))
        .map_err(|e| ConnectorError::Data(format!("json serialization: {e}")))?;

    // Subject = {schema}.{table}.{operation}
    let subject = format!("{}.{}.{}", relation.schema, relation.table, operation);

    // Idempotency key: {table}:{pk}:{lsn} where pk is first column value
    let pk = col_map
        .values()
        .next()
        .and_then(|v| v.clone())
        .unwrap_or_default();
    let idemp_key = format!("{}:{pk}:{wal_end}", relation.table);

    // Key is the primary key value
    let key = if pk.is_empty() {
        None
    } else {
        Some(Bytes::from(pk.into_bytes()))
    };

    Ok(SourceRecord {
        key,
        value: Bytes::from(value),
        subject,
        headers: vec![
            ("x-idempotency-key".to_string(), idemp_key),
            ("x-operation".to_string(), operation.to_string()),
            ("x-table".to_string(), relation.table.clone()),
            ("x-schema".to_string(), relation.schema.clone()),
            ("x-exspeed-source".to_string(), "postgres".to_string()),
        ],
    })
}

#[async_trait]
impl SourceConnector for PostgresSource {
    async fn start(&mut self, last_position: Option<String>) -> Result<(), ConnectorError> {
        match self.mode {
            PgMode::Cdc => self.start_cdc(last_position).await,
            PgMode::Poll => self.start_poll(last_position).await,
        }
    }

    async fn poll(&mut self, max_batch: usize) -> Result<SourceBatch, ConnectorError> {
        match self.mode {
            PgMode::Cdc => self.poll_cdc(max_batch).await,
            PgMode::Poll => self.poll_poll(max_batch).await,
        }
    }

    async fn commit(&mut self, position: String) -> Result<(), ConnectorError> {
        match self.mode {
            PgMode::Cdc => self.commit_cdc(position).await,
            PgMode::Poll => self.commit_poll(position).await,
        }
    }

    async fn stop(&mut self) -> Result<(), ConnectorError> {
        match self.mode {
            PgMode::Cdc => self.stop_cdc().await,
            PgMode::Poll => self.stop_poll().await,
        }
    }

    async fn health(&self) -> HealthStatus {
        match self.mode {
            PgMode::Cdc => {
                if self.repl_client.is_some() && self.client.is_some() {
                    HealthStatus::Healthy
                } else if self.client.is_some() {
                    HealthStatus::Degraded("replication client not connected".to_string())
                } else {
                    HealthStatus::Unhealthy("not connected".to_string())
                }
            }
            PgMode::Poll => {
                if self.client.is_some() {
                    HealthStatus::Healthy
                } else {
                    HealthStatus::Unhealthy("not connected".to_string())
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn make_config(settings: HashMap<String, String>) -> ConnectorConfig {
        ConnectorConfig {
            name: "test-pg".into(),
            connector_type: "source".into(),
            plugin: "postgres".into(),
            stream: "events".into(),
            subject_template: "".into(),
            subject_filter: "".into(),
            settings,
            batch_size: 100,
            poll_interval_ms: 50,
            dedup_enabled: true,
            dedup_key: String::new(),
            dedup_window_secs: 86400,
            transform_sql: String::new(),
        }
    }

    #[test]
    fn rejects_missing_connection() {
        let config = make_config(HashMap::from([("tables".into(), "public.orders".into())]));
        let err = PostgresSource::new(&config).unwrap_err();
        assert!(err.to_string().contains("connection"));
    }

    #[test]
    fn rejects_missing_tables() {
        let config = make_config(HashMap::from([(
            "connection".into(),
            "postgres://localhost/db".into(),
        )]));
        let err = PostgresSource::new(&config).unwrap_err();
        assert!(err.to_string().contains("tables"));
    }

    #[test]
    fn parses_valid_config() {
        let config = make_config(HashMap::from([
            ("connection".into(), "postgres://localhost/db".into()),
            ("tables".into(), "public.orders, public.customers".into()),
            ("timestamp_column".into(), "modified_at".into()),
        ]));
        let src = PostgresSource::new(&config).unwrap();
        assert_eq!(src.tables, vec!["public.orders", "public.customers"]);
        assert_eq!(src.timestamp_column, "modified_at");
    }

    #[test]
    fn default_timestamp_column() {
        let config = make_config(HashMap::from([
            ("connection".into(), "postgres://localhost/db".into()),
            ("tables".into(), "orders".into()),
        ]));
        let src = PostgresSource::new(&config).unwrap();
        assert_eq!(src.timestamp_column, "updated_at");
    }

    #[test]
    fn split_table_name_qualified() {
        assert_eq!(split_table_name("myschema.mytable"), ("myschema", "mytable"));
    }

    #[test]
    fn split_table_name_unqualified() {
        assert_eq!(split_table_name("mytable"), ("public", "mytable"));
    }

    #[test]
    fn auto_generates_slot_and_publication_names() {
        let config = make_config(HashMap::from([
            ("connection".into(), "postgres://localhost/db".into()),
            ("tables".into(), "orders".into()),
        ]));
        let src = PostgresSource::new(&config).unwrap();
        assert_eq!(src.slot_name, "exspeed_test_pg_slot");
        assert_eq!(src.publication_name, "exspeed_test_pg_pub");
    }

    #[test]
    fn cdc_mode_accepted() {
        let config = make_config(HashMap::from([
            ("connection".into(), "postgres://localhost/db".into()),
            ("tables".into(), "orders".into()),
            ("mode".into(), "cdc".into()),
        ]));
        let src = PostgresSource::new(&config).unwrap();
        assert_eq!(src.mode, PgMode::Cdc);
    }

    #[test]
    fn default_operations_include_all() {
        let config = make_config(HashMap::from([
            ("connection".into(), "postgres://localhost/db".into()),
            ("tables".into(), "orders".into()),
        ]));
        let src = PostgresSource::new(&config).unwrap();
        assert_eq!(src.operations, vec!["INSERT", "UPDATE", "DELETE"]);
    }

    #[test]
    fn custom_operations_parsed() {
        let config = make_config(HashMap::from([
            ("connection".into(), "postgres://localhost/db".into()),
            ("tables".into(), "orders".into()),
            ("operations".into(), "INSERT, UPDATE".into()),
        ]));
        let src = PostgresSource::new(&config).unwrap();
        assert_eq!(src.operations, vec!["INSERT", "UPDATE"]);
    }

    #[test]
    fn build_cdc_record_insert() {
        let relation = pgoutput::Relation {
            id: 1,
            schema: "public".into(),
            table: "orders".into(),
            columns: vec![
                pgoutput::ColumnDef {
                    name: "id".into(),
                    type_oid: 23,
                    type_modifier: -1,
                },
                pgoutput::ColumnDef {
                    name: "amount".into(),
                    type_oid: 23,
                    type_modifier: -1,
                },
                pgoutput::ColumnDef {
                    name: "status".into(),
                    type_oid: 25,
                    type_modifier: -1,
                },
            ],
        };
        let tuple = vec![
            pgoutput::ColValue::Text("42".into()),
            pgoutput::ColValue::Text("100".into()),
            pgoutput::ColValue::Text("pending".into()),
        ];

        let lsn = Lsn::from_u64(12345);
        let record = build_cdc_record(&relation, &tuple, "insert", lsn).unwrap();

        assert_eq!(record.subject, "public.orders.insert");
        assert!(record.key.is_some());

        // Check headers
        let headers: HashMap<String, String> = record.headers.into_iter().collect();
        assert_eq!(headers.get("x-operation").unwrap(), "insert");
        assert_eq!(headers.get("x-table").unwrap(), "orders");
        assert_eq!(headers.get("x-schema").unwrap(), "public");
        assert_eq!(headers.get("x-exspeed-source").unwrap(), "postgres");
        assert!(headers.get("x-idempotency-key").unwrap().contains("orders:"));

        // Check JSON value contains all columns
        let value: serde_json::Value = serde_json::from_slice(&record.value).unwrap();
        let obj = value.as_object().unwrap();
        assert_eq!(obj.get("id").unwrap(), "42");
        assert_eq!(obj.get("amount").unwrap(), "100");
        assert_eq!(obj.get("status").unwrap(), "pending");
    }

    #[test]
    fn build_cdc_record_delete_with_null() {
        let relation = pgoutput::Relation {
            id: 2,
            schema: "inventory".into(),
            table: "items".into(),
            columns: vec![
                pgoutput::ColumnDef {
                    name: "id".into(),
                    type_oid: 23,
                    type_modifier: -1,
                },
                pgoutput::ColumnDef {
                    name: "name".into(),
                    type_oid: 25,
                    type_modifier: -1,
                },
            ],
        };
        let tuple = vec![
            pgoutput::ColValue::Text("7".into()),
            pgoutput::ColValue::Null,
        ];

        let lsn = Lsn::from_u64(99999);
        let record = build_cdc_record(&relation, &tuple, "delete", lsn).unwrap();

        assert_eq!(record.subject, "inventory.items.delete");

        let value: serde_json::Value = serde_json::from_slice(&record.value).unwrap();
        let obj = value.as_object().unwrap();
        assert_eq!(obj.get("id").unwrap(), "7");
        assert!(obj.get("name").unwrap().is_null());
    }
}
