use std::collections::HashMap;

use async_trait::async_trait;
use bytes::Bytes;
use pgwire_replication::{Lsn, ReplicationClient, ReplicationConfig, ReplicationEvent};
use tokio_postgres::NoTls;
use tracing::{debug, error, info, warn};

use crate::builtin::pgoutput;
use crate::config::ConnectorConfig;
use crate::traits::{ConnectorError, HealthStatus, SourceBatch, SourceConnector, SourceRecord};

#[derive(Debug, Clone, PartialEq)]
enum OutboxMode {
    Poll,
    Cdc,
}

#[derive(Debug, Clone, PartialEq)]
enum CleanupMode {
    Delete,
    None,
}

pub struct PostgresOutboxSource {
    connection_string: String,
    outbox_table: String,
    id_column: String,
    aggregate_type_column: String,
    event_type_column: String,
    payload_column: String,
    key_column: String,
    subject_template: String,
    mode: OutboxMode,
    cleanup_mode: CleanupMode,
    slot_name: String,
    publication_name: String,
    /// Regular tokio-postgres client for slot/publication management and DELETE queries.
    client: Option<tokio_postgres::Client>,
    /// pgwire-replication CDC streaming client (CDC mode only).
    repl_client: Option<ReplicationClient>,
    /// Relation schemas received from the WAL (CDC mode only).
    relations: HashMap<u32, pgoutput::Relation>,
    /// Last committed LSN position (CDC mode only).
    last_lsn: Option<Lsn>,
    last_position: Option<String>,
    /// Row IDs from the last poll/CDC batch, used for DELETE on commit.
    pending_ids: Vec<String>,
}

/// Sanitize connector name to valid Postgres identifier chars (alphanumeric + underscore).
pub fn sanitize_pg_name(name: &str) -> String {
    name.chars()
        .map(|c| if c.is_alphanumeric() || c == '_' { c } else { '_' })
        .collect()
}

/// Parse a postgres connection string into components for ReplicationConfig.
///
/// Supports the URI format: `postgres://user:password@host:port/database`
fn parse_connection_string(
    conn_str: &str,
) -> Result<(String, u16, String, String, String), ConnectorError> {
    // Parse using tokio_postgres::Config to handle all valid formats.
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

    let port = pg_config
        .get_ports()
        .first()
        .copied()
        .unwrap_or(5432);

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

/// Column name configuration for outbox record extraction.
struct OutboxColumns<'a> {
    id: &'a str,
    aggregate_type: &'a str,
    event_type: &'a str,
    payload: &'a str,
    key: &'a str,
    subject_template: &'a str,
}

/// Extract outbox columns from a CDC insert event using the relation schema.
/// Free function to avoid borrow conflicts with &mut self in poll_cdc.
fn extract_outbox_record(
    relation: &pgoutput::Relation,
    tuple: &[pgoutput::ColValue],
    cols: &OutboxColumns<'_>,
) -> Result<(String, SourceRecord), ConnectorError> {
    let col_map = pgoutput::tuple_to_map(relation, tuple);

    let id = col_map
        .get(cols.id)
        .and_then(|v| v.clone())
        .ok_or_else(|| {
            ConnectorError::Data(format!("CDC insert missing '{}' column", cols.id))
        })?;

    let agg_type = col_map
        .get(cols.aggregate_type)
        .and_then(|v| v.clone())
        .unwrap_or_default();

    let evt_type = col_map
        .get(cols.event_type)
        .and_then(|v| v.clone())
        .unwrap_or_default();

    let payload_str = col_map
        .get(cols.payload)
        .and_then(|v| v.clone())
        .unwrap_or_default();

    let key_str = col_map
        .get(cols.key)
        .and_then(|v| v.clone())
        .unwrap_or_default();

    let subject = cols
        .subject_template
        .replace("{aggregate_type}", &agg_type)
        .replace("{event_type}", &evt_type);

    let record = SourceRecord {
        key: Some(Bytes::from(key_str.into_bytes())),
        value: Bytes::from(payload_str.into_bytes()),
        subject,
        headers: vec![
            ("x-idempotency-key".to_string(), id.clone()),
            ("x-aggregate-type".to_string(), agg_type),
            ("x-event-type".to_string(), evt_type),
        ],
    };

    Ok((id, record))
}

impl PostgresOutboxSource {
    pub fn new(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let connection_string = config
            .setting("connection")
            .map_err(ConnectorError::Config)?
            .to_string();

        let outbox_table = config.setting_or("outbox_table", "outbox_events");
        let id_column = config.setting_or("id_column", "id");
        let aggregate_type_column = config.setting_or("aggregate_type_column", "aggregate_type");
        let event_type_column = config.setting_or("event_type_column", "event_type");
        let payload_column = config.setting_or("payload_column", "payload");
        let key_column = config.setting_or("key_column", "aggregate_id");
        let subject_template = config.subject_template.clone();

        let mode = match config.setting_or("mode", "poll").as_str() {
            "cdc" => OutboxMode::Cdc,
            _ => OutboxMode::Poll,
        };

        let cleanup_mode = match config.setting_or("cleanup_mode", "delete").as_str() {
            "none" => CleanupMode::None,
            _ => CleanupMode::Delete,
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

        Ok(Self {
            connection_string,
            outbox_table,
            id_column,
            aggregate_type_column,
            event_type_column,
            payload_column,
            key_column,
            subject_template,
            mode,
            cleanup_mode,
            slot_name,
            publication_name,
            client: None,
            repl_client: None,
            relations: HashMap::new(),
            last_lsn: None,
            last_position: None,
            pending_ids: Vec::new(),
        })
    }

    // -----------------------------------------------------------------------
    // CDC mode: start / poll / commit / stop
    // -----------------------------------------------------------------------

    async fn start_cdc(&mut self, last_position: Option<String>) -> Result<(), ConnectorError> {
        // 1. Open a regular client for slot/publication management + DELETE queries
        let (client, connection) = tokio_postgres::connect(&self.connection_string, NoTls)
            .await
            .map_err(|e| ConnectorError::Connection(e.to_string()))?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("postgres management connection error: {}", e);
            }
        });

        // 2. Ensure publication and replication slot exist
        pgoutput::ensure_publication(
            &client,
            &self.publication_name,
            std::slice::from_ref(&self.outbox_table),
        )
        .await?;
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
            "starting CDC replication for outbox"
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
        self.last_position = last_position;

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
        self.pending_ids.clear();

        // Collect events until we hit a Commit, reach max_batch, or timeout
        loop {
            if records.len() >= max_batch {
                break;
            }

            // Use a timeout to avoid blocking forever when no events are available
            let event = match tokio::time::timeout(
                std::time::Duration::from_secs(1),
                repl_client.recv(),
            )
            .await
            {
                Ok(Ok(Some(ev))) => ev,
                Ok(Ok(None)) => {
                    // Stream ended
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
                    // Parse the pgoutput message using our existing parser
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

                            let cols = OutboxColumns {
                                id: &self.id_column,
                                aggregate_type: &self.aggregate_type_column,
                                event_type: &self.event_type_column,
                                payload: &self.payload_column,
                                key: &self.key_column,
                                subject_template: &self.subject_template,
                            };
                            match extract_outbox_record(relation, &new_tuple, &cols) {
                                Ok((id, record)) => {
                                    self.pending_ids.push(id);
                                    records.push(record);
                                }
                                Err(e) => {
                                    warn!(error = %e, "failed to extract outbox record, skipping");
                                }
                            }

                            // Report progress for this WAL position
                            repl_client.update_applied_lsn(wal_end);
                        }
                        pgoutput::WalEvent::Update { .. }
                        | pgoutput::WalEvent::Delete { .. } => {
                            // Outbox pattern only cares about inserts — skip
                            debug!("skipping non-insert WAL event in outbox CDC");
                        }
                        pgoutput::WalEvent::Begin { .. } => {
                            // Transaction boundary — continue collecting
                        }
                        pgoutput::WalEvent::Commit { end_lsn, .. } => {
                            // Transaction boundary — record the LSN and stop this batch
                            commit_lsn = Some(Lsn::from_u64(end_lsn));
                            repl_client.update_applied_lsn(Lsn::from_u64(end_lsn));
                            break;
                        }
                        pgoutput::WalEvent::Unknown(tag) => {
                            debug!(tag, "unknown pgoutput message type, skipping");
                        }
                    }
                }
                ReplicationEvent::Commit {
                    end_lsn, ..
                } => {
                    // High-level commit event from pgwire-replication
                    commit_lsn = Some(end_lsn);
                    repl_client.update_applied_lsn(end_lsn);
                    break;
                }
                ReplicationEvent::Begin { .. } => {
                    // Transaction start — continue
                }
                ReplicationEvent::KeepAlive { .. } => {
                    // Heartbeat — if we already have records, yield them
                    if !records.is_empty() {
                        break;
                    }
                }
                ReplicationEvent::StoppedAt { .. } => {
                    debug!("CDC replication reached stop LSN");
                    break;
                }
                ReplicationEvent::Message { .. } => {
                    // Logical decoding messages — not relevant for outbox
                }
            }
        }

        // Determine position from commit LSN or keep existing
        let position = commit_lsn
            .map(|lsn| lsn.to_string())
            .or_else(|| self.last_lsn.map(|lsn| lsn.to_string()));

        Ok(SourceBatch { records, position })
    }

    async fn commit_cdc(&mut self, position: String) -> Result<(), ConnectorError> {
        // Parse and store the LSN
        let lsn = Lsn::parse(&position).map_err(|e| {
            ConnectorError::Data(format!("invalid LSN in commit position '{position}': {e}"))
        })?;
        self.last_lsn = Some(lsn);
        self.last_position = Some(position);

        // If cleanup_mode=delete, DELETE the processed rows using the regular client
        if self.cleanup_mode == CleanupMode::Delete && !self.pending_ids.is_empty() {
            let client = match &self.client {
                Some(c) => c,
                None => {
                    self.pending_ids.clear();
                    return Ok(());
                }
            };

            // Build a DELETE ... WHERE id IN ($1, $2, ...) query
            let placeholders: Vec<String> = self
                .pending_ids
                .iter()
                .enumerate()
                .map(|(i, _)| format!("${}", i + 1))
                .collect();

            let delete_sql = format!(
                "DELETE FROM {} WHERE {} IN ({})",
                self.outbox_table,
                self.id_column,
                placeholders.join(", ")
            );

            let params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
                self.pending_ids.iter().map(|id| id as &(dyn tokio_postgres::types::ToSql + Sync)).collect();

            if let Err(e) = client.execute(&delete_sql, &params).await {
                warn!(
                    table = self.outbox_table.as_str(),
                    error = %e,
                    "failed to delete processed outbox rows (CDC)"
                );
            }
        }

        self.pending_ids.clear();
        Ok(())
    }

    async fn stop_cdc(&mut self) -> Result<(), ConnectorError> {
        // Stop the replication client
        if let Some(repl_client) = self.repl_client.take() {
            repl_client.stop();
            // Drop will clean up the background worker
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
                error!("postgres connection error: {}", e);
            }
        });

        self.client = Some(client);
        self.last_position = last_position;
        Ok(())
    }

    async fn poll_poll(&mut self, max_batch: usize) -> Result<SourceBatch, ConnectorError> {
        let client = match &self.client {
            Some(c) => c,
            None => return Err(ConnectorError::Connection("not connected".to_string())),
        };

        let (query, params): (String, Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>>) =
            match &self.last_position {
                Some(pos) => {
                    // Parse position as i64 for typed comparison
                    let pos_i64: i64 = pos
                        .parse()
                        .map_err(|e| ConnectorError::Data(format!("invalid position: {e}")))?;
                    (
                        format!(
                            "SELECT {id}, {agg_type}, {evt_type}, {payload}, {key} FROM {table} WHERE {id} > $1 ORDER BY {id} LIMIT $2",
                            id = self.id_column,
                            agg_type = self.aggregate_type_column,
                            evt_type = self.event_type_column,
                            payload = self.payload_column,
                            key = self.key_column,
                            table = self.outbox_table,
                        ),
                        vec![Box::new(pos_i64), Box::new(max_batch as i64)],
                    )
                }
                None => (
                    format!(
                        "SELECT {id}, {agg_type}, {evt_type}, {payload}, {key} FROM {table} ORDER BY {id} LIMIT $1",
                        id = self.id_column,
                        agg_type = self.aggregate_type_column,
                        evt_type = self.event_type_column,
                        payload = self.payload_column,
                        key = self.key_column,
                        table = self.outbox_table,
                    ),
                    vec![Box::new(max_batch as i64)],
                ),
            };

        let params_ref: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
            params.iter().map(|p| &**p as &(dyn tokio_postgres::types::ToSql + Sync)).collect();

        let rows = client
            .query(&query, &params_ref)
            .await
            .map_err(|e| ConnectorError::Data(e.to_string()))?;

        let mut records = Vec::with_capacity(rows.len());
        let mut last_id: Option<String> = None;
        self.pending_ids.clear();

        for row in &rows {
            let id: String = row
                .try_get::<_, String>(0)
                .or_else(|_| row.try_get::<_, i64>(0).map(|v| v.to_string()))
                .map_err(|e| ConnectorError::Data(format!("id column: {e}")))?;

            let agg_type: String = row
                .try_get::<_, String>(1)
                .or_else(|_| row.try_get::<_, i64>(1).map(|v| v.to_string()))
                .map_err(|e| ConnectorError::Data(format!("aggregate_type column: {e}")))?;

            let evt_type: String = row
                .try_get::<_, String>(2)
                .or_else(|_| row.try_get::<_, i64>(2).map(|v| v.to_string()))
                .map_err(|e| ConnectorError::Data(format!("event_type column: {e}")))?;

            let payload_bytes: Bytes = row
                .try_get::<_, String>(3)
                .map(|s| Bytes::from(s.into_bytes()))
                .or_else(|_| row.try_get::<_, Vec<u8>>(3).map(Bytes::from))
                .map_err(|e| ConnectorError::Data(format!("payload column: {e}")))?;

            let key_str: String = row
                .try_get::<_, String>(4)
                .or_else(|_| row.try_get::<_, i64>(4).map(|v| v.to_string()))
                .map_err(|e| ConnectorError::Data(format!("key column: {e}")))?;

            let subject = self
                .subject_template
                .replace("{aggregate_type}", &agg_type)
                .replace("{event_type}", &evt_type);

            let record = SourceRecord {
                key: Some(Bytes::from(key_str.into_bytes())),
                value: payload_bytes,
                subject,
                headers: vec![
                    ("x-idempotency-key".to_string(), id.clone()),
                    ("x-aggregate-type".to_string(), agg_type),
                    ("x-event-type".to_string(), evt_type),
                ],
            };

            self.pending_ids.push(id.clone());
            last_id = Some(id);
            records.push(record);
        }

        Ok(SourceBatch {
            records,
            position: last_id,
        })
    }

    async fn commit_poll(&mut self, position: String) -> Result<(), ConnectorError> {
        self.last_position = Some(position.clone());

        if self.cleanup_mode == CleanupMode::Delete && !self.pending_ids.is_empty() {
            let client = match &self.client {
                Some(c) => c,
                None => return Ok(()), // Can't delete if not connected
            };

            // DELETE processed rows by ID
            // For poll mode, we can use WHERE id <= position (simpler, single query)
            let pos_i64: i64 = position
                .parse()
                .map_err(|e| ConnectorError::Data(format!("invalid position for delete: {e}")))?;

            let delete_sql = format!(
                "DELETE FROM {} WHERE {} <= $1",
                self.outbox_table, self.id_column
            );

            if let Err(e) = client.execute(&delete_sql, &[&pos_i64]).await {
                // Log but don't fail -- broker dedup is the safety net
                warn!(
                    table = self.outbox_table.as_str(),
                    error = %e,
                    "failed to delete processed outbox rows"
                );
            }

            self.pending_ids.clear();
        }

        Ok(())
    }

    async fn stop_poll(&mut self) -> Result<(), ConnectorError> {
        self.client = None;
        Ok(())
    }
}

#[async_trait]
impl SourceConnector for PostgresOutboxSource {
    async fn start(&mut self, last_position: Option<String>) -> Result<(), ConnectorError> {
        match self.mode {
            OutboxMode::Cdc => self.start_cdc(last_position).await,
            OutboxMode::Poll => self.start_poll(last_position).await,
        }
    }

    async fn poll(&mut self, max_batch: usize) -> Result<SourceBatch, ConnectorError> {
        match self.mode {
            OutboxMode::Cdc => self.poll_cdc(max_batch).await,
            OutboxMode::Poll => self.poll_poll(max_batch).await,
        }
    }

    async fn commit(&mut self, position: String) -> Result<(), ConnectorError> {
        match self.mode {
            OutboxMode::Cdc => self.commit_cdc(position).await,
            OutboxMode::Poll => self.commit_poll(position).await,
        }
    }

    async fn stop(&mut self) -> Result<(), ConnectorError> {
        match self.mode {
            OutboxMode::Cdc => self.stop_cdc().await,
            OutboxMode::Poll => self.stop_poll().await,
        }
    }

    async fn health(&self) -> HealthStatus {
        match self.mode {
            OutboxMode::Cdc => {
                if self.repl_client.is_some() && self.client.is_some() {
                    HealthStatus::Healthy
                } else if self.client.is_some() {
                    HealthStatus::Degraded("replication client not connected".to_string())
                } else {
                    HealthStatus::Unhealthy("not connected".to_string())
                }
            }
            OutboxMode::Poll => {
                if self.client.is_some() {
                    HealthStatus::Healthy
                } else {
                    HealthStatus::Unhealthy("not connected".to_string())
                }
            }
        }
    }
}
