//! `postgres_cdc`: logical-replication CDC (pgoutput).
//!
//! Each row change becomes one record with a Debezium-style envelope:
//!
//! ```json
//! {"op": "u",
//!  "before": {"id": 1},                       // key columns (or full row with REPLICA IDENTITY FULL)
//!  "after":  {"id": 1, "name": "b"},          // unchanged TOAST columns omitted …
//!  "__unchanged": ["big_doc"],                // … and listed here
//!  "source": {"connector": "users-cdc", "lsn": "0/16B3748", "txid": 731,
//!             "schema": "public", "table": "users", "ts_ms": 1700000000000}}
//! ```
//!
//! - **Key**: the replica-identity key columns (from the Relation message
//!   flags) in column order: the raw text value for a single column, a JSON
//!   array of typed values for a composite key, none for REPLICA IDENTITY
//!   NOTHING.
//! - **Values** are typed by column OID (see [`pg::typed_value`]).
//! - **Checkpoint** = the end LSN of the last fully received transaction;
//!   the slot's `confirmed_flush_lsn` is advanced only in `ack()`, after the
//!   records are durable. With no transaction in flight, keepalives advance
//!   the checkpoint too, so an idle table doesn't pin WAL.
//! - **Idempotency key** `pgcdc:<slot>:<commit LSN>:<n>`: a replayed
//!   transaction is deduplicated by the broker within the stream's dedup
//!   window.
//!
//! Guarantee: at-least-once; effectively-once for replays inside the
//! stream's dedup window.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use pgwire_replication::{Lsn, ReplicationClient, ReplicationEvent};
use serde::Deserialize;
use serde_json::{json, Map, Value};
use tokio::time::Instant;
use tokio_postgres::Client;
use tracing::{debug, info, warn};

use crate::builtin::pg::{self, TableRef};
use crate::builtin::pgoutput::{self, ColValue, OldKind, Relation, WalEvent};
use crate::registry::PluginInit;
use crate::settings::{self, de};
use crate::traits::{ConnectorError, Lag, LagUnit, SourceBatch, SourceConnector, SourceRecord};

/// Seconds between the Unix epoch and the Postgres epoch (2000-01-01).
const PG_EPOCH_OFFSET_SECS: i64 = 946_684_800;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresCdcSettings {
    pub connection: String,
    #[serde(deserialize_with = "de::string_list")]
    pub tables: Vec<String>,
    /// `insert`, `update`, `delete` (default: all).
    #[serde(default = "default_ops", deserialize_with = "de::string_list")]
    pub operations: Vec<String>,
    #[serde(default)]
    pub slot_name: Option<String>,
    #[serde(default)]
    pub publication_name: Option<String>,
    /// Drop the replication slot (and a derived publication) when the
    /// connector is deleted.
    #[serde(default, deserialize_with = "de::bool")]
    pub drop_slot_on_delete: bool,
}

fn default_ops() -> Vec<String> {
    vec!["insert".into(), "update".into(), "delete".into()]
}

/// Transaction context while streaming.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Txn {
    pub final_lsn: u64,
    pub xid: u32,
    pub commit_time_micros: i64,
    pub ordinal: u32,
}

impl Txn {
    pub fn ts_ms(&self) -> i64 {
        self.commit_time_micros / 1000 + PG_EPOCH_OFFSET_SECS * 1000
    }
}

pub(crate) enum ChangeKind {
    Insert {
        new: Vec<ColValue>,
    },
    Update {
        old: Option<(OldKind, Vec<ColValue>)>,
        new: Vec<ColValue>,
    },
    Delete {
        old: (OldKind, Vec<ColValue>),
    },
}

pub(crate) struct Change {
    pub relation: Arc<Relation>,
    pub kind: ChangeKind,
    pub txn: Txn,
}

pub(crate) enum Step {
    Change(Change),
    /// A transaction fully received; resume position = `end_lsn`.
    Commit(Lsn),
    /// Nothing in flight; safe resume position from a keepalive.
    Idle(Lsn),
    /// No event before the deadline.
    Timeout,
}

/// Turn a `Transient` error from the middle of a replication stream into a
/// `Connection` error. The stream cannot be retried in place: the events
/// already consumed (and the records built from them) would be lost.
fn restart_from_checkpoint(e: ConnectorError) -> ConnectorError {
    match e {
        ConnectorError::Transient { message, .. } => {
            ConnectorError::connection(format!("{message}; restarting from the saved LSN"))
        }
        other => other,
    }
}

/// Shared pgoutput streaming used by `postgres_cdc` and `postgres_outbox`.
pub(crate) struct CdcStream {
    pub conn: String,
    pub slot: String,
    pub publication: String,
    pub tables: Vec<TableRef>,
    mgmt: Option<Client>,
    repl: Option<ReplicationClient>,
    relations: HashMap<u32, Arc<Relation>>,
    txn: Option<Txn>,
    acked: Lsn,
    server_end: u64,
}

impl CdcStream {
    pub fn new(conn: String, slot: String, publication: String, tables: Vec<TableRef>) -> Self {
        Self {
            conn,
            slot,
            publication,
            tables,
            mgmt: None,
            repl: None,
            relations: HashMap::new(),
            txn: None,
            acked: Lsn::ZERO,
            server_end: 0,
        }
    }

    pub async fn start(&mut self, checkpoint: Option<&str>) -> Result<(), ConnectorError> {
        let start_lsn = match checkpoint {
            Some(cp) => Lsn::parse(cp)
                .map_err(|e| ConnectorError::fatal(format!("stored LSN '{cp}' is invalid: {e}")))?,
            None => Lsn::ZERO,
        };
        let client = pg::connect(&self.conn).await?;
        pg::check_wal_level(&client).await?;
        pg::ensure_publication(&client, &self.publication, &self.tables).await?;
        pg::ensure_slot(&client, &self.slot).await?;
        self.mgmt = Some(client);

        let cfg = pg::replication_config(&self.conn, &self.slot, &self.publication, start_lsn)?;
        let repl = ReplicationClient::connect(cfg)
            .await
            .map_err(|e| ConnectorError::connection(format!("replication connect: {e}")))?;
        info!(slot = %self.slot, publication = %self.publication, start_lsn = %start_lsn, "CDC streaming started");
        self.repl = Some(repl);
        self.acked = start_lsn;
        self.txn = None;
        self.relations.clear();
        Ok(())
    }

    /// Next step, waiting at most until `deadline`.
    ///
    /// Every error is a `Connection` error (or `Fatal`), never `Transient`:
    /// the replication stream has already moved past whatever the caller
    /// collected in this poll, so retrying `poll` in place would silently
    /// lose those changes. A `Connection` error makes the supervisor restart
    /// the stream from the last saved LSN instead.
    pub async fn next(&mut self, deadline: Instant) -> Result<Step, ConnectorError> {
        loop {
            let repl = self
                .repl
                .as_mut()
                .ok_or_else(|| ConnectorError::connection("replication client not connected"))?;
            let ev = match tokio::time::timeout_at(deadline, repl.recv()).await {
                Err(_) => return Ok(Step::Timeout),
                Ok(Ok(Some(ev))) => ev,
                Ok(Ok(None)) => {
                    return Err(ConnectorError::connection("replication stream ended"));
                }
                Ok(Err(e)) => {
                    return Err(ConnectorError::connection(format!(
                        "replication error: {e}"
                    )));
                }
            };
            if let Some(step) = self.handle(ev).map_err(restart_from_checkpoint)? {
                return Ok(step);
            }
        }
    }

    /// Apply one replication event; `Some` when it completes a step.
    fn handle(&mut self, ev: ReplicationEvent) -> Result<Option<Step>, ConnectorError> {
        match ev {
            ReplicationEvent::Begin {
                final_lsn,
                xid,
                commit_time_micros,
            } => {
                self.txn = Some(Txn {
                    final_lsn: final_lsn.as_u64(),
                    xid,
                    commit_time_micros,
                    ordinal: 0,
                });
            }
            ReplicationEvent::Commit { end_lsn, .. } => {
                self.txn = None;
                self.server_end = self.server_end.max(end_lsn.as_u64());
                return Ok(Some(Step::Commit(end_lsn)));
            }
            ReplicationEvent::KeepAlive { wal_end, .. } => {
                self.server_end = self.server_end.max(wal_end.as_u64());
                if self.txn.is_none() && wal_end > self.acked {
                    return Ok(Some(Step::Idle(wal_end)));
                }
            }
            ReplicationEvent::XLogData { data, wal_end, .. } => {
                self.server_end = self.server_end.max(wal_end.as_u64());
                match pgoutput::parse_pgoutput_message(&data)? {
                    WalEvent::Relation(rel) => {
                        debug!(relation = rel.id, table = %rel.table, "relation");
                        self.relations.insert(rel.id, Arc::new(rel));
                    }
                    WalEvent::Insert {
                        relation_id,
                        new_tuple,
                    } => {
                        if let Some(c) =
                            self.change(relation_id, ChangeKind::Insert { new: new_tuple })?
                        {
                            return Ok(Some(Step::Change(c)));
                        }
                    }
                    WalEvent::Update {
                        relation_id,
                        old,
                        new_tuple,
                    } => {
                        if let Some(c) = self.change(
                            relation_id,
                            ChangeKind::Update {
                                old,
                                new: new_tuple,
                            },
                        )? {
                            return Ok(Some(Step::Change(c)));
                        }
                    }
                    WalEvent::Delete { relation_id, old } => {
                        if let Some(c) = self.change(relation_id, ChangeKind::Delete { old })? {
                            return Ok(Some(Step::Change(c)));
                        }
                    }
                    WalEvent::Truncate { relation_ids } => {
                        warn!(?relation_ids, "TRUNCATE is not emitted by CDC");
                    }
                    WalEvent::Begin { .. } | WalEvent::Commit { .. } | WalEvent::Unknown(_) => {}
                }
            }
            ReplicationEvent::Message { .. } => {}
            ReplicationEvent::StoppedAt { .. } => {
                return Err(ConnectorError::connection("replication stopped"));
            }
        }
        Ok(None)
    }

    fn change(
        &mut self,
        relation_id: u32,
        kind: ChangeKind,
    ) -> Result<Option<Change>, ConnectorError> {
        let relation = self.relations.get(&relation_id).cloned().ok_or_else(|| {
            ConnectorError::connection(format!(
                "change for unknown relation {relation_id}; restarting the stream"
            ))
        })?;
        let txn = self.txn.as_mut().ok_or_else(|| {
            ConnectorError::connection("change outside a transaction; restarting the stream")
        })?;
        let current = *txn;
        txn.ordinal += 1;
        Ok(Some(Change {
            relation,
            kind,
            txn: current,
        }))
    }

    /// Confirm everything up to `checkpoint` to Postgres.
    pub fn ack(&mut self, checkpoint: Option<&str>) -> Result<(), ConnectorError> {
        let Some(cp) = checkpoint else {
            return Ok(());
        };
        let lsn = Lsn::parse(cp)
            .map_err(|e| ConnectorError::fatal(format!("invalid LSN '{cp}': {e}")))?;
        if lsn > self.acked {
            self.acked = lsn;
        }
        if let Some(repl) = &self.repl {
            repl.update_applied_lsn(self.acked);
        }
        Ok(())
    }

    pub fn lag(&self) -> Option<Lag> {
        (self.server_end > 0).then(|| Lag {
            value: self.server_end.saturating_sub(self.acked.as_u64()),
            unit: LagUnit::Bytes,
        })
    }

    /// Stop streaming and wait for the replication connection to close, so
    /// the slot is free for the next start.
    pub async fn stop(&mut self) {
        if let Some(mut repl) = self.repl.take() {
            match tokio::time::timeout(Duration::from_secs(10), repl.shutdown()).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => debug!(error = %e, "replication shutdown"),
                Err(_) => {
                    warn!("replication shutdown timed out; aborting");
                    repl.abort();
                }
            }
        }
        self.mgmt = None;
        self.relations.clear();
        self.txn = None;
    }

    /// Non-destructive sample: peek (not consume) up to `max` changes from
    /// an existing slot. Creates nothing.
    pub async fn peek(&self, max: usize) -> Result<Vec<Change>, ConnectorError> {
        let client = pg::connect(&self.conn).await?;
        pg::check_wal_level(&client).await?;
        if !pg::slot_exists(&client, &self.slot).await? {
            info!(slot = %self.slot, "slot does not exist yet; it will be created on first start");
            return Ok(Vec::new());
        }
        let pub_exists = client
            .query_opt(
                "SELECT 1 FROM pg_publication WHERE pubname = $1",
                &[&self.publication],
            )
            .await
            .map_err(|e| pg::classify(&e, "query pg_publication"))?
            .is_some();
        if !pub_exists {
            info!(publication = %self.publication, "publication does not exist yet");
            return Ok(Vec::new());
        }
        let rows = client
            .query(
                "SELECT data FROM pg_logical_slot_peek_binary_changes($1, NULL, $3, \
                 'proto_version', '1', 'publication_names', $2)",
                &[&self.slot, &self.publication, &(max.min(100_000) as i32)],
            )
            .await
            .map_err(|e| pg::classify(&e, "peek slot"))?;
        let mut relations: HashMap<u32, Arc<Relation>> = HashMap::new();
        let mut txn: Option<Txn> = None;
        let mut out = Vec::new();
        for row in rows {
            let data: Vec<u8> = row.get(0);
            let ev = pgoutput::parse_pgoutput_message(&data)?;
            let kind = match ev {
                WalEvent::Begin {
                    final_lsn,
                    timestamp,
                    xid,
                } => {
                    txn = Some(Txn {
                        final_lsn,
                        xid,
                        commit_time_micros: timestamp,
                        ordinal: 0,
                    });
                    continue;
                }
                WalEvent::Relation(r) => {
                    relations.insert(r.id, Arc::new(r));
                    continue;
                }
                WalEvent::Insert {
                    relation_id,
                    new_tuple,
                } => (relation_id, ChangeKind::Insert { new: new_tuple }),
                WalEvent::Update {
                    relation_id,
                    old,
                    new_tuple,
                } => (
                    relation_id,
                    ChangeKind::Update {
                        old,
                        new: new_tuple,
                    },
                ),
                WalEvent::Delete { relation_id, old } => (relation_id, ChangeKind::Delete { old }),
                _ => continue,
            };
            if let (Some(rel), Some(t)) = (relations.get(&kind.0), txn.as_mut()) {
                out.push(Change {
                    relation: rel.clone(),
                    kind: kind.1,
                    txn: *t,
                });
                t.ordinal += 1;
                if out.len() >= max {
                    break;
                }
            }
        }
        Ok(out)
    }

    pub async fn drop_slot(&self, drop_publication: bool) -> Result<(), ConnectorError> {
        let client = pg::connect(&self.conn).await?;
        pg::drop_slot(&client, &self.slot).await?;
        if drop_publication {
            pg::drop_publication(&client, &self.publication).await?;
        }
        Ok(())
    }
}

/// Typed JSON object of a tuple. Unchanged TOAST columns are omitted and
/// returned separately. With `only_keys`, only key columns are included.
pub(crate) fn tuple_object(
    rel: &Relation,
    tuple: &[ColValue],
    only_keys: bool,
) -> (Map<String, Value>, Vec<String>) {
    let mut obj = Map::new();
    let mut unchanged = Vec::new();
    for (col, v) in rel.columns.iter().zip(tuple.iter()) {
        if only_keys && !col.is_key {
            continue;
        }
        match v {
            ColValue::Null => {
                obj.insert(col.name.clone(), Value::Null);
            }
            ColValue::Text(t) => {
                obj.insert(col.name.clone(), pg::typed_value(col.type_oid, t));
            }
            ColValue::Unchanged => unchanged.push(col.name.clone()),
        }
    }
    (obj, unchanged)
}

/// Record key from the replica-identity key columns of `tuple`.
pub(crate) fn key_of(rel: &Relation, tuple: &[ColValue]) -> Option<Bytes> {
    let idx = rel.key_columns();
    if idx.is_empty() {
        return None;
    }
    let values: Vec<(u32, Option<&str>)> = idx
        .iter()
        .map(|&i| {
            let oid = rel.columns[i].type_oid;
            match tuple.get(i) {
                Some(ColValue::Text(t)) => (oid, Some(t.as_str())),
                _ => (oid, None),
            }
        })
        .collect();
    if values.len() == 1 {
        return values[0].1.map(|t| Bytes::from(t.to_string()));
    }
    let arr: Vec<Value> = values
        .iter()
        .map(|(oid, t)| t.map(|t| pg::typed_value(*oid, t)).unwrap_or(Value::Null))
        .collect();
    Some(Bytes::from(Value::Array(arr).to_string()))
}

pub struct PostgresCdcSource {
    name: String,
    subject_template: String,
    ops: Vec<String>,
    drop_slot_on_delete: bool,
    publication_is_derived: bool,
    stream: CdcStream,
}

impl PostgresCdcSource {
    pub fn new(init: &PluginInit) -> Result<Self, ConnectorError> {
        let s: PostgresCdcSettings = settings::parse("postgres_cdc", &init.settings)?;
        pg::validate_connection_string(&s.connection)?;
        if s.tables.is_empty() {
            return Err(ConnectorError::config(
                "postgres_cdc: 'tables' must list at least one table",
            ));
        }
        let tables = s
            .tables
            .iter()
            .map(|t| TableRef::parse(t))
            .collect::<Result<Vec<_>, _>>()?;
        let ops: Vec<String> = s
            .operations
            .iter()
            .map(|o| o.to_ascii_lowercase())
            .collect();
        if let Some(bad) = ops
            .iter()
            .find(|o| !["insert", "update", "delete"].contains(&o.as_str()))
        {
            return Err(ConnectorError::config(format!(
                "postgres_cdc: unknown operation '{bad}' (use insert, update, delete)"
            )));
        }
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
        Ok(Self {
            name: init.config.name.clone(),
            subject_template: if init.config.subject_template.is_empty() {
                "{schema}.{table}.{op}".to_string()
            } else {
                init.config.subject_template.clone()
            },
            ops,
            drop_slot_on_delete: s.drop_slot_on_delete,
            publication_is_derived,
            stream: CdcStream::new(s.connection, slot, publication, tables),
        })
    }

    /// Build the record for one change, or `None` if its operation is
    /// filtered out.
    fn record(&self, c: &Change, slot: &str) -> Option<SourceRecord> {
        let rel = &c.relation;
        let (op_name, op, before, after, unchanged, key) = match &c.kind {
            ChangeKind::Insert { new } => {
                let (after, unchanged) = tuple_object(rel, new, false);
                (
                    "insert",
                    "c",
                    Value::Null,
                    Value::Object(after),
                    unchanged,
                    key_of(rel, new),
                )
            }
            ChangeKind::Update { old, new } => {
                let (after, unchanged) = tuple_object(rel, new, false);
                let before = match old {
                    Some((kind, t)) => Value::Object(tuple_object(rel, t, *kind == OldKind::Key).0),
                    None => Value::Null,
                };
                (
                    "update",
                    "u",
                    before,
                    Value::Object(after),
                    unchanged,
                    key_of(rel, new),
                )
            }
            ChangeKind::Delete { old: (kind, t) } => {
                let before = Value::Object(tuple_object(rel, t, *kind == OldKind::Key).0);
                (
                    "delete",
                    "d",
                    before,
                    Value::Null,
                    Vec::new(),
                    key_of(rel, t),
                )
            }
        };
        if !self.ops.iter().any(|o| o == op_name) {
            return None;
        }
        let lsn = Lsn::from_u64(c.txn.final_lsn).to_string();
        let mut envelope = json!({
            "op": op,
            "before": before,
            "after": after,
            "source": {
                "connector": self.name,
                "lsn": lsn,
                "txid": c.txn.xid,
                "schema": rel.schema,
                "table": rel.table,
                "ts_ms": c.txn.ts_ms(),
            },
        });
        if !unchanged.is_empty() {
            envelope["__unchanged"] = json!(unchanged);
        }
        let subject = crate::subject::render(
            &self.subject_template,
            &[
                ("schema", &rel.schema),
                ("table", &rel.table),
                ("op", op_name),
            ],
            Some(&envelope),
        );
        Some(SourceRecord {
            key,
            value: Bytes::from(envelope.to_string()),
            subject,
            headers: vec![
                (
                    "x-idempotency-key".into(),
                    format!("pgcdc:{slot}:{lsn}:{}", c.txn.ordinal),
                ),
                ("x-exspeed-source".into(), "postgres_cdc".into()),
                ("x-op".into(), op.into()),
                ("x-table".into(), format!("{}.{}", rel.schema, rel.table)),
            ],
        })
    }
}

#[async_trait]
impl SourceConnector for PostgresCdcSource {
    async fn start(&mut self, checkpoint: Option<String>) -> Result<(), ConnectorError> {
        self.stream.start(checkpoint.as_deref()).await
    }

    async fn poll(&mut self, max_batch: usize) -> Result<SourceBatch, ConnectorError> {
        let started = Instant::now();
        let mut records = Vec::new();
        let mut checkpoint: Option<Lsn> = None;
        loop {
            if records.len() >= max_batch {
                break;
            }
            // Wait up to 1 s for the first event, then briefly for more.
            let deadline = if records.is_empty() && checkpoint.is_none() {
                started + Duration::from_secs(1)
            } else {
                (Instant::now() + Duration::from_millis(20)).min(started + Duration::from_secs(1))
            };
            match self.stream.next(deadline).await? {
                Step::Change(c) => {
                    let slot = self.stream.slot.clone();
                    if let Some(r) = self.record(&c, &slot) {
                        records.push(r);
                    }
                }
                Step::Commit(lsn) | Step::Idle(lsn) => checkpoint = Some(lsn),
                Step::Timeout => break,
            }
        }
        Ok(SourceBatch {
            records,
            checkpoint: checkpoint.map(|l| l.to_string()),
        })
    }

    async fn ack(&mut self, checkpoint: Option<&str>) -> Result<(), ConnectorError> {
        self.stream.ack(checkpoint)
    }

    async fn stop(&mut self) -> Result<(), ConnectorError> {
        self.stream.stop().await;
        Ok(())
    }

    fn lag(&self) -> Option<Lag> {
        self.stream.lag()
    }

    async fn dry_run(&mut self, max: usize) -> Result<Vec<SourceRecord>, ConnectorError> {
        let slot = self.stream.slot.clone();
        let changes = self.stream.peek(max.max(1) * 4).await?;
        Ok(changes
            .iter()
            .filter_map(|c| self.record(c, &slot))
            .take(max)
            .collect())
    }

    async fn cleanup(&mut self) -> Result<(), ConnectorError> {
        if self.drop_slot_on_delete {
            self.stream.drop_slot(self.publication_is_derived).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builtin::pgoutput::ColumnDef;
    use crate::config::{ConnectorConfig, ConnectorType};

    fn init(settings: Value) -> PluginInit {
        let (m, _) = exspeed_common::Metrics::new();
        PluginInit {
            config: ConnectorConfig::new("users-cdc", ConnectorType::Source, "postgres_cdc", "s"),
            settings: settings.as_object().unwrap().clone(),
            metrics: Arc::new(m),
        }
    }

    fn rel() -> Arc<Relation> {
        Arc::new(Relation {
            id: 1,
            schema: "public".into(),
            table: "users".into(),
            replica_identity: b'd',
            columns: vec![
                ColumnDef {
                    name: "name".into(),
                    type_oid: 25,
                    type_modifier: -1,
                    is_key: false,
                },
                ColumnDef {
                    name: "id".into(),
                    type_oid: 23,
                    type_modifier: -1,
                    is_key: true,
                },
                ColumnDef {
                    name: "doc".into(),
                    type_oid: 3802,
                    type_modifier: -1,
                    is_key: false,
                },
                ColumnDef {
                    name: "active".into(),
                    type_oid: 16,
                    type_modifier: -1,
                    is_key: false,
                },
            ],
        })
    }

    fn txn() -> Txn {
        Txn {
            final_lsn: 0x16B3748,
            xid: 7,
            commit_time_micros: 0,
            ordinal: 2,
        }
    }

    /// A malformed pgoutput message in the middle of a poll must restart the
    /// stream from the saved LSN (`Connection`), never be retried in place
    /// (`Transient`): the runtime would re-poll a stream that has already
    /// moved past the changes collected so far, losing them.
    #[test]
    fn stream_errors_restart_instead_of_retrying_in_place() {
        use crate::traits::ErrorKind;
        let mut st = CdcStream::new("postgres://h/db".into(), "s".into(), "p".into(), vec![]);
        st.handle(ReplicationEvent::Begin {
            final_lsn: Lsn::from(100u64),
            xid: 1,
            commit_time_micros: 0,
        })
        .unwrap();
        for bad in [&b""[..], b"I", b"I\x00\x00\x00\x01N", b"R\x00\x00"] {
            let Err(err) = st.handle(ReplicationEvent::XLogData {
                wal_start: Lsn::from(100u64),
                wal_end: Lsn::from(100u64),
                server_time_micros: 0,
                data: Bytes::copy_from_slice(bad),
            }) else {
                panic!("{bad:?} must fail");
            };
            assert_eq!(err.kind(), ErrorKind::Connection, "{bad:?}: {err}");
        }
        // A change for a relation never announced also restarts.
        let mut ins = vec![b'I'];
        ins.extend_from_slice(&42u32.to_be_bytes());
        ins.extend_from_slice(b"N\x00\x00");
        let Err(err) = st.handle(ReplicationEvent::XLogData {
            wal_start: Lsn::from(100u64),
            wal_end: Lsn::from(100u64),
            server_time_micros: 0,
            data: Bytes::from(ins),
        }) else {
            panic!("unknown relation must fail");
        };
        assert_eq!(err.kind(), ErrorKind::Connection, "{err}");
        // Any transient error surfacing from the stream is converted.
        let e = restart_from_checkpoint(ConnectorError::transient("x"));
        assert_eq!(e.kind(), ErrorKind::Connection);
        let e = restart_from_checkpoint(ConnectorError::fatal("x"));
        assert_eq!(e.kind(), ErrorKind::Fatal);
    }

    #[test]
    fn settings_validation() {
        let ok = json!({"connection": "postgres://u:p@localhost/db", "tables": ["public.users"]});
        let s = PostgresCdcSource::new(&init(ok)).unwrap();
        assert_eq!(s.stream.slot, "exspeed_users_cdc_slot");
        assert!(PostgresCdcSource::new(&init(
            json!({"connection": "postgres://h/db", "tables": []})
        ))
        .is_err());
        assert!(PostgresCdcSource::new(&init(
            json!({"connection": "postgres://h/db", "tables": "t", "mode": "cdc"})
        ))
        .is_err());
        assert!(PostgresCdcSource::new(&init(
            json!({"connection": "postgres://h/db", "tables": "t", "slot_name": "Bad"})
        ))
        .is_err());
        assert!(PostgresCdcSource::new(&init(
            json!({"connection": "postgres://h/db", "tables": "t", "operations": ["upsert"]})
        ))
        .is_err());
    }

    #[test]
    fn insert_envelope_typed_values_and_key() {
        let s = PostgresCdcSource::new(&init(
            json!({"connection": "postgres://h/db", "tables": "users"}),
        ))
        .unwrap();
        let c = Change {
            relation: rel(),
            kind: ChangeKind::Insert {
                new: vec![
                    ColValue::Text("ann".into()),
                    ColValue::Text("42".into()),
                    ColValue::Text(r#"{"a":1}"#.into()),
                    ColValue::Text("t".into()),
                ],
            },
            txn: txn(),
        };
        let r = s.record(&c, "slot").unwrap();
        assert_eq!(r.key.as_deref(), Some(&b"42"[..]));
        assert_eq!(r.subject, "public.users.insert");
        let v: Value = serde_json::from_slice(&r.value).unwrap();
        assert_eq!(v["op"], "c");
        assert_eq!(v["before"], Value::Null);
        assert_eq!(v["after"]["id"], 42);
        assert_eq!(v["after"]["doc"]["a"], 1);
        assert_eq!(v["after"]["active"], true);
        assert_eq!(v["source"]["lsn"], "0/16B3748");
        assert_eq!(v["source"]["txid"], 7);
        assert!(r
            .headers
            .contains(&("x-idempotency-key".into(), "pgcdc:slot:0/16B3748:2".into())));
    }

    #[test]
    fn update_with_unchanged_toast_and_delete_with_key_only() {
        let s = PostgresCdcSource::new(&init(
            json!({"connection": "postgres://h/db", "tables": "users"}),
        ))
        .unwrap();
        let upd = Change {
            relation: rel(),
            kind: ChangeKind::Update {
                old: None,
                new: vec![
                    ColValue::Text("bob".into()),
                    ColValue::Text("42".into()),
                    ColValue::Unchanged,
                    ColValue::Null,
                ],
            },
            txn: txn(),
        };
        let v: Value = serde_json::from_slice(&s.record(&upd, "s").unwrap().value).unwrap();
        assert_eq!(v["op"], "u");
        assert!(
            v["after"].get("doc").is_none(),
            "unchanged TOAST must not become null"
        );
        assert_eq!(v["after"]["active"], Value::Null);
        assert_eq!(v["__unchanged"], json!(["doc"]));

        let del = Change {
            relation: rel(),
            kind: ChangeKind::Delete {
                old: (
                    OldKind::Key,
                    vec![
                        ColValue::Null,
                        ColValue::Text("42".into()),
                        ColValue::Null,
                        ColValue::Null,
                    ],
                ),
            },
            txn: txn(),
        };
        let r = s.record(&del, "s").unwrap();
        assert_eq!(r.key.as_deref(), Some(&b"42"[..]));
        let v: Value = serde_json::from_slice(&r.value).unwrap();
        assert_eq!(v["op"], "d");
        assert_eq!(v["before"], json!({"id": 42}));
        assert_eq!(v["after"], Value::Null);
    }

    #[test]
    fn composite_and_missing_keys() {
        let mut r = (*rel()).clone();
        r.columns[0].is_key = true; // name + id
        let k = key_of(
            &r,
            &[
                ColValue::Text("ann".into()),
                ColValue::Text("42".into()),
                ColValue::Null,
                ColValue::Null,
            ],
        );
        assert_eq!(k.as_deref(), Some(&br#"["ann",42]"#[..]));
        for c in &mut r.columns {
            c.is_key = false;
        }
        assert_eq!(key_of(&r, &[]), None);
    }

    #[test]
    fn operations_filter() {
        let s = PostgresCdcSource::new(&init(
            json!({"connection": "postgres://h/db", "tables": "users", "operations": "insert"}),
        ))
        .unwrap();
        let del = Change {
            relation: rel(),
            kind: ChangeKind::Delete {
                old: (OldKind::Key, vec![]),
            },
            txn: txn(),
        };
        assert!(s.record(&del, "s").is_none());
    }
}
