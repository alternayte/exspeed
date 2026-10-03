//! [`ExqlEngine`]: bounded queries, continuous-query lifecycle, tables and
//! connections.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::session_state::SessionState;
use exspeed_broker::leadership::ClusterLeadership;
use exspeed_broker::log::{Log, LogError};
use exspeed_common::metrics::Metrics;
use exspeed_common::{Offset, StreamName};
use exspeed_streams::{ReadLimits, StorageEngine, StorageError};
use futures_util::FutureExt;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as Json};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::bounded::QueryResult;
use crate::catalog::Resolver;
use crate::continuous::checkpoint::ckpt_stream;
use crate::continuous::plan::{compile, Dataflow};
use crate::continuous::runner::{QueryStats, RunCtx, Runner, Sink, H_OP, H_QUERY};
use crate::convert::batch_rows_json;
use crate::error::ExqlError;
use crate::external::{ConnectionConfig, ConnectionRegistry, ExternalTables};
use crate::session::{build_state, runtime_env, ExqlConfig};
use crate::sql::{parse_statement, validate_object_name, validate_query_id, CreateKind, CreateQuery, Statement};
use crate::tables::{rows_to_batch, MaterializedTable, TableRegistry};

/// What a continuous query writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum QueryKind {
    Stream,
    Table,
}

/// Persisted desired state of a query.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DesiredState {
    /// Runs whenever this node is the leader.
    Running,
    /// Paused by the user (`PAUSE QUERY`); state is kept.
    Paused,
    /// Stopped after a failure; `RESUME QUERY` restarts it.
    Stopped,
}

/// The persisted definition of a continuous query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryDef {
    pub id: String,
    pub sql: String,
    pub kind: QueryKind,
    /// Output stream (for `STREAM`) or table name (for `TABLE`; its changelog
    /// stream has the same name).
    pub name: String,
    pub desired: DesiredState,
    #[serde(default)]
    pub error: Option<String>,
    pub created_at: String,
}

/// Runtime status of a query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryStatus {
    Running,
    Paused,
    Failed(String),
    /// Desired running, but not running here (not the leader, or starting).
    Pending,
}

impl QueryStatus {
    fn as_str(&self) -> &'static str {
        match self {
            QueryStatus::Running => "running",
            QueryStatus::Paused => "paused",
            QueryStatus::Failed(_) => "failed",
            QueryStatus::Pending => "pending",
        }
    }
}

/// A query as reported by the API.
#[derive(Debug, Clone, Serialize)]
pub struct QueryInfo {
    pub id: String,
    pub sql: String,
    pub kind: QueryKind,
    pub name: String,
    /// Stream the query writes (the table's changelog for tables).
    pub target_stream: String,
    pub status: String,
    pub desired_state: DesiredState,
    pub error: Option<String>,
    pub created_at: String,
    pub stats: Json,
}

/// A materialized table as reported by the API.
#[derive(Debug, Clone, Serialize)]
pub struct TableInfo {
    pub name: String,
    pub query_id: String,
    pub columns: Vec<String>,
    pub row_count: usize,
}

/// Result of [`ExqlEngine::execute`].
#[derive(Debug, Clone)]
pub enum StatementResult {
    Rows(QueryResult),
    Created(QueryInfo),
    Dropped { kind: &'static str, name: String },
    Query(QueryInfo),
}

impl StatementResult {
    pub fn to_json(&self) -> Json {
        match self {
            StatementResult::Rows(r) => r.to_json(),
            StatementResult::Created(q) => {
                let mut v = json!(q);
                v["query_id"] = json!(q.id);
                v
            }
            StatementResult::Dropped { kind, name } => json!({"status": "dropped", "kind": kind, "name": name}),
            StatementResult::Query(q) => {
                let mut v = json!(q);
                v["query_id"] = json!(q.id);
                v
            }
        }
    }

    pub fn http_status(&self) -> u16 {
        match self {
            StatementResult::Created(_) => 201,
            _ => 200,
        }
    }
}

struct Entry {
    def: QueryDef,
    status: QueryStatus,
    stats: Arc<QueryStats>,
    cancel: Option<CancellationToken>,
    handle: Option<JoinHandle<()>>,
    /// Bumped on every start so a finishing task doesn't clobber a newer run.
    generation: u64,
}

/// The ExQL engine.
pub struct ExqlEngine {
    log: Arc<Log>,
    storage: Arc<dyn StorageEngine>,
    cfg: ExqlConfig,
    runtime: Arc<RuntimeEnv>,
    pub connection_registry: Arc<ConnectionRegistry>,
    external: Arc<ExternalTables>,
    tables: Arc<TableRegistry>,
    queries: Mutex<HashMap<String, Entry>>,
    pub leadership: Arc<ClusterLeadership>,
    pub metrics: Arc<Metrics>,
    data_dir: PathBuf,
    tenure: Mutex<Option<CancellationToken>>,
    ddl: tokio::sync::Mutex<()>,
    id_counter: AtomicU64,
}

fn now_rfc3339() -> String {
    chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
}

fn write_atomic(path: &Path, data: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    let dir = path.parent().expect("file in a directory");
    std::fs::create_dir_all(dir)?;
    let tmp = dir.join(format!(
        ".{}.tmp",
        path.file_name().and_then(|n| n.to_str()).unwrap_or("query")
    ));
    {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(data)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path)?;
    if let Ok(d) = std::fs::File::open(dir) {
        let _ = d.sync_all();
    }
    Ok(())
}

fn stream_name(name: &str) -> Result<StreamName, ExqlError> {
    StreamName::try_from(name).map_err(|e| ExqlError::Plan(e.to_string()))
}

impl ExqlEngine {
    /// Create an engine. Query output is written through `log`; reads go
    /// to `log.storage()`.
    pub fn new(
        log: Arc<Log>,
        data_dir: PathBuf,
        leadership: Arc<ClusterLeadership>,
        metrics: Arc<Metrics>,
        cfg: ExqlConfig,
    ) -> Result<Self, ExqlError> {
        let storage = log.storage().clone();
        let connection_registry = Arc::new(ConnectionRegistry::new(data_dir.clone()));
        let external = Arc::new(ExternalTables::new(
            connection_registry.clone(),
            cfg.external.clone(),
        ));
        let runtime = runtime_env(&cfg)?;
        Ok(Self {
            log,
            storage,
            cfg,
            runtime,
            connection_registry,
            external,
            tables: Arc::new(TableRegistry::new()),
            queries: Mutex::new(HashMap::new()),
            leadership,
            metrics,
            data_dir,
            tenure: Mutex::new(None),
            ddl: tokio::sync::Mutex::new(()),
            id_counter: AtomicU64::new(0),
        })
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    pub fn config(&self) -> &ExqlConfig {
        &self.cfg
    }

    pub fn tables(&self) -> &Arc<TableRegistry> {
        &self.tables
    }

    fn queries_dir(&self) -> PathBuf {
        self.data_dir.join("exql").join("queries")
    }

    fn resolver(&self, allow_external: bool) -> Resolver {
        Resolver {
            storage: self.storage.clone(),
            tables: self.tables.clone(),
            external: self.external.clone(),
            allow_external,
        }
    }

    fn state(&self, allow_external: bool) -> Result<SessionState, ExqlError> {
        build_state(&self.cfg, self.runtime.clone(), self.resolver(allow_external))
    }

    // -- persistence --------------------------------------------------------

    fn persist(&self, def: &QueryDef) -> Result<(), ExqlError> {
        validate_query_id(&def.id)?;
        let path = self.queries_dir().join(format!("{}.json", def.id));
        let data = serde_json::to_vec_pretty(def).map_err(|e| ExqlError::Internal(e.to_string()))?;
        write_atomic(&path, &data).map_err(|e| ExqlError::Storage(format!("persist query {}: {e}", def.id)))
    }

    fn unpersist(&self, id: &str) -> Result<(), ExqlError> {
        validate_query_id(id)?;
        let path = self.queries_dir().join(format!("{id}.json"));
        match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(ExqlError::Storage(format!("delete query {id}: {e}"))),
        }
    }

    /// Load connections and persisted queries; register tables and fill
    /// them from their changelogs. Queries start when this node leads
    /// ([`Self::resume_all_and_run`]).
    pub async fn load(&self) -> Result<(), ExqlError> {
        self.connection_registry.load_all();
        let dir = self.queries_dir();
        let mut defs: Vec<QueryDef> = vec![];
        if let Ok(entries) = std::fs::read_dir(&dir) {
            for e in entries.flatten() {
                let path = e.path();
                if path.extension().and_then(|x| x.to_str()) != Some("json") {
                    continue;
                }
                match std::fs::read(&path)
                    .map_err(|e| e.to_string())
                    .and_then(|b| serde_json::from_slice::<QueryDef>(&b).map_err(|e| e.to_string()))
                {
                    Ok(d) if validate_query_id(&d.id).is_ok() => defs.push(d),
                    Ok(_) => warn!(path = %path.display(), "ignoring query file with an invalid id"),
                    Err(e) => warn!(path = %path.display(), "ignoring unreadable query file: {e}"),
                }
            }
        }
        // Tables first, so queries joining them can be planned.
        defs.sort_by_key(|d| (d.kind != QueryKind::Table, d.created_at.clone()));
        for def in defs {
            let mut status = match def.desired {
                DesiredState::Running => QueryStatus::Pending,
                DesiredState::Paused => QueryStatus::Paused,
                DesiredState::Stopped => {
                    QueryStatus::Failed(def.error.clone().unwrap_or_else(|| "stopped".into()))
                }
            };
            if def.kind == QueryKind::Table {
                if let Err(e) = self.register_table(&def).await {
                    warn!(query = %def.id, "cannot plan table query: {e}");
                    status = QueryStatus::Failed(e.to_string());
                }
            }
            info!(query = %def.id, name = %def.name, status = status.as_str(), "loaded continuous query");
            self.queries.lock().unwrap().insert(
                def.id.clone(),
                Entry {
                    def,
                    status,
                    stats: Arc::new(QueryStats::default()),
                    cancel: None,
                    handle: None,
                    generation: 0,
                },
            );
        }
        Ok(())
    }

    async fn plan_def(&self, def: &QueryDef) -> Result<(CreateQuery, Dataflow), ExqlError> {
        let Statement::Create(c) = parse_statement(&def.sql)? else {
            return Err(ExqlError::Internal(format!("query {} is not a CREATE", def.id)));
        };
        let state = self.state(false)?;
        let df = compile(&state, &c.query, c.kind, &self.tables, self.cfg.default_grace_ms).await?;
        Ok((c, df))
    }

    async fn register_table(&self, def: &QueryDef) -> Result<Arc<MaterializedTable>, ExqlError> {
        let (_, df) = self.plan_def(def).await?;
        let table = Arc::new(MaterializedTable::new(&def.name, &def.id, df.out_schema.clone()));
        self.load_changelog(&table, &def.name, &def.id).await?;
        self.tables.insert(table.clone());
        Ok(table)
    }

    /// Fill a table from its changelog stream (latest row per key).
    async fn load_changelog(&self, table: &MaterializedTable, stream: &str, qid: &str) -> Result<(), ExqlError> {
        let stream = stream_name(stream)?;
        let (earliest, end) = match self.storage.stream_bounds(&stream).await {
            Ok(b) => b,
            Err(StorageError::StreamNotFound(_)) => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        let schema = table.schema_ref();
        let mut p = earliest.0;
        while p < end.0 {
            let b = self
                .storage
                .read_batch(
                    &stream,
                    Offset(p),
                    ReadLimits {
                        max_records: 1000,
                        max_bytes: 8 * 1024 * 1024,
                    },
                )
                .await?;
            if b.records.is_empty() {
                break;
            }
            for r in &b.records {
                let h = |k: &str| r.headers.iter().find(|(x, _)| x == k).map(|(_, v)| v.as_str());
                if h(H_QUERY) != Some(qid) {
                    continue;
                }
                let key = r
                    .key
                    .as_ref()
                    .map(|k| String::from_utf8_lossy(k).into_owned())
                    .unwrap_or_default();
                if h(H_OP) == Some("delete") {
                    table.delete_str(&key);
                    continue;
                }
                let Ok(Json::Object(obj)) = serde_json::from_slice::<Json>(&r.value) else {
                    continue;
                };
                let row = schema
                    .fields()
                    .iter()
                    .map(|f| crate::convert::json_to_scalar(obj.get(f.name()).unwrap_or(&Json::Null), f))
                    .collect();
                table.upsert_str(key, row);
            }
            p = b.next_offset.0;
        }
        Ok(())
    }

    // -- bounded ------------------------------------------------------------

    /// Run a bounded query (SELECT / WITH / EXPLAIN …).
    pub async fn execute_bounded(&self, sql: &str) -> Result<QueryResult, ExqlError> {
        let state = self.state(true)?;
        crate::bounded::execute(state, sql, &self.cfg).await
    }

    /// Run any ExQL statement: a bounded query, `CREATE STREAM/TABLE`,
    /// `DROP STREAM/TABLE/QUERY`, `PAUSE/RESUME QUERY`.
    pub async fn execute(self: &Arc<Self>, sql: &str) -> Result<StatementResult, ExqlError> {
        match parse_statement(sql)? {
            Statement::Query(_) => Ok(StatementResult::Rows(self.execute_bounded(sql).await?)),
            Statement::Create(c) => Ok(StatementResult::Created(self.create(sql, c).await?)),
            Statement::Drop { kind, name, if_exists } => {
                self.drop_object(kind, &name, if_exists).await?;
                Ok(StatementResult::Dropped {
                    kind: match kind {
                        CreateKind::Stream => "stream",
                        CreateKind::Table => "table",
                    },
                    name,
                })
            }
            Statement::PauseQuery(id) => Ok(StatementResult::Query(self.pause_query(&id).await?)),
            Statement::ResumeQuery(id) => Ok(StatementResult::Query(self.resume_query(&id).await?)),
            Statement::DropQuery(id) => {
                self.drop_query(&id).await?;
                Ok(StatementResult::Dropped { kind: "query", name: id })
            }
        }
    }

    // -- continuous: create / drop ------------------------------------------

    /// Create a continuous query from `CREATE STREAM|TABLE … AS SELECT`
    /// (or the `VIEW` / `MATERIALIZED VIEW` aliases).
    pub async fn create_continuous(self: &Arc<Self>, sql: &str) -> Result<QueryInfo, ExqlError> {
        match parse_statement(sql)? {
            Statement::Create(c) => self.create(sql, c).await,
            _ => Err(ExqlError::parse(
                "expected CREATE STREAM <name> AS SELECT … or CREATE TABLE <name> AS SELECT …",
            )),
        }
    }

    fn new_id(&self, name: &str) -> String {
        let n = self.id_counter.fetch_add(1, Ordering::Relaxed);
        let t = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or_default();
        let mix = (t as u64) ^ (n.wrapping_mul(0x9e37_79b9_7f4a_7c15)) ^ (std::process::id() as u64) << 32;
        let short: String = name.chars().take(40).collect();
        format!("{short}_{:08x}", (mix ^ (mix >> 29)) as u32)
    }

    fn query_for_name(&self, name: &str) -> Option<QueryDef> {
        self.queries
            .lock()
            .unwrap()
            .values()
            .find(|e| e.def.name == name)
            .map(|e| e.def.clone())
    }

    async fn create(self: &Arc<Self>, sql: &str, c: CreateQuery) -> Result<QueryInfo, ExqlError> {
        let _g = self.ddl.lock().await;
        validate_object_name(&c.name)?;
        let out = stream_name(&c.name)?;
        if let Some(existing) = self.query_for_name(&c.name) {
            if c.if_not_exists {
                return self.info(&existing.id);
            }
            if !c.or_replace {
                return Err(ExqlError::Conflict(format!(
                    "'{}' is already written by query {}; use CREATE OR REPLACE or drop it first",
                    c.name, existing.id
                )));
            }
        }
        if c.kind == CreateKind::Stream && self.tables.get(&c.name).is_some() && self.query_for_name(&c.name).is_none() {
            return Err(ExqlError::Conflict(format!("'{}' is a table", c.name)));
        }
        // Validate everything before persisting anything.
        let state = self.state(false)?;
        let df = compile(&state, &c.query, c.kind, &self.tables, self.cfg.default_grace_ms).await?;
        if df.sources.iter().any(|s| s.stream == out) {
            return Err(ExqlError::Plan(format!(
                "query reads its own output '{}'",
                c.name
            )));
        }
        if df.tables_read.iter().any(|t| t == &c.name) {
            return Err(ExqlError::Plan(format!("query reads its own table '{}'", c.name)));
        }
        let exists = match self.storage.stream_bounds(&out).await {
            Ok(_) => true,
            Err(StorageError::StreamNotFound(_)) => false,
            Err(e) => return Err(e.into()),
        };
        if c.kind == CreateKind::Table && exists && self.query_for_name(&c.name).is_none() {
            return Err(ExqlError::Conflict(format!(
                "stream '{}' already exists; a table needs its own changelog stream",
                c.name
            )));
        }
        if !self.leadership.is_currently_leader() || !self.log.can_write() {
            return Err(ExqlError::NotLeader);
        }
        if let Some(existing) = self.query_for_name(&c.name) {
            // OR REPLACE: drop the old query (keep the output stream).
            self.remove_query(&existing.id).await?;
        }
        self.log.ensure_stream(&out).await?;
        let id = self.new_id(&c.name);
        let def = QueryDef {
            id: id.clone(),
            sql: sql.trim().trim_end_matches(';').trim().to_string(),
            kind: match c.kind {
                CreateKind::Stream => QueryKind::Stream,
                CreateKind::Table => QueryKind::Table,
            },
            name: c.name.clone(),
            desired: DesiredState::Running,
            error: None,
            created_at: now_rfc3339(),
        };
        if def.kind == QueryKind::Table {
            let table = Arc::new(MaterializedTable::new(&def.name, &def.id, df.out_schema.clone()));
            self.tables.insert(table);
        }
        if let Err(e) = self.persist(&def) {
            self.tables.remove(&def.name);
            return Err(e);
        }
        self.queries.lock().unwrap().insert(
            id.clone(),
            Entry {
                def,
                status: QueryStatus::Pending,
                stats: Arc::new(QueryStats::default()),
                cancel: None,
                handle: None,
                generation: 0,
            },
        );
        let tenure = self.tenure_token().await;
        self.start(&id, &tenure);
        info!(query = %id, name = %c.name, "created continuous query");
        self.info(&id)
    }

    async fn tenure_token(&self) -> CancellationToken {
        let t = self.tenure.lock().unwrap().clone();
        match t {
            Some(t) if !t.is_cancelled() => t,
            _ => self.leadership.current_child_token().await,
        }
    }

    /// Stop a query's task and wait for it (it writes a final checkpoint).
    async fn stop_task(&self, id: &str) {
        let (cancel, handle) = {
            let mut q = self.queries.lock().unwrap();
            match q.get_mut(id) {
                Some(e) => {
                    e.generation += 1;
                    (e.cancel.take(), e.handle.take())
                }
                None => (None, None),
            }
        };
        if let Some(c) = cancel {
            c.cancel();
        }
        if let Some(h) = handle {
            if tokio::time::timeout(Duration::from_secs(30), h).await.is_err() {
                warn!(query = %id, "query did not stop within 30s");
            }
        }
    }

    /// Stop and forget a query; delete its checkpoint stream. The output
    /// stream / changelog is kept. A table query's table is unregistered.
    async fn remove_query(&self, id: &str) -> Result<QueryDef, ExqlError> {
        validate_query_id(id)?;
        let def = self
            .queries
            .lock()
            .unwrap()
            .get(id)
            .map(|e| e.def.clone())
            .ok_or_else(|| ExqlError::NotFound(format!("query '{id}' not found")))?;
        if def.kind == QueryKind::Table {
            let readers = self.readers_of_table(&def.name, Some(id));
            if !readers.is_empty() {
                return Err(ExqlError::Conflict(format!(
                    "table '{}' is read by queries {readers:?}; drop them first",
                    def.name
                )));
            }
        }
        self.stop_task(id).await;
        self.unpersist(id)?;
        self.queries.lock().unwrap().remove(id);
        if def.kind == QueryKind::Table {
            self.tables.remove(&def.name);
        }
        let ck = ckpt_stream(id)?;
        match self.log.delete_stream(&ck).await {
            Ok(()) | Err(LogError::Storage(StorageError::StreamNotFound(_))) => {}
            Err(e) => warn!(query = %id, "could not delete checkpoint stream: {e}"),
        }
        Ok(def)
    }

    fn readers_of_table(&self, table: &str, except: Option<&str>) -> Vec<String> {
        let q = self.queries.lock().unwrap();
        q.values()
            .filter(|e| Some(e.def.id.as_str()) != except)
            .filter(|e| {
                // Cheap textual pre-check, then a parse.
                e.def.sql.contains(table)
                    && matches!(parse_statement(&e.def.sql), Ok(Statement::Create(c)) if c.query.relations.iter().any(|r| r.name == table))
            })
            .map(|e| e.def.id.clone())
            .collect()
    }

    fn readers_of_stream(&self, stream: &str) -> Vec<String> {
        let q = self.queries.lock().unwrap();
        q.values()
            .filter(|e| {
                e.def.name != stream
                    && e.def.sql.contains(stream)
                    && matches!(parse_statement(&e.def.sql), Ok(Statement::Create(c)) if c.query.relations.iter().any(|r| r.name == stream))
            })
            .map(|e| e.def.id.clone())
            .collect()
    }

    /// `DROP QUERY <id>`: stop and remove the query; keep its output.
    pub async fn drop_query(&self, id: &str) -> Result<(), ExqlError> {
        let _g = self.ddl.lock().await;
        self.remove_query(id).await.map(|_| ())
    }

    /// `DROP STREAM|TABLE <name>`: drop the query writing it (if any) and
    /// delete the stream / changelog.
    pub async fn drop_object(&self, kind: CreateKind, name: &str, if_exists: bool) -> Result<(), ExqlError> {
        let _g = self.ddl.lock().await;
        let stream = stream_name(name)?;
        let owner = self.query_for_name(name);
        match kind {
            CreateKind::Table => {
                let Some(def) = owner.filter(|d| d.kind == QueryKind::Table) else {
                    if if_exists {
                        return Ok(());
                    }
                    return Err(ExqlError::NotFound(format!("table '{name}' not found")));
                };
                self.remove_query(&def.id).await?;
            }
            CreateKind::Stream => {
                if self.tables.get(name).is_some() {
                    return Err(ExqlError::Conflict(format!("'{name}' is a table; use DROP TABLE")));
                }
                let readers = self.readers_of_stream(name);
                if !readers.is_empty() {
                    return Err(ExqlError::Conflict(format!(
                        "stream '{name}' is read by queries {readers:?}; drop them first"
                    )));
                }
                if let Some(def) = owner {
                    self.remove_query(&def.id).await?;
                }
            }
        }
        match self.log.delete_stream(&stream).await {
            Ok(()) => Ok(()),
            Err(LogError::Storage(StorageError::StreamNotFound(_))) => {
                if if_exists || kind == CreateKind::Table {
                    Ok(())
                } else {
                    Err(ExqlError::NotFound(format!("stream '{name}' not found")))
                }
            }
            Err(e) => Err(e.into()),
        }
    }

    // -- continuous: pause / resume -----------------------------------------

    fn set_desired(&self, id: &str, desired: DesiredState, error: Option<String>) -> Result<QueryDef, ExqlError> {
        validate_query_id(id)?;
        let def = {
            let mut q = self.queries.lock().unwrap();
            let e = q
                .get_mut(id)
                .ok_or_else(|| ExqlError::NotFound(format!("query '{id}' not found")))?;
            e.def.desired = desired;
            e.def.error = error;
            e.def.clone()
        };
        self.persist(&def)?;
        Ok(def)
    }

    /// `PAUSE QUERY <id>`: stop processing; state and position are kept.
    pub async fn pause_query(&self, id: &str) -> Result<QueryInfo, ExqlError> {
        let _g = self.ddl.lock().await;
        self.set_desired(id, DesiredState::Paused, None)?;
        self.stop_task(id).await;
        if let Some(e) = self.queries.lock().unwrap().get_mut(id) {
            e.status = QueryStatus::Paused;
        }
        self.info(id)
    }

    /// `RESUME QUERY <id>`: continue a paused or failed query from its last
    /// checkpoint.
    pub async fn resume_query(self: &Arc<Self>, id: &str) -> Result<QueryInfo, ExqlError> {
        let _g = self.ddl.lock().await;
        let def = self.set_desired(id, DesiredState::Running, None)?;
        if def.kind == QueryKind::Table && self.tables.get(&def.name).is_none() {
            self.register_table(&def).await?;
        }
        if let Some(e) = self.queries.lock().unwrap().get_mut(id) {
            if !matches!(e.status, QueryStatus::Running) {
                e.status = QueryStatus::Pending;
            }
        }
        if self.leadership.is_currently_leader() {
            let tenure = self.tenure_token().await;
            self.start(id, &tenure);
        }
        self.info(id)
    }

    // -- running ------------------------------------------------------------

    /// Resume every query whose desired state is running, under `token`
    /// (one leadership tenure). Returns when `token` is cancelled.
    pub async fn resume_all_and_run(self: Arc<Self>, token: CancellationToken) {
        *self.tenure.lock().unwrap() = Some(token.clone());
        let ids: Vec<String> = {
            let q = self.queries.lock().unwrap();
            let mut v: Vec<(bool, String, String)> = q
                .values()
                .filter(|e| e.def.desired == DesiredState::Running)
                .filter(|e| !matches!(e.status, QueryStatus::Failed(_)))
                .map(|e| (e.def.kind != QueryKind::Table, e.def.created_at.clone(), e.def.id.clone()))
                .collect();
            v.sort();
            v.into_iter().map(|(_, _, id)| id).collect()
        };
        for id in ids {
            self.start(&id, &token);
        }
        token.cancelled().await;
        info!("ExQL leader tenure ended");
    }

    /// Cancel all running queries and wait for their final checkpoints.
    pub async fn shutdown(&self) {
        let ids: Vec<String> = self.queries.lock().unwrap().keys().cloned().collect();
        for id in ids {
            self.stop_task(&id).await;
        }
    }

    /// Abort every query task without a final checkpoint (simulates a
    /// crash; for tests).
    #[doc(hidden)]
    pub async fn abort_all(&self) {
        let handles: Vec<JoinHandle<()>> = {
            let mut q = self.queries.lock().unwrap();
            q.values_mut()
                .filter_map(|e| {
                    e.generation += 1;
                    e.cancel = None;
                    e.handle.take()
                })
                .collect()
        };
        for h in handles {
            h.abort();
            let _ = h.await;
        }
    }

    fn start(self: &Arc<Self>, id: &str, tenure: &CancellationToken) {
        if tenure.is_cancelled() {
            return;
        }
        let mut q = self.queries.lock().unwrap();
        let Some(e) = q.get_mut(id) else {
            return;
        };
        if e.cancel.as_ref().is_some_and(|c| !c.is_cancelled()) || e.def.desired != DesiredState::Running {
            return;
        }
        let token = tenure.child_token();
        e.cancel = Some(token.clone());
        e.generation += 1;
        e.status = QueryStatus::Running;
        let generation = e.generation;
        let def = e.def.clone();
        let stats = Arc::new(QueryStats::default());
        e.stats = stats.clone();
        let engine = self.clone();
        e.handle = Some(tokio::spawn(async move {
            engine.supervise(def, stats, token, generation).await;
        }));
    }

    async fn supervise(self: Arc<Self>, def: QueryDef, stats: Arc<QueryStats>, token: CancellationToken, generation: u64) {
        let mut backoff = Duration::from_millis(500);
        let outcome = loop {
            let run = std::panic::AssertUnwindSafe(self.run_once(&def, stats.clone(), token.clone()))
                .catch_unwind()
                .await;
            match run {
                Ok(Ok(())) => break None,
                Ok(Err(e)) if token.is_cancelled() => {
                    info!(query = %def.id, "query stopped: {e}");
                    break None;
                }
                Ok(Err(e)) if e.is_transient() => {
                    warn!(query = %def.id, "query hit a transient error, restarting from its checkpoint: {e}");
                    tokio::select! {
                        _ = tokio::time::sleep(backoff) => {}
                        _ = token.cancelled() => break None,
                    }
                    backoff = (backoff * 2).min(Duration::from_secs(30));
                }
                Ok(Err(e)) => break Some(e.to_string()),
                Err(panic) => {
                    let msg = panic
                        .downcast_ref::<String>()
                        .cloned()
                        .or_else(|| panic.downcast_ref::<&str>().map(|s| s.to_string()))
                        .unwrap_or_else(|| "unknown panic".into());
                    break Some(format!("query panicked: {msg}"));
                }
            }
        };
        let mut q = self.queries.lock().unwrap();
        let Some(e) = q.get_mut(&def.id) else {
            return;
        };
        if e.generation != generation {
            return;
        }
        e.cancel = None;
        match outcome {
            Some(err) => {
                warn!(query = %def.id, "query failed: {err}");
                e.status = QueryStatus::Failed(err.clone());
                e.def.desired = DesiredState::Stopped;
                e.def.error = Some(err);
                let d = e.def.clone();
                drop(q);
                if let Err(pe) = self.persist(&d) {
                    warn!(query = %d.id, "could not persist failure: {pe}");
                }
            }
            None => {
                e.status = match e.def.desired {
                    DesiredState::Paused => QueryStatus::Paused,
                    DesiredState::Running => QueryStatus::Pending,
                    DesiredState::Stopped => QueryStatus::Failed(e.def.error.clone().unwrap_or_default()),
                };
            }
        }
    }

    async fn run_once(&self, def: &QueryDef, stats: Arc<QueryStats>, token: CancellationToken) -> Result<(), ExqlError> {
        let (_, df) = self.plan_def(def).await?;
        let out = stream_name(&def.name)?;
        self.log.ensure_stream(&out).await?;
        let sink = match def.kind {
            QueryKind::Stream => Sink::Stream(out),
            QueryKind::Table => {
                let table = match self.tables.get(&def.name) {
                    Some(t) if t.schema_ref() == df.out_schema => t,
                    _ => {
                        let t = Arc::new(MaterializedTable::new(&def.name, &def.id, df.out_schema.clone()));
                        self.tables.insert(t.clone());
                        t
                    }
                };
                Sink::Table { table, changelog: out }
            }
        };
        let ctx = RunCtx {
            query_id: def.id.clone(),
            log: self.log.clone(),
            cfg: self.cfg.clone(),
            stats,
            metrics: Some(self.metrics.clone()),
        };
        Runner::new(ctx, df, sink)?.run(token).await
    }

    // -- introspection ------------------------------------------------------

    fn info_of(e: &Entry) -> QueryInfo {
        QueryInfo {
            id: e.def.id.clone(),
            sql: e.def.sql.clone(),
            kind: e.def.kind,
            name: e.def.name.clone(),
            target_stream: e.def.name.clone(),
            status: e.status.as_str().to_string(),
            desired_state: e.def.desired,
            error: match &e.status {
                QueryStatus::Failed(m) => Some(m.clone()),
                _ => e.def.error.clone(),
            },
            created_at: e.def.created_at.clone(),
            stats: e.stats.to_json(),
        }
    }

    pub fn info(&self, id: &str) -> Result<QueryInfo, ExqlError> {
        self.get_query(id)
            .ok_or_else(|| ExqlError::NotFound(format!("query '{id}' not found")))
    }

    pub fn get_query(&self, id: &str) -> Option<QueryInfo> {
        self.queries.lock().unwrap().get(id).map(Self::info_of)
    }

    pub fn list_queries(&self) -> Vec<QueryInfo> {
        let mut v: Vec<QueryInfo> = self.queries.lock().unwrap().values().map(Self::info_of).collect();
        v.sort_by(|a, b| a.created_at.cmp(&b.created_at).then(a.id.cmp(&b.id)));
        v
    }

    pub fn list_tables(&self) -> Vec<TableInfo> {
        self.tables
            .list()
            .iter()
            .map(|t| TableInfo {
                name: t.name.clone(),
                query_id: t.query_id.clone(),
                columns: t.columns(),
                row_count: t.len(),
            })
            .collect()
    }

    /// All rows of a table: `{columns, rows, row_count}`.
    pub fn table_rows(&self, name: &str) -> Result<Json, ExqlError> {
        let t = self
            .tables
            .get(name)
            .ok_or_else(|| ExqlError::NotFound(format!("table '{name}' not found")))?;
        let (batch, _) = t.snapshot()?;
        let rows = batch_rows_json(&batch);
        Ok(json!({"columns": t.columns(), "rows": rows, "row_count": rows.len()}))
    }

    /// One row of a table by key: `{columns, row}`. The key is the group
    /// value (a JSON array for composite keys, empty for a global aggregate).
    pub fn table_row(&self, name: &str, key: &str) -> Result<Json, ExqlError> {
        let t = self
            .tables
            .get(name)
            .ok_or_else(|| ExqlError::NotFound(format!("table '{name}' not found")))?;
        let row = t
            .get(key)
            .ok_or_else(|| ExqlError::NotFound(format!("key '{key}' not found in table '{name}'")))?;
        let batch = rows_to_batch(&t.schema_ref(), &[&row])?;
        let mut rows = batch_rows_json(&batch);
        Ok(json!({"columns": t.columns(), "row": rows.pop().unwrap_or_default()}))
    }

    // -- connections --------------------------------------------------------

    pub fn add_connection(&self, cfg: ConnectionConfig) -> Result<(), ExqlError> {
        let name = cfg.name.clone();
        self.connection_registry.add(cfg).map_err(ExqlError::Plan)?;
        self.external.invalidate(&name);
        Ok(())
    }

    pub fn remove_connection(&self, name: &str) -> Result<(), ExqlError> {
        self.connection_registry.remove(name).map_err(|e| {
            if e.contains("not found") {
                ExqlError::NotFound(e)
            } else {
                ExqlError::Plan(e)
            }
        })?;
        self.external.invalidate(name);
        Ok(())
    }

    pub fn list_connections(&self) -> Vec<(String, String)> {
        let mut v = self.connection_registry.list();
        v.sort();
        v
    }
}
