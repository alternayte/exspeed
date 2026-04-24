pub mod error;
pub mod external;
pub mod materialized_view;
pub mod parser;
pub mod planner;
pub mod query_registry;
pub mod runtime;
pub mod types;

use std::path::PathBuf;
use std::sync::Arc;

use exspeed_broker::leadership::ClusterLeadership;
use exspeed_common::metrics::Metrics;
use exspeed_streams::StorageEngine;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::error::ExqlError;
use crate::external::ConnectionRegistry;
use crate::materialized_view::MaterializedViewRegistry;
use crate::parser::ast::ExqlStatement;
use crate::query_registry::{generate_query_id, QueryInfoSnapshot, QueryRegistry};
use crate::types::ResultSet;

/// Top-level ExQL engine that ties together bounded execution, continuous
/// queries, and connection/query registries.
pub struct ExqlEngine {
    pub storage: Arc<dyn StorageEngine>,
    pub connection_registry: Arc<ConnectionRegistry>,
    pub query_registry: Arc<QueryRegistry>,
    pub mv_registry: Arc<MaterializedViewRegistry>,
    /// Cluster-leader lease wrapper. Continuous query tasks are only spawned
    /// while this pod holds leadership. The leader supervisor calls
    /// `resume_all_and_run(token)` once per leadership tenure, passing a
    /// `CancellationToken` that fires on demotion or shutdown.
    pub leadership: Arc<ClusterLeadership>,
    /// Shared server metrics.
    pub metrics: Arc<Metrics>,
    /// Root data directory for persisted state.
    data_dir: PathBuf,
}

impl ExqlEngine {
    /// Create a new engine backed by the given storage and data directory.
    pub fn new(
        storage: Arc<dyn StorageEngine>,
        data_dir: PathBuf,
        leadership: Arc<ClusterLeadership>,
        metrics: Arc<Metrics>,
    ) -> Self {
        let connection_registry = Arc::new(ConnectionRegistry::new(data_dir.clone()));
        let query_registry = Arc::new(QueryRegistry::new(data_dir.clone()));
        let mv_registry = Arc::new(MaterializedViewRegistry::new());
        Self {
            storage,
            connection_registry,
            query_registry,
            mv_registry,
            leadership,
            metrics,
            data_dir,
        }
    }

    /// Load persisted state (connections + queries + secondary indexes) from disk.
    pub fn load(&self) -> Result<(), String> {
        self.connection_registry.load_all();
        self.query_registry.load_all()?;

        // Load secondary index definitions and register on partitions
        let index_dir = self.data_dir.join("indexes");
        if index_dir.is_dir() {
            if let Ok(entries) = std::fs::read_dir(&index_dir) {
                for entry in entries.flatten() {
                    if entry.path().extension().and_then(|e| e.to_str()) == Some("json") {
                        if let Ok(content) = std::fs::read_to_string(entry.path()) {
                            if let Ok(meta) = serde_json::from_str::<serde_json::Value>(&content) {
                                let name = meta["name"].as_str().unwrap_or("").to_string();
                                let stream = meta["stream"].as_str().unwrap_or("").to_string();
                                let field_path =
                                    meta["field_path"].as_str().unwrap_or("").to_string();
                                if !name.is_empty() && !stream.is_empty() {
                                    if let Ok(sn) =
                                        exspeed_common::StreamName::try_from(stream.as_str())
                                    {
                                        let storage = self.storage.clone();
                                        let _ = tokio::task::block_in_place(|| {
                                            tokio::runtime::Handle::current().block_on(
                                                storage.register_secondary_index(
                                                    &sn,
                                                    name.clone(),
                                                    field_path.clone(),
                                                ),
                                            )
                                        });
                                        info!(
                                            index = %name,
                                            stream = %stream,
                                            field = %field_path,
                                            "loaded secondary index"
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Return the data directory path.
    pub fn data_dir(&self) -> &std::path::Path {
        &self.data_dir
    }

    /// Create a secondary index from a `CREATE INDEX` statement.
    ///
    /// Parses `sql`, persists index metadata as JSON under
    /// `{data_dir}/indexes/{name}.json`, and returns the index name.
    pub async fn create_index(&self, sql: &str) -> Result<String, ExqlError> {
        let stmt = crate::parser::parse(sql)?;
        match stmt {
            crate::parser::ExqlStatement::CreateIndex {
                name,
                stream,
                field_path,
            } => {
                let index_dir = self.data_dir().join("indexes");
                std::fs::create_dir_all(&index_dir)
                    .map_err(|e| ExqlError::Storage(e.to_string()))?;
                let meta = serde_json::json!({
                    "name": name,
                    "stream": stream,
                    "field_path": field_path,
                });
                let path = index_dir.join(format!("{name}.json"));
                std::fs::write(
                    &path,
                    serde_json::to_string_pretty(&meta).unwrap(),
                )
                .map_err(|e| ExqlError::Storage(e.to_string()))?;

                // Register on running partitions immediately
                if let Ok(sn) = exspeed_common::StreamName::try_from(stream.as_str()) {
                    self.storage
                        .register_secondary_index(&sn, name.clone(), field_path.clone())
                        .await
                        .map_err(|e| {
                            ExqlError::Storage(format!("failed to register index: {e}"))
                        })?;
                }

                Ok(name)
            }
            _ => Err(ExqlError::Execution(
                "expected CREATE INDEX statement".into(),
            )),
        }
    }

    /// Drop a secondary index from a `DROP INDEX` statement.
    ///
    /// Parses `sql`, removes the index metadata JSON file if it exists, and
    /// returns the index name.
    pub async fn drop_index(&self, sql: &str) -> Result<String, ExqlError> {
        let stmt = crate::parser::parse(sql)?;
        match stmt {
            crate::parser::ExqlStatement::DropIndex(name) => {
                let path = self
                    .data_dir()
                    .join("indexes")
                    .join(format!("{name}.json"));
                if path.exists() {
                    std::fs::remove_file(&path)
                        .map_err(|e| ExqlError::Storage(e.to_string()))?;
                }
                Ok(name)
            }
            _ => Err(ExqlError::Execution(
                "expected DROP INDEX statement".into(),
            )),
        }
    }

    /// Execute a bounded (batch) SQL query.
    pub async fn execute_bounded(&self, sql: &str) -> Result<ResultSet, ExqlError> {
        let indexes = self.load_index_defs();
        runtime::bounded::execute_bounded_with_mv(
            sql,
            &self.storage,
            Some(&self.connection_registry),
            Some(&self.mv_registry),
            &indexes,
        )
        .await
    }

    /// Load secondary index definitions from `{data_dir}/indexes/*.json`.
    fn load_index_defs(&self) -> Vec<runtime::bounded::IndexDef> {
        let mut defs = Vec::new();
        let index_dir = self.data_dir.join("indexes");
        if let Ok(entries) = std::fs::read_dir(&index_dir) {
            for entry in entries.flatten() {
                if entry.path().extension().and_then(|e| e.to_str()) == Some("json") {
                    if let Ok(content) = std::fs::read_to_string(entry.path()) {
                        if let Ok(meta) = serde_json::from_str::<serde_json::Value>(&content) {
                            defs.push(runtime::bounded::IndexDef {
                                name: meta["name"].as_str().unwrap_or("").to_string(),
                                stream: meta["stream"].as_str().unwrap_or("").to_string(),
                                field_path: meta["field_path"]
                                    .as_str()
                                    .unwrap_or("")
                                    .to_string(),
                            });
                        }
                    }
                }
            }
        }
        defs
    }

    /// Resume every persisted continuous query under `token`. Called once
    /// per leadership tenure by the leader supervisor. Returns when
    /// `token.cancelled()` fires (demotion or shutdown).
    pub async fn resume_all_and_run(
        self: Arc<Self>,
        token: CancellationToken,
    ) {
        let queries = self.query_registry.list();
        for query in queries {
            let engine = self.clone();
            let child = token.child_token();
            tokio::spawn(async move {
                engine.spawn_query_with_token(query, child).await;
            });
        }
        token.cancelled().await;
        info!("ExqlEngine leader tenure ended");
    }

    /// Spawn a single continuous query task governed by `token`. Exits when
    /// the query finishes naturally or when `token` is cancelled.
    async fn spawn_query_with_token(
        self: Arc<Self>,
        query: QueryInfoSnapshot,
        token: CancellationToken,
    ) {
        let query_id = query.id.clone();
        let Some(sql) = self.query_registry.get_sql(&query_id) else {
            return;
        };

        let (cancel_tx, cancel_rx) = oneshot::channel::<()>();
        if self
            .query_registry
            .set_running(&query_id, cancel_tx)
            .is_err()
        {
            // Another concurrent path (e.g., the API) already claimed Running.
            return;
        }

        let storage = self.storage.clone();
        let registry = self.query_registry.clone();
        let target = query.target_stream.clone();

        let work = runtime::continuous::run_continuous_query(
            query_id.clone(),
            sql,
            target,
            storage,
            registry,
            cancel_rx,
            None,
        );

        tokio::select! {
            _ = work => {}
            _ = token.cancelled() => {
                info!(query_id = %query_id, "query cancelled (leader token fired)");
            }
        }
    }

    /// Create and start a continuous query from a CREATE VIEW statement.
    ///
    /// Returns the generated query ID. The query is always registered in the
    /// local query registry. On the leader pod the runtime task is spawned
    /// immediately under the current leader token. On a standby pod this call
    /// returns `Err` — the `leader_gate` middleware should have already
    /// returned 503 before this point, but we check here as defence-in-depth.
    pub async fn create_continuous(self: &Arc<Self>, sql: &str) -> Result<String, ExqlError> {
        // Parse to validate and extract target stream name
        let stmt = crate::parser::parse(sql)?;
        let target_stream = match &stmt {
            ExqlStatement::CreateStream { name, .. } => name.clone(),
            _ => {
                return Err(ExqlError::Execution(
                    "create_continuous requires a CREATE VIEW statement".into(),
                ));
            }
        };

        let id = generate_query_id();

        self.query_registry
            .register(&id, sql, &target_stream)
            .map_err(ExqlError::Execution)?;

        // Defence-in-depth: leader_gate should have caught this first.
        if !self.leadership.is_currently_leader() {
            return Err(ExqlError::Execution(
                "not leader; continuous queries can only be created on the leader".into(),
            ));
        }

        let child = self.leadership.current_child_token().await;
        let engine = self.clone();

        let query = QueryInfoSnapshot {
            id: id.clone(),
            sql: sql.to_string(),
            target_stream: target_stream.clone(),
            status: "stopped".to_string(),
        };

        tokio::spawn(async move {
            engine.spawn_query_with_token(query, child).await;
        });

        Ok(id)
    }

    /// Create and start a materialized view from a CREATE MATERIALIZED VIEW
    /// statement.
    ///
    /// The continuous executor writes output rows into the MV state HashMap
    /// rather than an output stream. Returns the generated query ID.
    ///
    /// Leadership semantics match [`Self::create_continuous`]: only the leader
    /// pod runs the writer task.
    pub async fn create_materialized_view(
        self: &Arc<Self>,
        sql: &str,
    ) -> Result<String, ExqlError> {
        let stmt = crate::parser::parse(sql)?;
        let (name, query) = match stmt {
            ExqlStatement::CreateMaterializedView { name, query } => (name, query),
            _ => {
                return Err(ExqlError::Execution(
                    "expected CREATE MATERIALIZED VIEW".into(),
                ));
            }
        };

        // Determine columns from SELECT items
        let columns: Vec<String> = query
            .select
            .iter()
            .map(|item| {
                item.alias.clone().unwrap_or_else(|| match &item.expr {
                    crate::parser::ast::Expr::Column { name, .. } => name.clone(),
                    crate::parser::ast::Expr::Aggregate { func, .. } => match func {
                        crate::parser::ast::AggregateFunc::Count => "count".into(),
                        crate::parser::ast::AggregateFunc::Sum => "sum".into(),
                        crate::parser::ast::AggregateFunc::Avg => "avg".into(),
                        crate::parser::ast::AggregateFunc::Min => "min".into(),
                        crate::parser::ast::AggregateFunc::Max => "max".into(),
                    },
                    _ => "?".into(),
                })
            })
            .collect();

        let id = generate_query_id();
        let mv_state = self.mv_registry.register(&name, columns, &id);

        self.query_registry
            .register(&id, sql, &name)
            .map_err(ExqlError::Execution)?;

        // Defence-in-depth: leader_gate should have caught this first.
        if !self.leadership.is_currently_leader() {
            return Err(ExqlError::Execution(
                "not leader; materialized views can only be created on the leader".into(),
            ));
        }

        let child = self.leadership.current_child_token().await;
        let engine = self.clone();
        let query_id = id.clone();
        let sql_owned = sql.to_string();
        let mv_name = name.clone();
        let storage = self.storage.clone();
        let registry = self.query_registry.clone();

        tokio::spawn(async move {
            let (cancel_tx, cancel_rx) = oneshot::channel::<()>();
            if engine
                .query_registry
                .set_running(&query_id, cancel_tx)
                .is_err()
            {
                return;
            }

            let work = runtime::continuous::run_continuous_query(
                query_id.clone(),
                sql_owned,
                mv_name,
                storage,
                registry,
                cancel_rx,
                Some(mv_state),
            );

            tokio::select! {
                _ = work => {}
                _ = child.cancelled() => {
                    info!(query_id = %query_id, "materialized view cancelled (leader token fired)");
                }
            }
        });

        Ok(id)
    }

    /// Stop a running continuous query.
    pub fn stop_query(&self, id: &str) -> Result<(), ExqlError> {
        self.query_registry.stop(id).map_err(ExqlError::Execution)
    }

    /// Remove a continuous query (stop if running, delete from disk).
    pub fn remove_query(&self, id: &str) -> Result<(), ExqlError> {
        self.query_registry.remove(id).map_err(ExqlError::Execution)
    }

    /// List all registered continuous queries.
    pub fn list_queries(&self) -> Vec<QueryInfoSnapshot> {
        self.query_registry.list()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use exspeed_broker::lease::NoopLeaderLease;
    use exspeed_common::StreamName;
    use exspeed_storage::memory::MemoryStorage;

    async fn make_leadership_and_engine(
        storage: Arc<dyn StorageEngine>,
        data_dir: std::path::PathBuf,
    ) -> (Arc<ClusterLeadership>, Arc<ExqlEngine>) {
        let lease: Arc<dyn exspeed_broker::LeaderLease> =
            Arc::new(NoopLeaderLease::new());
        let (metrics, _r) = exspeed_common::Metrics::new();
        let metrics = Arc::new(metrics);
        let leadership = Arc::new(
            ClusterLeadership::spawn(lease, metrics.clone(), None).await,
        );
        // Wait for Noop to grant leadership.
        for _ in 0..40 {
            if leadership.is_currently_leader() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        let engine = Arc::new(ExqlEngine::new(
            storage,
            data_dir,
            leadership.clone(),
            metrics,
        ));
        (leadership, engine)
    }

    /// `create_continuous` must spawn a running task when this pod is the leader.
    #[tokio::test]
    async fn create_continuous_spawns_on_leader() {
        let storage: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
        // Source stream the CREATE VIEW reads from.
        storage
            .create_stream(&StreamName::try_from("src").unwrap(), 0, 0)
            .await
            .unwrap();

        let dir = tempfile::tempdir().unwrap();
        let (_leadership, engine) =
            make_leadership_and_engine(storage, dir.path().to_path_buf())
                .await;

        let id = engine
            .create_continuous(r#"CREATE VIEW derived AS SELECT * FROM "src""#)
            .await
            .expect("create_continuous ok");

        // The query should be registered in the registry.
        let queries = engine.list_queries();
        assert!(
            queries.iter().any(|q| q.id == id),
            "expected query {id} in registry; got {:?}",
            queries.iter().map(|q| &q.id).collect::<Vec<_>>()
        );
    }

    /// Same contract for `create_materialized_view`.
    #[tokio::test]
    async fn create_materialized_view_spawns_on_leader() {
        let storage: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
        storage
            .create_stream(&StreamName::try_from("src").unwrap(), 0, 0)
            .await
            .unwrap();

        let dir = tempfile::tempdir().unwrap();
        let (_leadership, engine) =
            make_leadership_and_engine(storage, dir.path().to_path_buf())
                .await;

        let id = engine
            .create_materialized_view(
                r#"CREATE MATERIALIZED VIEW mv AS SELECT * FROM "src""#,
            )
            .await
            .expect("create_materialized_view ok");

        let queries = engine.list_queries();
        assert!(
            queries.iter().any(|q| q.id == id),
            "expected query {id} in registry; got {:?}",
            queries.iter().map(|q| &q.id).collect::<Vec<_>>()
        );
    }
}
