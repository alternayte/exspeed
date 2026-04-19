pub mod error;
pub mod external;
pub mod materialized_view;
pub mod parser;
pub mod planner;
pub mod query_registry;
pub mod runtime;
pub mod types;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use exspeed_broker::lease::LeaderLease;
use exspeed_streams::StorageEngine;
use tokio::sync::oneshot;
use tokio::sync::RwLock as AsyncRwLock;

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
    /// Cluster-wide lease backend. Continuous query tasks started via
    /// `resume_all` only spawn after acquiring `query:<id>`. Noop always
    /// grants (single-pod). Interactive create_continuous / create_mv calls
    /// still spawn directly (they're user-initiated on a specific pod).
    pub lease: Arc<dyn LeaderLease>,
    /// Query IDs for which this pod currently holds the lease and has an
    /// active task. Used to avoid re-spawning and by the LeaseRetrier to
    /// skip already-running entries. The value is unit — the actual
    /// `LeaseGuard` lives inside each spawned task.
    pub running_query_leases: Arc<AsyncRwLock<HashMap<String, ()>>>,
}

impl ExqlEngine {
    /// Create a new engine backed by the given storage and data directory.
    pub fn new(
        storage: Arc<dyn StorageEngine>,
        data_dir: PathBuf,
        lease: Arc<dyn LeaderLease>,
    ) -> Self {
        let connection_registry = Arc::new(ConnectionRegistry::new(data_dir.clone()));
        let query_registry = Arc::new(QueryRegistry::new(data_dir));
        let mv_registry = Arc::new(MaterializedViewRegistry::new());
        Self {
            storage,
            connection_registry,
            query_registry,
            mv_registry,
            lease,
            running_query_leases: Arc::new(AsyncRwLock::new(HashMap::new())),
        }
    }

    /// Load persisted state (connections + queries) from disk.
    pub fn load(&self) -> Result<(), String> {
        self.connection_registry.load_all();
        self.query_registry.load_all()?;
        Ok(())
    }

    /// Execute a bounded (batch) SQL query.
    pub async fn execute_bounded(&self, sql: &str) -> Result<ResultSet, ExqlError> {
        runtime::bounded::execute_bounded_with_mv(
            sql,
            &self.storage,
            Some(&self.connection_registry),
            Some(&self.mv_registry),
        )
        .await
    }

    /// Create and start a continuous query from a CREATE VIEW statement.
    ///
    /// Returns the generated query ID.
    pub fn create_continuous(&self, sql: &str) -> Result<String, ExqlError> {
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

        let (cancel_tx, cancel_rx) = oneshot::channel::<()>();
        self.query_registry
            .set_running(&id, cancel_tx)
            .map_err(ExqlError::Execution)?;

        // Spawn the continuous query task
        let storage = self.storage.clone();
        let registry = self.query_registry.clone();
        let query_id = id.clone();
        let sql_owned = sql.to_string();
        let target = target_stream.clone();

        tokio::spawn(async move {
            runtime::continuous::run_continuous_query(
                query_id, sql_owned, target, storage, registry, cancel_rx, None,
            )
            .await;
        });

        Ok(id)
    }

    /// Create and start a materialized view from a CREATE MATERIALIZED VIEW
    /// statement.
    ///
    /// The continuous executor writes output rows into the MV state HashMap
    /// rather than an output stream. Returns the generated query ID.
    pub fn create_materialized_view(&self, sql: &str) -> Result<String, ExqlError> {
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

        let (cancel_tx, cancel_rx) = oneshot::channel::<()>();
        self.query_registry
            .set_running(&id, cancel_tx)
            .map_err(ExqlError::Execution)?;

        let storage = self.storage.clone();
        let registry = self.query_registry.clone();
        let query_id = id.clone();
        let sql_owned = sql.to_string();
        let mv_name = name.clone();

        tokio::spawn(async move {
            runtime::continuous::run_continuous_query(
                query_id,
                sql_owned,
                mv_name,
                storage,
                registry,
                cancel_rx,
                Some(mv_state),
            )
            .await;
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

    /// Resume all continuous queries that were previously running.
    ///
    /// On startup, all queries are loaded with status `Stopped`. This method
    /// spawns a task that calls [`Self::attempt_acquire_unheld_queries`],
    /// which lease-gates each query (one holder per query cluster-wide).
    /// Under the Noop backend this behaves exactly like the pre-lease code
    /// path (Noop always grants); on Pg/Redis, non-holders remain in standby
    /// and the [`exspeed_broker::LeaseRetrier`] retries on its tick.
    ///
    /// Note: MV resume is deferred — MVs need to be re-registered separately.
    pub fn resume_all(self: &Arc<Self>) {
        let engine = self.clone();
        tokio::spawn(async move {
            engine.attempt_acquire_unheld_queries().await;
        });
    }

    /// Attempt to acquire the lease for every registered query we're not
    /// already running, and spawn the continuous task for each one we take.
    ///
    /// Called once from `resume_all` at startup and then repeatedly by the
    /// [`exspeed_broker::LeaseRetrier`] on every tick. Idempotent: an entry
    /// in `running_query_leases` short-circuits the acquire.
    pub async fn attempt_acquire_unheld_queries(&self) {
        let snapshots = self.query_registry.list();
        for snap in snapshots {
            let query_id = snap.id.clone();
            let Some(sql) = self.query_registry.get_sql(&query_id) else {
                continue;
            };

            let lease_name = format!("query:{query_id}");
            let ttl = exspeed_broker::lease::ttl_from_env();

            // Race-safe check+acquire: hold the write lock across
            // try_acquire so concurrent calls (e.g., retrier racing
            // resume_all) can't both attempt acquire and end up with one
            // taking the lease while the other re-enters. Mirrors the
            // pattern in exspeed-connectors::manager::start_source.
            let guard = {
                let mut held = self.running_query_leases.write().await;
                if held.contains_key(&query_id) {
                    continue;
                }
                match self.lease.try_acquire(&lease_name, ttl).await {
                    Ok(Some(g)) => {
                        held.insert(query_id.clone(), ());
                        g
                    }
                    Ok(None) => {
                        tracing::debug!(
                            query_id = %query_id,
                            "another pod holds lease; standby"
                        );
                        continue;
                    }
                    Err(e) => {
                        tracing::error!(
                            query_id = %query_id,
                            error = %e,
                            "lease backend error"
                        );
                        continue;
                    }
                }
            };

            // Reserve the Running slot in the query registry (and get a
            // cancel channel for manual `stop_query`). If this pod can't
            // claim Running (e.g., another task already marked it), give
            // the lease up and move on.
            let (cancel_tx, cancel_rx) = oneshot::channel::<()>();
            if self
                .query_registry
                .set_running(&query_id, cancel_tx)
                .is_err()
            {
                self.running_query_leases.write().await.remove(&query_id);
                drop(guard);
                continue;
            }

            let mut on_lost = guard.on_lost.clone();
            let storage = self.storage.clone();
            let registry = self.query_registry.clone();
            let target = snap.target_stream.clone();
            let qid_task = query_id.clone();
            let running = self.running_query_leases.clone();

            tokio::spawn(async move {
                // Keep the lease guard alive for the lifetime of this task.
                // When the task exits (normal, error, or cancellation), the
                // guard drops and the heartbeat stops — releasing the lease
                // so another pod can take over via the LeaseRetrier.
                let _guard = guard;

                let work = runtime::continuous::run_continuous_query(
                    qid_task.clone(),
                    sql,
                    target,
                    storage,
                    registry,
                    cancel_rx,
                    None,
                );

                tokio::select! {
                    _ = work => {}
                    _ = on_lost.changed() => {
                        tracing::warn!(
                            query_id = %qid_task,
                            "lease lost; stopping continuous query"
                        );
                    }
                }

                // Clear the running_query_leases entry so the retrier can
                // pick us up again if the lease is lost or the task
                // exited for any reason.
                running.write().await.remove(&qid_task);
            });
        }
    }
}

#[async_trait::async_trait]
impl exspeed_broker::LeaseRetrierTarget for ExqlEngine {
    async fn attempt_acquire_unheld(&self) {
        self.attempt_acquire_unheld_queries().await;
    }
    fn name(&self) -> &'static str {
        "queries"
    }
}
