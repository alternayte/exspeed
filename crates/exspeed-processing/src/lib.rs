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

use exspeed_streams::StorageEngine;
use tokio::sync::oneshot;

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
}

impl ExqlEngine {
    /// Create a new engine backed by the given storage and data directory.
    pub fn new(storage: Arc<dyn StorageEngine>, data_dir: PathBuf) -> Self {
        let connection_registry = Arc::new(ConnectionRegistry::new(data_dir.clone()));
        let query_registry = Arc::new(QueryRegistry::new(data_dir));
        let mv_registry = Arc::new(MaterializedViewRegistry::new());
        Self {
            storage,
            connection_registry,
            query_registry,
            mv_registry,
        }
    }

    /// Load persisted state (connections + queries) from disk.
    pub fn load(&self) -> Result<(), String> {
        self.connection_registry.load_all();
        self.query_registry.load_all()?;
        Ok(())
    }

    /// Execute a bounded (batch) SQL query.
    pub fn execute_bounded(&self, sql: &str) -> Result<ResultSet, ExqlError> {
        runtime::bounded::execute_bounded_with_mv(
            sql,
            &self.storage,
            Some(&self.connection_registry),
            Some(&self.mv_registry),
        )
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
    /// iterates through them and re-spawns any that the user had previously
    /// created (all loaded queries are candidates for resumption).
    ///
    /// Note: MV resume is deferred — MVs need to be re-registered separately.
    pub fn resume_all(&self) {
        let snapshots = self.query_registry.list();
        for snap in snapshots {
            // Retrieve the SQL so we can re-launch
            if let Some(sql) = self.query_registry.get_sql(&snap.id) {
                let (cancel_tx, cancel_rx) = oneshot::channel::<()>();
                if self.query_registry.set_running(&snap.id, cancel_tx).is_ok() {
                    let storage = self.storage.clone();
                    let registry = self.query_registry.clone();
                    let query_id = snap.id.clone();
                    let target = snap.target_stream.clone();

                    tokio::spawn(async move {
                        runtime::continuous::run_continuous_query(
                            query_id, sql, target, storage, registry, cancel_rx, None,
                        )
                        .await;
                    });
                }
            }
        }
    }
}
