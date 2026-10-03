//! DataFusion session setup shared by bounded and continuous queries.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::memory_pool::{GreedyMemoryPool, TrackConsumersPool};
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::execution::session_state::{SessionState, SessionStateBuilder};
use datafusion::execution::FunctionRegistry;
use datafusion::optimizer::analyzer::resolve_grouping_function::ResolveGroupingFunction;
use datafusion::optimizer::analyzer::type_coercion::TypeCoercion;
use datafusion::optimizer::AnalyzerRule;
use datafusion::physical_optimizer::optimizer::PhysicalOptimizer;
use datafusion::prelude::SessionConfig;

use crate::catalog::{ExspeedCatalogList, Resolver, CATALOG, SCHEMA};
use crate::error::ExqlError;
use crate::external::ExternalConfig;
use crate::json_rule::JsonNumericRule;
use crate::stream_table::ReverseTailRule;

/// Engine settings. [`ExqlConfig::from_env`] reads the `EXSPEED_QUERY_*` /
/// `EXSPEED_EXQL_*` environment variables.
#[derive(Debug, Clone)]
pub struct ExqlConfig {
    /// Bounded query timeout (`EXSPEED_QUERY_TIMEOUT_SECS`, default 30).
    pub query_timeout: Duration,
    /// Rows returned before a result is marked `truncated`
    /// (`EXSPEED_QUERY_MAX_ROWS`, default 10 000).
    pub max_result_rows: usize,
    /// Memory pool shared by all bounded queries
    /// (`EXSPEED_QUERY_MEMORY_MB`, default 512).
    pub memory_limit_bytes: usize,
    /// DataFusion target partitions (`EXSPEED_QUERY_PARTITIONS`, default 1).
    pub target_partitions: usize,
    pub batch_size: usize,
    /// How often continuous queries checkpoint
    /// (`EXSPEED_EXQL_CHECKPOINT_MS`, default 5000).
    pub checkpoint_interval: Duration,
    /// Also checkpoint after this many micro-batches (0 = time-based only).
    pub checkpoint_every_batches: u64,
    /// Max records read per source per micro-batch.
    pub micro_batch_records: usize,
    /// Poll interval when a source has no append notifications.
    pub poll_interval: Duration,
    /// Default allowed lateness (`GRACE PERIOD`) when a query names none
    /// (`EXSPEED_EXQL_DEFAULT_GRACE_MS`, default 0).
    pub default_grace_ms: i64,
    pub external: ExternalConfig,
}

impl Default for ExqlConfig {
    fn default() -> Self {
        Self {
            query_timeout: Duration::from_secs(30),
            max_result_rows: 10_000,
            memory_limit_bytes: 512 * 1024 * 1024,
            target_partitions: 1,
            batch_size: 8192,
            checkpoint_interval: Duration::from_secs(5),
            checkpoint_every_batches: 0,
            micro_batch_records: 1000,
            poll_interval: Duration::from_millis(200),
            default_grace_ms: 0,
            external: ExternalConfig::default(),
        }
    }
}

fn env_u64(name: &str) -> Option<u64> {
    std::env::var(name).ok().and_then(|v| v.trim().parse().ok())
}

impl ExqlConfig {
    pub fn from_env() -> Self {
        let mut c = Self::default();
        if let Some(v) = env_u64("EXSPEED_QUERY_TIMEOUT_SECS") {
            c.query_timeout = Duration::from_secs(v.max(1));
        }
        if let Some(v) = env_u64("EXSPEED_QUERY_MAX_ROWS") {
            c.max_result_rows = v.max(1) as usize;
        }
        if let Some(v) = env_u64("EXSPEED_QUERY_MEMORY_MB") {
            c.memory_limit_bytes = (v.max(16) as usize) * 1024 * 1024;
        }
        if let Some(v) = env_u64("EXSPEED_QUERY_PARTITIONS") {
            c.target_partitions = v.clamp(1, 64) as usize;
        }
        if let Some(v) = env_u64("EXSPEED_EXQL_CHECKPOINT_MS") {
            c.checkpoint_interval = Duration::from_millis(v.max(100));
        }
        if let Some(v) = env_u64("EXSPEED_EXQL_DEFAULT_GRACE_MS") {
            c.default_grace_ms = v as i64;
        }
        c
    }
}

/// The runtime (memory pool, no spilling) shared by every session.
pub fn runtime_env(cfg: &ExqlConfig) -> Result<Arc<RuntimeEnv>, ExqlError> {
    let pool = TrackConsumersPool::new(
        GreedyMemoryPool::new(cfg.memory_limit_bytes),
        NonZeroUsize::new(5).unwrap(),
    );
    Ok(RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::new(pool))
        .with_disk_manager_builder(
            DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled),
        )
        .build_arc()?)
}

fn analyzer_rules() -> Vec<Arc<dyn AnalyzerRule + Send + Sync>> {
    vec![
        Arc::new(ResolveGroupingFunction::new()),
        Arc::new(JsonNumericRule),
        Arc::new(TypeCoercion::new()),
    ]
}

fn register_functions(state: &mut SessionState) -> Result<(), ExqlError> {
    datafusion_functions_json::register_all(state)?;
    for udf in crate::udfs::all() {
        state.register_udf(Arc::new(udf))?;
    }
    Ok(())
}

/// A session with ExQL's functions and rules and DataFusion's default
/// in-memory catalog (for evaluating expressions outside the engine).
pub fn standalone_state() -> Result<SessionState, ExqlError> {
    let mut state = SessionStateBuilder::new()
        .with_config(SessionConfig::new().with_target_partitions(1))
        .with_default_features()
        .with_analyzer_rules(analyzer_rules())
        .build();
    register_functions(&mut state)?;
    Ok(state)
}

/// A session with ExQL's catalog, functions and rules.
pub fn build_state(
    cfg: &ExqlConfig,
    runtime: Arc<RuntimeEnv>,
    resolver: Resolver,
) -> Result<SessionState, ExqlError> {
    let mut config = SessionConfig::new()
        .with_default_catalog_and_schema(CATALOG, SCHEMA)
        .with_create_default_catalog_and_schema(false)
        .with_information_schema(false)
        .with_target_partitions(cfg.target_partitions)
        .with_batch_size(cfg.batch_size);
    config.options_mut().optimizer.enable_sort_pushdown = true;
    let analyzer = analyzer_rules();
    let mut physical = PhysicalOptimizer::new().rules;
    physical.push(Arc::new(ReverseTailRule));
    let mut state = SessionStateBuilder::new()
        .with_config(config)
        .with_runtime_env(runtime)
        .with_default_features()
        .with_catalog_list(Arc::new(ExspeedCatalogList::new(resolver)))
        .with_analyzer_rules(analyzer)
        .with_physical_optimizer_rules(physical)
        .build();
    register_functions(&mut state)?;
    Ok(state)
}
