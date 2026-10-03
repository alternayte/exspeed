//! External Postgres tables, reachable only through registered connections.
//!
//! A table reference `conn.table` (schema `public`) or `conn.schema.table`
//! resolves to an [`ExternalTable`] provider:
//!
//! - the column list comes from `information_schema.columns` (parameterized
//!   query, cached per table with the snapshot TTL);
//! - each scan fetches only the projected columns, with simple filters
//!   (`col = literal`, numeric `<`/`>`, `IN`, `IS [NOT] NULL`) pushed into
//!   the remote `WHERE` as bind parameters. Identifiers are quoted
//!   (`"schema"."table"`, embedded quotes doubled), never interpolated raw.
//!   DataFusion re-applies every filter, so pushdown only ever narrows the
//!   fetch;
//! - fetched snapshots are cached per (table, columns, filters) in a small
//!   LRU with a TTL, pools are reused per connection, and a scan reserves
//!   its snapshot's size from the query memory pool
//!   (`RESOURCES_EXHAUSTED` when it doesn't fit);
//! - `numeric(p, s)` with `p <= 38` maps to `Decimal128(p, s)`;
//!   unconstrained `numeric` maps to `Float64`.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use datafusion::arrow::array::{
    ArrayRef, BooleanBuilder, Date32Builder, Float64Builder, Int64Builder, RecordBatch,
    RecordBatchOptions, StringBuilder, TimestampMillisecondBuilder,
};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{Result as DFResult, ScalarValue};
use datafusion::datasource::{MemTable, TableType};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{BinaryExpr, Expr, Operator, TableProviderFilterPushDown};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use sqlx::postgres::{PgArguments, PgPool, PgPoolOptions, PgRow};
use sqlx::{Arguments, Row};

use crate::convert::{ts_type, JSON_META};
use crate::error::ExqlError;
use crate::external::connections::{ConnectionConfig, ConnectionRegistry};

/// Quote an identifier for Postgres.
pub fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ColKind {
    Int,
    Float,
    Decimal(u8, i8),
    Bool,
    Text,
    Json,
    Timestamp,
    Date,
}

impl ColKind {
    fn from_pg(data_type: &str, precision: Option<i32>, scale: Option<i32>) -> Self {
        match data_type {
            "smallint" | "integer" | "bigint" => ColKind::Int,
            "numeric" => match (precision, scale) {
                (Some(p), Some(s)) if (1..=38).contains(&p) && (0..=p).contains(&s) => {
                    ColKind::Decimal(p as u8, s as i8)
                }
                _ => ColKind::Float,
            },
            "real" | "double precision" => ColKind::Float,
            "boolean" => ColKind::Bool,
            "json" | "jsonb" => ColKind::Json,
            "timestamp without time zone" | "timestamp with time zone" => ColKind::Timestamp,
            "date" => ColKind::Date,
            _ => ColKind::Text,
        }
    }

    fn arrow_field(self, name: &str) -> Field {
        match self {
            ColKind::Int => Field::new(name, DataType::Int64, true),
            ColKind::Float => Field::new(name, DataType::Float64, true),
            ColKind::Decimal(p, s) => Field::new(name, DataType::Decimal128(p, s), true),
            ColKind::Bool => Field::new(name, DataType::Boolean, true),
            ColKind::Text => Field::new(name, DataType::Utf8, true),
            ColKind::Json => Field::new(name, DataType::Utf8, true)
                .with_metadata(HashMap::from([(JSON_META.to_string(), "true".to_string())])),
            ColKind::Timestamp => Field::new(name, ts_type(), true),
            ColKind::Date => Field::new(name, DataType::Date32, true),
        }
    }

    /// The select-list expression that yields a value sqlx can decode.
    fn select_expr(self, name: &str) -> String {
        let q = quote_ident(name);
        match self {
            ColKind::Int => format!("{q}::int8"),
            ColKind::Float => format!("{q}::float8"),
            ColKind::Bool => q,
            ColKind::Decimal(..) | ColKind::Text | ColKind::Json => format!("{q}::text"),
            ColKind::Timestamp => format!("(extract(epoch from {q}) * 1000)::int8"),
            ColKind::Date => format!("({q} - date '1970-01-01')::int4"),
        }
    }
}

#[derive(Debug, Clone)]
struct Column {
    name: String,
    kind: ColKind,
}

struct Cached<T> {
    loaded: Instant,
    last_used: Instant,
    value: T,
}

/// A small TTL + LRU cache.
struct Lru<K, V> {
    ttl: Duration,
    cap: usize,
    map: Mutex<HashMap<K, Cached<V>>>,
}

impl<K: std::hash::Hash + Eq + Clone, V: Clone> Lru<K, V> {
    fn new(ttl: Duration, cap: usize) -> Self {
        Self {
            ttl,
            cap: cap.max(1),
            map: Mutex::new(HashMap::new()),
        }
    }

    fn get(&self, k: &K) -> Option<V> {
        let mut m = self.map.lock().unwrap();
        match m.get_mut(k) {
            Some(c) if c.loaded.elapsed() < self.ttl => {
                c.last_used = Instant::now();
                Some(c.value.clone())
            }
            Some(_) => {
                m.remove(k);
                None
            }
            None => None,
        }
    }

    fn put(&self, k: K, v: V) {
        let mut m = self.map.lock().unwrap();
        if m.len() >= self.cap && !m.contains_key(&k) {
            if let Some(oldest) = m
                .iter()
                .min_by_key(|(_, c)| c.last_used)
                .map(|(k, _)| k.clone())
            {
                m.remove(&oldest);
            }
        }
        let now = Instant::now();
        m.insert(
            k,
            Cached {
                loaded: now,
                last_used: now,
                value: v,
            },
        );
    }

    fn retain(&self, f: impl Fn(&K) -> bool) {
        self.map.lock().unwrap().retain(|k, _| f(k));
    }
}

/// Settings for external tables.
#[derive(Debug, Clone)]
pub struct ExternalConfig {
    pub cache_ttl: Duration,
    pub cache_entries: usize,
    pub max_rows: usize,
    pub fetch_timeout: Duration,
}

impl Default for ExternalConfig {
    fn default() -> Self {
        Self {
            cache_ttl: Duration::from_secs(30),
            cache_entries: 64,
            max_rows: 1_000_000,
            fetch_timeout: Duration::from_secs(30),
        }
    }
}

type TableKey = (String, String, String);
/// (connection, rendered SQL with its bind values)
type SnapshotKey = (String, String);

/// Resolves external tables; shared cache of column lists and snapshots.
pub struct ExternalTables {
    registry: Arc<ConnectionRegistry>,
    config: ExternalConfig,
    columns: Lru<TableKey, Arc<Vec<Column>>>,
    snapshots: Arc<Lru<SnapshotKey, RecordBatch>>,
    pools: Mutex<HashMap<String, (String, PgPool)>>,
}

impl ExternalTables {
    pub fn new(registry: Arc<ConnectionRegistry>, config: ExternalConfig) -> Self {
        Self {
            registry,
            columns: Lru::new(config.cache_ttl, config.cache_entries),
            snapshots: Arc::new(Lru::new(config.cache_ttl, config.cache_entries)),
            config,
            pools: Mutex::new(HashMap::new()),
        }
    }

    pub fn registry(&self) -> &Arc<ConnectionRegistry> {
        &self.registry
    }

    /// Whether `name` is a registered connection.
    pub fn has_connection(&self, name: &str) -> bool {
        self.registry.get(name).is_some()
    }

    /// Forget cached columns, snapshots and pools of a connection.
    pub fn invalidate(&self, conn: &str) {
        self.columns.retain(|k| k.0 != conn);
        self.snapshots.retain(|k| k.0 != conn);
        self.pools.lock().unwrap().remove(conn);
    }

    fn pool(&self, cfg: &ConnectionConfig) -> Result<PgPool, ExqlError> {
        let mut pools = self.pools.lock().unwrap();
        if let Some((url, pool)) = pools.get(&cfg.name) {
            if *url == cfg.url {
                return Ok(pool.clone());
            }
        }
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .acquire_timeout(Duration::from_secs(5))
            .connect_lazy(&cfg.url)
            .map_err(|e| ExqlError::Execution(format!("connection '{}': {e}", cfg.name)))?;
        pools.insert(cfg.name.clone(), (cfg.url.clone(), pool.clone()));
        Ok(pool)
    }

    /// Resolve `conn.schema.table` to a table provider, or `None` if the
    /// connection or table doesn't exist.
    pub async fn table(
        &self,
        conn: &str,
        schema: &str,
        table: &str,
    ) -> Result<Option<Arc<ExternalTable>>, ExqlError> {
        let Some(cfg) = self.registry.get(conn) else {
            return Ok(None);
        };
        if !matches!(cfg.driver.as_str(), "postgres" | "postgresql") {
            return Err(ExqlError::unsupported(
                format!("driver '{}'", cfg.driver),
                "external tables support the postgres driver",
            ));
        }
        let pool = self.pool(&cfg)?;
        let key = (conn.to_string(), schema.to_string(), table.to_string());
        let cols = match self.columns.get(&key) {
            Some(c) => c,
            None => {
                let fut = fetch_columns(&pool, schema, table);
                let cols = self.timed(conn, schema, table, fut).await?;
                if cols.is_empty() {
                    return Ok(None);
                }
                let cols = Arc::new(cols);
                self.columns.put(key, cols.clone());
                cols
            }
        };
        let arrow_schema = Arc::new(Schema::new(
            cols.iter()
                .map(|c| c.kind.arrow_field(&c.name))
                .collect::<Vec<_>>(),
        ));
        Ok(Some(Arc::new(ExternalTable {
            conn: conn.to_string(),
            schema_name: schema.to_string(),
            table: table.to_string(),
            cols,
            arrow_schema,
            pool,
            config: self.config.clone(),
            snapshots: self.snapshots.clone(),
        })))
    }

    async fn timed<T>(
        &self,
        conn: &str,
        schema: &str,
        table: &str,
        fut: impl std::future::Future<Output = Result<T, ExqlError>>,
    ) -> Result<T, ExqlError> {
        timed(&self.config, conn, schema, table, fut).await
    }
}

async fn timed<T>(
    config: &ExternalConfig,
    conn: &str,
    schema: &str,
    table: &str,
    fut: impl std::future::Future<Output = Result<T, ExqlError>>,
) -> Result<T, ExqlError> {
    tokio::time::timeout(config.fetch_timeout, fut)
        .await
        .map_err(|_| {
            ExqlError::Execution(format!(
                "fetching {conn}.{schema}.{table} timed out after {:?}",
                config.fetch_timeout
            ))
        })?
}

fn ext_err(e: sqlx::Error) -> ExqlError {
    ExqlError::Execution(format!("external query failed: {e}"))
}

async fn fetch_columns(pool: &PgPool, schema: &str, table: &str) -> Result<Vec<Column>, ExqlError> {
    sqlx::query(
        "SELECT column_name::text, data_type::text, numeric_precision::int4, numeric_scale::int4 \
         FROM information_schema.columns \
         WHERE table_schema = $1 AND table_name = $2 ORDER BY ordinal_position",
    )
    .bind(schema)
    .bind(table)
    .fetch_all(pool)
    .await
    .map_err(ext_err)?
    .into_iter()
    .map(|r: PgRow| {
        let name: String = r.try_get(0)?;
        let ty: String = r.try_get(1)?;
        let p: Option<i32> = r.try_get(2)?;
        let s: Option<i32> = r.try_get(3)?;
        Ok(Column {
            name,
            kind: ColKind::from_pg(&ty, p, s),
        })
    })
    .collect::<Result<_, sqlx::Error>>()
    .map_err(ext_err)
}

/// One external table; every scan fetches what it needs (or reuses a
/// cached fetch).
pub struct ExternalTable {
    conn: String,
    schema_name: String,
    table: String,
    cols: Arc<Vec<Column>>,
    arrow_schema: SchemaRef,
    pool: PgPool,
    config: ExternalConfig,
    snapshots: Arc<Lru<SnapshotKey, RecordBatch>>,
}

impl fmt::Debug for ExternalTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ExternalTable({}.{}.{})", self.conn, self.schema_name, self.table)
    }
}

/// A bind value for a pushed-down filter.
#[derive(Debug, Clone)]
enum Bind {
    Int(i64),
    Float(f64),
    Bool(bool),
    Text(String),
}

impl fmt::Display for Bind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Bind::Int(v) => write!(f, "i{v}"),
            Bind::Float(v) => write!(f, "f{v:?}"),
            Bind::Bool(v) => write!(f, "b{v}"),
            Bind::Text(v) => write!(f, "t{v:?}"),
        }
    }
}

/// Translates DataFusion filters into a remote `WHERE` clause.
struct Where<'a> {
    cols: &'a [Column],
    sql: Vec<String>,
    binds: Vec<Bind>,
}

impl<'a> Where<'a> {
    fn new(cols: &'a [Column]) -> Self {
        Self {
            cols,
            sql: vec![],
            binds: vec![],
        }
    }

    fn column(&self, e: &Expr) -> Option<&'a Column> {
        match e {
            Expr::Column(c) => self.cols.iter().find(|x| x.name == c.name),
            _ => None,
        }
    }

    /// A literal compatible with `kind`, for an exact comparison.
    fn bind(kind: ColKind, e: &Expr) -> Option<Bind> {
        let Expr::Literal(v, _) = e else { return None };
        match (kind, v) {
            (ColKind::Int, ScalarValue::Int64(Some(x))) => Some(Bind::Int(*x)),
            (ColKind::Int, ScalarValue::Int32(Some(x))) => Some(Bind::Int(*x as i64)),
            (ColKind::Int, ScalarValue::Int16(Some(x))) => Some(Bind::Int(*x as i64)),
            (ColKind::Int, ScalarValue::Int8(Some(x))) => Some(Bind::Int(*x as i64)),
            (ColKind::Float, ScalarValue::Float64(Some(x))) => Some(Bind::Float(*x)),
            (ColKind::Bool, ScalarValue::Boolean(Some(x))) => Some(Bind::Bool(*x)),
            (ColKind::Text, ScalarValue::Utf8(Some(x)) | ScalarValue::Utf8View(Some(x))) => {
                Some(Bind::Text(x.clone()))
            }
            _ => None,
        }
    }

    fn placeholder(&mut self, b: Bind) -> String {
        self.binds.push(b);
        format!("${}", self.binds.len())
    }

    /// The remote condition for `e`, or `None` if it can't be pushed.
    fn translate(&mut self, e: &Expr) -> Option<String> {
        match e {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                if *op == Operator::And {
                    let l = self.translate(left)?;
                    let r = self.translate(right)?;
                    return Some(format!("({l} AND {r})"));
                }
                let (col, lit, op) = match (self.column(left), self.column(right)) {
                    (Some(c), None) => (c, right.as_ref(), *op),
                    (None, Some(c)) => (c, left.as_ref(), op.swap()?),
                    _ => return None,
                };
                let ordered = matches!(col.kind, ColKind::Int | ColKind::Float);
                let sym = match op {
                    Operator::Eq => "=",
                    Operator::NotEq => "<>",
                    Operator::Lt if ordered => "<",
                    Operator::LtEq if ordered => "<=",
                    Operator::Gt if ordered => ">",
                    Operator::GtEq if ordered => ">=",
                    _ => return None,
                };
                let b = Self::bind(col.kind, lit)?;
                let ph = self.placeholder(b);
                Some(format!("{} {sym} {ph}", quote_ident(&col.name)))
            }
            Expr::IsNull(inner) => Some(format!("{} IS NULL", quote_ident(&self.column(inner)?.name))),
            Expr::IsNotNull(inner) => {
                Some(format!("{} IS NOT NULL", quote_ident(&self.column(inner)?.name)))
            }
            Expr::InList(il) if !il.negated && !il.list.is_empty() && il.list.len() <= 1000 => {
                let col = self.column(&il.expr)?;
                let binds: Vec<Bind> = il
                    .list
                    .iter()
                    .map(|x| Self::bind(col.kind, x))
                    .collect::<Option<_>>()?;
                let phs: Vec<String> = binds.into_iter().map(|b| self.placeholder(b)).collect();
                Some(format!("{} IN ({})", quote_ident(&col.name), phs.join(", ")))
            }
            _ => None,
        }
    }

    /// Push `e` if possible; on failure nothing is recorded.
    fn push(&mut self, e: &Expr) -> bool {
        let (n_sql, n_binds) = (self.sql.len(), self.binds.len());
        match self.translate(e) {
            Some(s) => {
                self.sql.push(s);
                true
            }
            None => {
                self.sql.truncate(n_sql);
                self.binds.truncate(n_binds);
                false
            }
        }
    }
}

#[async_trait]
impl TableProvider for ExternalTable {
    fn schema(&self) -> SchemaRef {
        self.arrow_schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        // Inexact: DataFusion re-applies the filter, so the remote WHERE
        // only has to be a superset of the true answer.
        Ok(filters
            .iter()
            .map(|f| {
                if Where::new(&self.cols).push(f) {
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let idx: Vec<usize> = match projection {
            Some(p) => p.clone(),
            None => (0..self.cols.len()).collect(),
        };
        let cols: Vec<Column> = idx.iter().map(|&i| self.cols[i].clone()).collect();
        let mut w = Where::new(&self.cols);
        for f in filters {
            w.push(f);
        }
        let select = if cols.is_empty() {
            "1".to_string()
        } else {
            cols.iter()
                .map(|c| c.kind.select_expr(&c.name))
                .collect::<Vec<_>>()
                .join(", ")
        };
        let max = self.config.max_rows;
        let fetch = limit.map_or(max + 1, |l| l.min(max + 1));
        let mut sql = format!(
            "SELECT {select} FROM {}.{}",
            quote_ident(&self.schema_name),
            quote_ident(&self.table)
        );
        if !w.sql.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&w.sql.join(" AND "));
        }
        sql.push_str(&format!(" LIMIT {fetch}"));
        let key = (
            self.conn.clone(),
            format!(
                "{sql} -- {}",
                w.binds.iter().map(|b| b.to_string()).collect::<Vec<_>>().join(",")
            ),
        );
        let schema = Arc::new(Schema::new(
            cols.iter()
                .map(|c| c.kind.arrow_field(&c.name))
                .collect::<Vec<_>>(),
        ));
        let batch = match self.snapshots.get(&key) {
            Some(b) => b,
            None => {
                let fut = fetch_rows(&self.pool, &sql, &w.binds, &cols, schema.clone());
                let batch = timed(&self.config, &self.conn, &self.schema_name, &self.table, fut)
                    .await
                    .map_err(|e| e.into_df())?;
                if batch.num_rows() > max {
                    return Err(ExqlError::Execution(format!(
                        "external table {}.{} has more than {max} matching rows",
                        self.schema_name, self.table
                    ))
                    .into_df());
                }
                self.snapshots.put(key, batch.clone());
                batch
            }
        };
        // Count the snapshot against the query memory pool while the plan
        // runs.
        let reservation = MemoryConsumer::new(format!(
            "external {}.{}.{}",
            self.conn, self.schema_name, self.table
        ))
        .register(&state.runtime_env().memory_pool);
        reservation.try_grow(batch.get_array_memory_size())?;
        let mem = MemTable::try_new(schema, vec![vec![batch]])?;
        let inner = mem.scan(state, None, &[], limit).await?;
        Ok(Arc::new(ReservedExec {
            inner,
            reservation: Arc::new(reservation),
        }))
    }
}

async fn fetch_rows(
    pool: &PgPool,
    sql: &str,
    binds: &[Bind],
    cols: &[Column],
    schema: SchemaRef,
) -> Result<RecordBatch, ExqlError> {
    let mut args = PgArguments::default();
    for b in binds {
        let r = match b {
            Bind::Int(v) => args.add(*v),
            Bind::Float(v) => args.add(*v),
            Bind::Bool(v) => args.add(*v),
            Bind::Text(v) => args.add(v.clone()),
        };
        r.map_err(|e| ExqlError::Execution(format!("external query: {e}")))?;
    }
    let rows = sqlx::query_with(sql, args)
        .fetch_all(pool)
        .await
        .map_err(ext_err)?;
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(cols.len());
    for (i, c) in cols.iter().enumerate() {
        let arr: ArrayRef = match c.kind {
            ColKind::Int => {
                let mut b = Int64Builder::with_capacity(rows.len());
                for r in &rows {
                    b.append_option(r.try_get::<Option<i64>, _>(i).map_err(ext_err)?);
                }
                Arc::new(b.finish())
            }
            ColKind::Float => {
                let mut b = Float64Builder::with_capacity(rows.len());
                for r in &rows {
                    b.append_option(r.try_get::<Option<f64>, _>(i).map_err(ext_err)?);
                }
                Arc::new(b.finish())
            }
            ColKind::Bool => {
                let mut b = BooleanBuilder::with_capacity(rows.len());
                for r in &rows {
                    b.append_option(r.try_get::<Option<bool>, _>(i).map_err(ext_err)?);
                }
                Arc::new(b.finish())
            }
            ColKind::Decimal(p, s) => {
                let mut b = StringBuilder::new();
                for r in &rows {
                    b.append_option(r.try_get::<Option<String>, _>(i).map_err(ext_err)?);
                }
                let text: ArrayRef = Arc::new(b.finish());
                cast(&text, &DataType::Decimal128(p, s))?
            }
            ColKind::Text | ColKind::Json => {
                let mut b = StringBuilder::new();
                for r in &rows {
                    b.append_option(r.try_get::<Option<String>, _>(i).map_err(ext_err)?);
                }
                Arc::new(b.finish())
            }
            ColKind::Timestamp => {
                let mut b =
                    TimestampMillisecondBuilder::with_capacity(rows.len()).with_timezone("UTC");
                for r in &rows {
                    b.append_option(r.try_get::<Option<i64>, _>(i).map_err(ext_err)?);
                }
                Arc::new(b.finish())
            }
            ColKind::Date => {
                let mut b = Date32Builder::with_capacity(rows.len());
                for r in &rows {
                    b.append_option(r.try_get::<Option<i32>, _>(i).map_err(ext_err)?);
                }
                Arc::new(b.finish())
            }
        };
        arrays.push(arr);
    }
    Ok(RecordBatch::try_new_with_options(
        schema,
        arrays,
        &RecordBatchOptions::new().with_row_count(Some(rows.len())),
    )?)
}

/// Holds a memory reservation for as long as the wrapped plan lives.
#[derive(Debug)]
struct ReservedExec {
    inner: Arc<dyn ExecutionPlan>,
    reservation: Arc<MemoryReservation>,
}

impl DisplayAs for ReservedExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ExternalSnapshotExec: reserved={}", self.reservation.size())
    }
}

impl ExecutionPlan for ReservedExec {
    fn name(&self) -> &str {
        "ExternalSnapshotExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.inner.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.inner]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DFResult<TreeNodeRecursion>,
    ) -> DFResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(ReservedExec {
            inner: children.remove(0),
            reservation: self.reservation.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        self.inner.execute(partition, context)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quotes_identifiers() {
        assert_eq!(quote_ident("users"), "\"users\"");
        assert_eq!(
            quote_ident("a\"; DROP TABLE x; --"),
            "\"a\"\"; DROP TABLE x; --\""
        );
    }
}
