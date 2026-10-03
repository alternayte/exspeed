//! External Postgres tables, reachable only through registered connections.
//!
//! A table reference `conn.table` (schema `public`) or `conn.schema.table`
//! resolves to a [`DataFusion` table](datafusion::catalog::TableProvider)
//! backed by a snapshot of the remote table:
//!
//! - the column list comes from `information_schema.columns` (parameterized
//!   query);
//! - the snapshot is fetched with a `SELECT` whose identifiers are quoted
//!   (`"schema"."table"`, embedded quotes doubled), never interpolated raw;
//! - snapshots are cached per `(connection, schema, table)` in a small
//!   LRU with a TTL, and pools are reused per connection.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use datafusion::arrow::array::{
    ArrayRef, BooleanBuilder, Date32Builder, Float64Builder, Int64Builder, RecordBatch,
    StringBuilder, TimestampMillisecondBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::MemTable;
use sqlx::postgres::{PgPool, PgPoolOptions, PgRow};
use sqlx::Row;

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
    Bool,
    Text,
    Json,
    Timestamp,
    Date,
}

impl ColKind {
    fn from_pg(data_type: &str) -> Self {
        match data_type {
            "smallint" | "integer" | "bigint" => ColKind::Int,
            "real" | "double precision" | "numeric" => ColKind::Float,
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
            ColKind::Bool => Field::new(name, DataType::Boolean, true),
            ColKind::Text => Field::new(name, DataType::Utf8, true),
            ColKind::Json => Field::new(name, DataType::Utf8, true).with_metadata(HashMap::from([(
                JSON_META.to_string(),
                "true".to_string(),
            )])),
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
            ColKind::Text | ColKind::Json => format!("{q}::text"),
            ColKind::Timestamp => format!("(extract(epoch from {q}) * 1000)::int8"),
            ColKind::Date => format!("({q} - date '1970-01-01')::int4"),
        }
    }
}

struct Cached {
    loaded: Instant,
    last_used: Instant,
    schema: SchemaRef,
    batch: RecordBatch,
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

/// Fetches and caches external tables.
pub struct ExternalTables {
    registry: Arc<ConnectionRegistry>,
    config: ExternalConfig,
    cache: Mutex<HashMap<(String, String, String), Cached>>,
    pools: Mutex<HashMap<String, (String, PgPool)>>,
}

impl ExternalTables {
    pub fn new(registry: Arc<ConnectionRegistry>, config: ExternalConfig) -> Self {
        Self {
            registry,
            config,
            cache: Mutex::new(HashMap::new()),
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

    /// Forget cached snapshots and pools of a connection.
    pub fn invalidate(&self, conn: &str) {
        self.cache.lock().unwrap().retain(|k, _| k.0 != conn);
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
    ) -> Result<Option<Arc<MemTable>>, ExqlError> {
        let Some(cfg) = self.registry.get(conn) else {
            return Ok(None);
        };
        if !matches!(cfg.driver.as_str(), "postgres" | "postgresql") {
            return Err(ExqlError::unsupported(
                format!("driver '{}'", cfg.driver),
                "external tables support the postgres driver",
            ));
        }
        let key = (conn.to_string(), schema.to_string(), table.to_string());
        {
            let mut cache = self.cache.lock().unwrap();
            if let Some(c) = cache.get_mut(&key) {
                if c.loaded.elapsed() < self.config.cache_ttl {
                    c.last_used = Instant::now();
                    let t = MemTable::try_new(c.schema.clone(), vec![vec![c.batch.clone()]])?;
                    return Ok(Some(Arc::new(t)));
                }
                cache.remove(&key);
            }
        }
        let pool = self.pool(&cfg)?;
        let fut = fetch_table(&pool, schema, table, self.config.max_rows);
        let fetched = tokio::time::timeout(self.config.fetch_timeout, fut)
            .await
            .map_err(|_| {
                ExqlError::Execution(format!(
                    "fetching {conn}.{schema}.{table} timed out after {:?}",
                    self.config.fetch_timeout
                ))
            })??;
        let Some((schema_ref, batch)) = fetched else {
            return Ok(None);
        };
        {
            let mut cache = self.cache.lock().unwrap();
            if cache.len() >= self.config.cache_entries {
                if let Some(oldest) = cache
                    .iter()
                    .min_by_key(|(_, c)| c.last_used)
                    .map(|(k, _)| k.clone())
                {
                    cache.remove(&oldest);
                }
            }
            let now = Instant::now();
            cache.insert(
                key,
                Cached {
                    loaded: now,
                    last_used: now,
                    schema: schema_ref.clone(),
                    batch: batch.clone(),
                },
            );
        }
        Ok(Some(Arc::new(MemTable::try_new(schema_ref, vec![vec![batch]])?)))
    }
}

async fn fetch_table(
    pool: &PgPool,
    schema: &str,
    table: &str,
    max_rows: usize,
) -> Result<Option<(SchemaRef, RecordBatch)>, ExqlError> {
    let ext = |e: sqlx::Error| ExqlError::Execution(format!("external query failed: {e}"));
    let cols: Vec<(String, String)> = sqlx::query(
        "SELECT column_name::text, data_type::text FROM information_schema.columns \
         WHERE table_schema = $1 AND table_name = $2 ORDER BY ordinal_position",
    )
    .bind(schema)
    .bind(table)
    .fetch_all(pool)
    .await
    .map_err(ext)?
    .into_iter()
    .map(|r: PgRow| Ok((r.try_get(0)?, r.try_get(1)?)))
    .collect::<Result<_, sqlx::Error>>()
    .map_err(ext)?;
    if cols.is_empty() {
        return Ok(None);
    }
    let kinds: Vec<ColKind> = cols.iter().map(|(_, t)| ColKind::from_pg(t)).collect();
    let arrow_schema: SchemaRef = Arc::new(Schema::new(
        cols.iter()
            .zip(&kinds)
            .map(|((n, _), k)| k.arrow_field(n))
            .collect::<Vec<_>>(),
    ));
    let select = cols
        .iter()
        .zip(&kinds)
        .map(|((n, _), k)| k.select_expr(n))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT {select} FROM {}.{} LIMIT {}",
        quote_ident(schema),
        quote_ident(table),
        max_rows + 1
    );
    let rows = sqlx::query(&sql).fetch_all(pool).await.map_err(ext)?;
    if rows.len() > max_rows {
        return Err(ExqlError::Execution(format!(
            "external table {schema}.{table} has more than {max_rows} rows"
        )));
    }
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(kinds.len());
    for (i, k) in kinds.iter().enumerate() {
        let arr: ArrayRef = match k {
            ColKind::Int => {
                let mut b = Int64Builder::with_capacity(rows.len());
                for r in &rows {
                    b.append_option(r.try_get::<Option<i64>, _>(i).map_err(ext)?);
                }
                Arc::new(b.finish())
            }
            ColKind::Float => {
                let mut b = Float64Builder::with_capacity(rows.len());
                for r in &rows {
                    b.append_option(r.try_get::<Option<f64>, _>(i).map_err(ext)?);
                }
                Arc::new(b.finish())
            }
            ColKind::Bool => {
                let mut b = BooleanBuilder::with_capacity(rows.len());
                for r in &rows {
                    b.append_option(r.try_get::<Option<bool>, _>(i).map_err(ext)?);
                }
                Arc::new(b.finish())
            }
            ColKind::Text | ColKind::Json => {
                let mut b = StringBuilder::new();
                for r in &rows {
                    b.append_option(r.try_get::<Option<String>, _>(i).map_err(ext)?);
                }
                Arc::new(b.finish())
            }
            ColKind::Timestamp => {
                let mut b = TimestampMillisecondBuilder::with_capacity(rows.len())
                    .with_timezone("UTC");
                for r in &rows {
                    b.append_option(r.try_get::<Option<i64>, _>(i).map_err(ext)?);
                }
                Arc::new(b.finish())
            }
            ColKind::Date => {
                let mut b = Date32Builder::with_capacity(rows.len());
                for r in &rows {
                    b.append_option(r.try_get::<Option<i32>, _>(i).map_err(ext)?);
                }
                Arc::new(b.finish())
            }
        };
        arrays.push(arr);
    }
    let batch = RecordBatch::try_new(arrow_schema.clone(), arrays)?;
    Ok(Some((arrow_schema, batch)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quotes_identifiers() {
        assert_eq!(quote_ident("users"), "\"users\"");
        assert_eq!(quote_ident("a\"; DROP TABLE x; --"), "\"a\"\"; DROP TABLE x; --\"");
    }
}
