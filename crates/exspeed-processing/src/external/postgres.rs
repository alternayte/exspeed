use sqlx::Column as _;
use sqlx::Row as _;

use crate::error::ExqlError;
use crate::types::{Row, Value};

/// Query all rows from a Postgres table and return them as `Vec<Row>`.
///
/// This is an async function; callers in a sync context should bridge via
/// `tokio::task::block_in_place` + `Handle::current().block_on(...)`.
///
/// # Example (manual test)
///
/// ```ignore
/// // Requires a running Postgres instance:
/// //   docker run --rm -e POSTGRES_PASSWORD=pw -p 5432:5432 postgres:16
/// //   psql postgresql://postgres:pw@localhost/postgres -c \
/// //     "CREATE TABLE test(id INT, name TEXT); INSERT INTO test VALUES (1,'a'),(2,'b');"
/// //
/// // Then:
/// let rows = query_postgres("postgresql://postgres:pw@localhost/postgres", "test").await?;
/// assert_eq!(rows.len(), 2);
/// ```
pub async fn query_postgres(url: &str, table: &str) -> Result<Vec<Row>, ExqlError> {
    let pool = sqlx::PgPool::connect(url)
        .await
        .map_err(|e| ExqlError::Execution(format!("postgres connect: {e}")))?;

    let sql = format!("SELECT * FROM {table}");
    let rows: Vec<sqlx::postgres::PgRow> = sqlx::query(&sql)
        .fetch_all(&pool)
        .await
        .map_err(|e| ExqlError::Execution(format!("postgres query: {e}")))?;

    let result: Vec<Row> = rows
        .iter()
        .map(|row| {
            let columns: Vec<String> = row.columns().iter().map(|c| c.name().to_string()).collect();
            let values: Vec<Value> = row
                .columns()
                .iter()
                .map(|c| {
                    // Try types in order: string, i64, f64, bool — fall back to Null.
                    if let Ok(v) = row.try_get::<String, _>(c.name()) {
                        Value::Text(v)
                    } else if let Ok(v) = row.try_get::<i64, _>(c.name()) {
                        Value::Int(v)
                    } else if let Ok(v) = row.try_get::<i32, _>(c.name()) {
                        Value::Int(v as i64)
                    } else if let Ok(v) = row.try_get::<f64, _>(c.name()) {
                        Value::Float(v)
                    } else if let Ok(v) = row.try_get::<f32, _>(c.name()) {
                        Value::Float(v as f64)
                    } else if let Ok(v) = row.try_get::<bool, _>(c.name()) {
                        Value::Bool(v)
                    } else {
                        Value::Null
                    }
                })
                .collect();
            Row { columns, values }
        })
        .collect();

    pool.close().await;
    Ok(result)
}
