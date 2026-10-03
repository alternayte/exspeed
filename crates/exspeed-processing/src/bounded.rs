//! Bounded (one-shot) query execution on DataFusion.

use std::time::Instant;

use datafusion::execution::session_state::SessionState;
use datafusion::prelude::{SQLOptions, SessionContext};
use futures_util::StreamExt;
use serde::Serialize;
use serde_json::{json, Value as Json};

use crate::convert::cell_to_json;
use crate::error::ExqlError;
use crate::session::ExqlConfig;

/// The result of a bounded query.
#[derive(Debug, Clone, Serialize)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Json>>,
    pub row_count: usize,
    pub execution_time_ms: u64,
    /// More rows existed than `max_result_rows`; only the first ones are
    /// returned.
    pub truncated: bool,
}

impl QueryResult {
    pub fn to_json(&self) -> Json {
        json!({
            "columns": self.columns,
            "rows": self.rows,
            "row_count": self.row_count,
            "execution_time_ms": self.execution_time_ms,
            "truncated": self.truncated,
        })
    }
}

/// A spawned task that is aborted when this handle is dropped.
struct AbortOnDrop<T>(tokio::task::JoinHandle<T>);

impl<T> std::future::Future for AbortOnDrop<T> {
    type Output = Result<T, tokio::task::JoinError>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        std::pin::Pin::new(&mut self.0).poll(cx)
    }
}

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

fn sql_options() -> SQLOptions {
    SQLOptions::new()
        .with_allow_ddl(false)
        .with_allow_dml(false)
        .with_allow_statements(false)
}

/// Run `sql` with the configured timeout and row cap. Dropping the returned
/// future cancels the query.
pub async fn execute(
    state: SessionState,
    sql: &str,
    cfg: &ExqlConfig,
) -> Result<QueryResult, ExqlError> {
    let start = Instant::now();
    let max_rows = cfg.max_result_rows;
    let normalized = match crate::sql::parse_statement(sql)? {
        crate::sql::Statement::Query(q) => q,
        _ => {
            return Err(ExqlError::unsupported(
                "this statement as a bounded query",
                "CREATE / DROP / PAUSE / RESUME statements go through POST /api/v1/queries",
            ))
        }
    };
    let fut = async move {
        let mut stmt = crate::ast::parse_one(&normalized)?;
        crate::ast::rewrite_json_numeric_args(&mut stmt)?;
        crate::ast::rewrite_distinct_order_by(&mut stmt)?;
        crate::ast::alias_qualified_json(&mut stmt);
        let plan = state.statement_to_plan(stmt).await?;
        sql_options().verify_plan(&plan)?;
        let ctx = SessionContext::new_with_state(state);
        let df = ctx.execute_logical_plan(plan).await?;
        let columns: Vec<String> = df
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        let mut stream = df.execute_stream().await?;
        let mut rows: Vec<Vec<Json>> = Vec::new();
        let mut truncated = false;
        'outer: while let Some(batch) = stream.next().await {
            let batch = batch?;
            let schema = batch.schema();
            for r in 0..batch.num_rows() {
                if rows.len() >= max_rows {
                    truncated = true;
                    break 'outer;
                }
                rows.push(
                    batch
                        .columns()
                        .iter()
                        .zip(schema.fields().iter())
                        .map(|(c, f)| cell_to_json(c, f, r))
                        .collect(),
                );
            }
        }
        Ok::<_, ExqlError>((columns, rows, truncated))
    };
    // Run on its own task so the timeout (and a dropped request) take effect
    // even while DataFusion is busy on another worker; the guard aborts the
    // task when this future completes or is dropped.
    let task = AbortOnDrop(tokio::spawn(fut));
    let (columns, rows, truncated) = match tokio::time::timeout(cfg.query_timeout, task).await {
        Err(_) => return Err(ExqlError::Timeout(cfg.query_timeout.as_millis() as u64)),
        Ok(Err(e)) if e.is_panic() => {
            return Err(ExqlError::Internal(format!("query panicked: {e}")))
        }
        Ok(Err(_)) => return Err(ExqlError::Cancelled),
        Ok(Ok(r)) => r?,
    };
    Ok(QueryResult {
        row_count: rows.len(),
        columns,
        rows,
        execution_time_ms: start.elapsed().as_millis() as u64,
        truncated,
    })
}
