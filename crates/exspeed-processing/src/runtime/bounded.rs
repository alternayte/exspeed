use std::sync::Arc;
use std::time::Instant;

use exspeed_common::{Offset, StreamName};
use exspeed_streams::StorageEngine;

const MAX_RESULT_ROWS: usize = 10_000;

/// Boxed future returned by the recursive `build_operator` helper.
type BuildOperatorFuture<'a> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<Box<dyn Operator>, ExqlError>> + Send + 'a>>;

use crate::error::ExqlError;
use crate::external::connections::ConnectionRegistry;
use crate::materialized_view::MaterializedViewRegistry;
use crate::parser::ast::{BinaryOperator, Expr, ExqlStatement, LiteralValue};
use crate::planner::physical::PhysicalPlan;
use crate::runtime::operators::aggregate::AggregateOperator;
use crate::runtime::operators::filter::FilterOperator;
use crate::runtime::operators::index_scan::IndexScanOperator;
use crate::runtime::operators::join::HashJoinOperator;
use crate::runtime::operators::limit::LimitOperator;
use crate::runtime::operators::project::ProjectOperator;
use crate::runtime::operators::scan::ScanOperator;
use crate::runtime::operators::sort::SortOperator;
use crate::runtime::operators::Operator;
use crate::types::{ResultSet, Row};

/// Metadata for a secondary index loaded from `{data_dir}/indexes/{name}.json`.
#[derive(Debug, Clone)]
pub struct IndexDef {
    pub name: String,
    pub stream: String,
    pub field_path: String,
}

/// Execute a bounded (batch) SQL query against storage, returning a ResultSet.
pub async fn execute_bounded(
    sql: &str,
    storage: &Arc<dyn StorageEngine>,
) -> Result<ResultSet, ExqlError> {
    execute_bounded_with_mv(sql, storage, None, None, &[]).await
}

/// Execute a bounded (batch) SQL query with optional external connection support.
pub async fn execute_bounded_with_connections(
    sql: &str,
    storage: &Arc<dyn StorageEngine>,
    connections: Option<&ConnectionRegistry>,
) -> Result<ResultSet, ExqlError> {
    execute_bounded_with_mv(sql, storage, connections, None, &[]).await
}

/// Execute a bounded (batch) SQL query with optional connection and MV registry support.
pub async fn execute_bounded_with_mv(
    sql: &str,
    storage: &Arc<dyn StorageEngine>,
    connections: Option<&ConnectionRegistry>,
    mv_registry: Option<&MaterializedViewRegistry>,
    indexes: &[IndexDef],
) -> Result<ResultSet, ExqlError> {
    let start = Instant::now();

    // 1. Parse SQL → AST
    let stmt = crate::parser::parse(sql)?;

    // 2. Extract QueryExpr (only SELECT queries supported)
    let query = match stmt {
        ExqlStatement::Query(q) => q,
        _ => {
            return Err(ExqlError::Execution(
                "only SELECT queries are supported in bounded execution".into(),
            ));
        }
    };

    // 3. Plan → PhysicalPlan
    let plan = crate::planner::plan(&query, crate::parser::ast::EmitMode::Changes)?;

    // 4. Build operator tree
    let mut root = build_operator(&plan, storage, connections, mv_registry, indexes).await?;

    // 5. Pull all rows from the root operator.
    //
    // ScanOperator::next() uses tokio::task::block_in_place internally.
    // block_in_place requires a multi-threaded runtime, so we always drain
    // the operator tree on a dedicated blocking thread via spawn_blocking.
    // This works on both multi-threaded and current-thread runtimes.
    let columns = root.columns();
    let (columns, rows) = tokio::task::spawn_blocking(move || {
        let mut rows = Vec::new();
        while let Some(row) = root.next() {
            rows.push(row);
            if rows.len() >= MAX_RESULT_ROWS {
                break;
            }
        }
        (columns, rows)
    })
    .await
    .map_err(|e| ExqlError::Execution(format!("operator drain panicked: {e}")))?;

    let elapsed = start.elapsed().as_millis() as u64;

    Ok(ResultSet {
        columns,
        rows,
        execution_time_ms: elapsed,
    })
}

/// Recursively build an operator tree from a PhysicalPlan.
fn build_operator<'a>(
    plan: &'a PhysicalPlan,
    storage: &'a Arc<dyn StorageEngine>,
    connections: Option<&'a ConnectionRegistry>,
    mv_registry: Option<&'a MaterializedViewRegistry>,
    indexes: &'a [IndexDef],
) -> BuildOperatorFuture<'a> {
    Box::pin(async move {
    match plan {
        PhysicalPlan::SeqScan { stream, alias, required_columns, predicate, reverse_limit, timestamp_lower_bound, key_eq_filter } => {
            // Check materialized views first
            if let Some(mv_reg) = mv_registry {
                if let Some((_columns, rows)) = mv_reg.get_rows(stream) {
                    let scan: Box<dyn Operator> = Box::new(ScanOperator::from_rows(rows));
                    // If predicate was pushed down, re-apply as a FilterOperator
                    // since MV rows bypass storage-level filtering.
                    if let Some(pred) = predicate {
                        return Ok(Box::new(FilterOperator::new(scan, pred.clone())) as Box<dyn Operator>);
                    }
                    return Ok(scan);
                }
            }

            let stream_name = StreamName::try_from(stream.as_str()).map_err(|e| {
                ExqlError::Storage(format!("invalid stream name '{}': {}", stream, e))
            })?;

            // Check if an index can serve this query (only for forward scans
            // without reverse_limit or timestamp bounds).
            if reverse_limit.is_none() && timestamp_lower_bound.is_none() {
                if let Some(ref pred) = predicate {
                    if let Some((idx_name, _field, lookup_val)) =
                        find_indexed_payload_eq(pred, indexes, stream)
                    {
                        if let Some(partition_dir) = storage.partition_dir_path(stream, 0) {
                            return Ok(Box::new(IndexScanOperator::new(
                                storage.clone(),
                                stream_name,
                                alias.clone(),
                                required_columns.clone(),
                                partition_dir,
                                idx_name,
                                lookup_val,
                                predicate.clone(),
                            )) as Box<dyn Operator>);
                        }
                    }
                }
            }

            if let Some(limit) = reverse_limit {
                Ok(Box::new(ScanOperator::reverse_tail(
                    storage.clone(), stream_name, alias.clone(),
                    required_columns.clone(), predicate.clone(), *limit,
                )) as Box<dyn Operator>)
            } else {
                let start = if let Some(ts_bound) = timestamp_lower_bound {
                    storage.seek_by_time(&stream_name, *ts_bound).await.unwrap_or(Offset(0))
                } else {
                    Offset(0)
                };
                Ok(Box::new(ScanOperator::streaming_from(
                    storage.clone(),
                    stream_name,
                    alias.clone(),
                    required_columns.clone(),
                    predicate.clone(),
                    start,
                    key_eq_filter.clone(),
                )) as Box<dyn Operator>)
            }
        }

        PhysicalPlan::ExternalScan {
            connection,
            table,
            alias: _,
            driver,
        } => {
            // Resolve the driver and URL for this external scan.
            let (resolved_driver, url) =
                resolve_external_connection(connection, driver.as_deref(), connections)?;

            // Run the async query synchronously. We use block_in_place so the
            // tokio runtime can still make progress on other tasks while we wait.
            let rows = run_external_query(&resolved_driver, &url, table)?;
            Ok(Box::new(ScanOperator::new(rows)) as Box<dyn Operator>)
        }

        PhysicalPlan::Filter { input, predicate } => {
            let child = build_operator(input, storage, connections, mv_registry, indexes).await?;
            Ok(Box::new(FilterOperator::new(child, predicate.clone())) as Box<dyn Operator>)
        }

        PhysicalPlan::Project { input, items } => {
            let child = build_operator(input, storage, connections, mv_registry, indexes).await?;
            Ok(Box::new(ProjectOperator::new(child, items.clone())) as Box<dyn Operator>)
        }

        PhysicalPlan::HashJoin {
            left,
            right,
            on,
            join_type,
        } => {
            let left_op = build_operator(left, storage, connections, mv_registry, indexes).await?;
            let right_op = build_operator(right, storage, connections, mv_registry, indexes).await?;
            Ok(Box::new(HashJoinOperator::new(
                left_op,
                right_op,
                join_type.clone(),
                on.clone(),
            )) as Box<dyn Operator>)
        }

        PhysicalPlan::HashAggregate {
            input,
            group_by,
            select_items,
        } => {
            let child = build_operator(input, storage, connections, mv_registry, indexes).await?;
            Ok(Box::new(AggregateOperator::new(
                child,
                group_by.clone(),
                select_items.clone(),
            )) as Box<dyn Operator>)
        }

        PhysicalPlan::Sort { input, order_by } => {
            let child = build_operator(input, storage, connections, mv_registry, indexes).await?;
            Ok(Box::new(SortOperator::new(child, order_by.clone())) as Box<dyn Operator>)
        }

        PhysicalPlan::Limit {
            input,
            limit,
            offset,
        } => {
            let child = build_operator(input, storage, connections, mv_registry, indexes).await?;
            Ok(Box::new(LimitOperator::new(
                child,
                *limit,
                offset.unwrap_or(0),
            )) as Box<dyn Operator>)
        }

        PhysicalPlan::TopN { input, order_by, limit } => {
            let child = build_operator(input, storage, connections, mv_registry, indexes).await?;
            Ok(Box::new(crate::runtime::operators::topn::TopNOperator::new(child, order_by.clone(), *limit)) as Box<dyn Operator>)
        }

        PhysicalPlan::WindowedAggregate { .. } => Err(ExqlError::Execution(
            "WindowedAggregate is not supported in bounded execution".into(),
        )),

        PhysicalPlan::StreamStreamJoin { .. } => Err(ExqlError::Execution(
            "StreamStreamJoin is not supported in bounded execution".into(),
        )),

        PhysicalPlan::IndexScan { stream, alias, required_columns, index_name, field_path: _, lookup_value, predicate } => {
            let stream_name = StreamName::try_from(stream.as_str()).map_err(|e| {
                ExqlError::Storage(format!("invalid stream name '{}': {}", stream, e))
            })?;
            let partition_dir = storage.partition_dir_path(stream, 0)
                .ok_or_else(|| ExqlError::Storage("no partition directory available".into()))?;
            Ok(Box::new(IndexScanOperator::new(
                storage.clone(),
                stream_name,
                alias.clone(),
                required_columns.clone(),
                partition_dir,
                index_name.clone(),
                lookup_value.clone(),
                predicate.clone(),
            )) as Box<dyn Operator>)
        }
    }
    })
}

// ---------------------------------------------------------------------------
// Index-aware predicate detection
// ---------------------------------------------------------------------------

/// Walk a predicate expression looking for `payload->>'field_path' = 'value'`
/// patterns that match a secondary index. Returns `(index_name, field_path,
/// lookup_value)` on the first match found.
///
/// For AND-combined predicates we recurse into both sides so a compound filter
/// like `payload->>'region' = 'eu' AND payload->>'total' > 100` can still use
/// the index on `region`.
fn find_indexed_payload_eq(
    expr: &Expr,
    indexes: &[IndexDef],
    stream: &str,
) -> Option<(String, String, String)> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => {
            // Check left = payload->>'field', right = literal
            if let Some((field, value)) = extract_json_access_eq(left, right) {
                if let Some(idx) = indexes
                    .iter()
                    .find(|i| i.stream == stream && i.field_path == field)
                {
                    return Some((idx.name.clone(), field, value));
                }
            }
            // Check reversed: right = payload->>'field', left = literal
            if let Some((field, value)) = extract_json_access_eq(right, left) {
                if let Some(idx) = indexes
                    .iter()
                    .find(|i| i.stream == stream && i.field_path == field)
                {
                    return Some((idx.name.clone(), field, value));
                }
            }
            None
        }
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => find_indexed_payload_eq(left, indexes, stream)
            .or_else(|| find_indexed_payload_eq(right, indexes, stream)),
        _ => None,
    }
}

/// Match a pair of expressions against the pattern:
///   `json_side` = `payload->>'field'`  (JsonAccess with as_text=true on Column "payload")
///   `literal_side` = string literal
///
/// Returns `(field_name, literal_value)` if the pattern matches.
fn extract_json_access_eq(json_side: &Expr, literal_side: &Expr) -> Option<(String, String)> {
    if let Expr::JsonAccess {
        expr,
        field,
        as_text: _,
    } = json_side
    {
        if matches!(expr.as_ref(), Expr::Column { name, .. } if name == "payload") {
            let val_str = match literal_side {
                Expr::Literal(LiteralValue::String(s)) => Some(s.clone()),
                Expr::Literal(LiteralValue::Int(n)) => Some(n.to_string()),
                Expr::Literal(LiteralValue::Float(f)) => Some(f.to_string()),
                _ => None,
            };
            if let Some(val) = val_str {
                return Some((field.clone(), val));
            }
        }
    }
    None
}

/// Resolve the driver and URL for an external scan.
///
/// If the `driver` field is provided on the plan node, it is used as an inline
/// connection specification (the `connection` field is treated as the URL).
/// Otherwise, the connection name is looked up in the registry.
fn resolve_external_connection(
    connection: &str,
    driver: Option<&str>,
    registry: Option<&ConnectionRegistry>,
) -> Result<(String, String), ExqlError> {
    if let Some(drv) = driver {
        // Inline connection: connection field *is* the URL.
        Ok((drv.to_string(), connection.to_string()))
    } else if let Some(reg) = registry {
        let cfg = reg
            .get(connection)
            .ok_or_else(|| ExqlError::Execution(format!("unknown connection '{connection}'")))?;
        Ok((cfg.driver.clone(), cfg.url.clone()))
    } else {
        Err(ExqlError::Execution(format!(
            "connection '{connection}' referenced but no connection registry available"
        )))
    }
}

/// Bridge async external queries into the sync operator pipeline.
///
/// Uses `tokio::task::block_in_place` + `Handle::current().block_on` to run the
/// async query without blocking the tokio runtime. This works because the bounded
/// executor runs inside a multi-threaded tokio runtime.
fn run_external_query(driver: &str, url: &str, table: &str) -> Result<Vec<Row>, ExqlError> {
    // If we are inside a tokio runtime, use block_in_place to bridge.
    // Otherwise (e.g., tests without a runtime), create a temporary runtime.
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => tokio::task::block_in_place(|| {
            handle.block_on(dispatch_external_query(driver, url, table))
        }),
        Err(_) => {
            // No runtime — build a temporary one (useful for testing).
            let rt = tokio::runtime::Runtime::new().map_err(|e| {
                ExqlError::Execution(format!("failed to create tokio runtime: {e}"))
            })?;
            rt.block_on(dispatch_external_query(driver, url, table))
        }
    }
}

async fn dispatch_external_query(
    driver: &str,
    url: &str,
    table: &str,
) -> Result<Vec<Row>, ExqlError> {
    use crate::external::{mssql, postgres};

    match driver {
        "postgres" | "postgresql" => postgres::query_postgres(url, table).await,
        "mssql" | "sqlserver" => mssql::query_mssql(url, table).await,
        other => Err(ExqlError::Execution(format!(
            "unsupported external driver '{other}'"
        ))),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use crate::types::Value;
    use exspeed_storage::memory::MemoryStorage;
    use exspeed_streams::Record;

    async fn setup_test_data() -> Arc<dyn StorageEngine> {
        let storage = Arc::new(MemoryStorage::new());
        storage
            .create_stream(&StreamName::try_from("orders").unwrap(), 0, 0)
            .await
            .unwrap();

        for i in 0..5 {
            let record = Record {
                key: Some(Bytes::from(format!("ord-{i}"))),
                value: Bytes::from(format!(
                    r#"{{"total": {}, "region": "{}"}}"#,
                    (i + 1) * 100,
                    if i % 2 == 0 { "eu" } else { "us" }
                )),
                subject: format!("order.{}.created", if i % 2 == 0 { "eu" } else { "us" }),
                headers: vec![],

                timestamp_ns: None,
            };
            storage
                .append(&StreamName::try_from("orders").unwrap(), &record)
                .await
                .unwrap();
        }

        storage
    }

    #[tokio::test]
    async fn select_star() {
        let storage = setup_test_data().await;
        let result = execute_bounded("SELECT * FROM \"orders\"", &storage).await.unwrap();
        assert_eq!(result.rows.len(), 5);
    }

    #[tokio::test]
    async fn select_with_limit() {
        let storage = setup_test_data().await;
        let result = execute_bounded("SELECT * FROM \"orders\" LIMIT 2", &storage).await.unwrap();
        assert_eq!(result.rows.len(), 2);
    }

    #[tokio::test]
    async fn select_with_where() {
        let storage = setup_test_data().await;
        let result = execute_bounded(
            "SELECT * FROM \"orders\" WHERE payload->>'region' = 'eu'",
            &storage,
        )
        .await
        .unwrap();
        assert_eq!(result.rows.len(), 3); // indices 0, 2, 4
    }

    #[tokio::test]
    async fn select_with_json_access() {
        let storage = setup_test_data().await;
        let result = execute_bounded(
            "SELECT payload->>'total' AS total FROM \"orders\" LIMIT 1",
            &storage,
        )
        .await
        .unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.columns, vec!["total"]);
    }

    #[tokio::test]
    async fn select_with_aggregate() {
        let storage = setup_test_data().await;
        let result = execute_bounded("SELECT COUNT(*) AS cnt FROM \"orders\"", &storage).await.unwrap();
        assert_eq!(result.rows.len(), 1);
        // count should be 5
        assert_eq!(result.rows[0].get("cnt"), Some(&Value::Int(5)));
    }

    #[tokio::test]
    async fn select_with_order_by() {
        let storage = setup_test_data().await;
        let result = execute_bounded(
            "SELECT key FROM \"orders\" ORDER BY key DESC LIMIT 2",
            &storage,
        )
        .await
        .unwrap();
        assert_eq!(result.rows.len(), 2);
    }

    #[tokio::test]
    async fn select_from_materialized_view() {
        use crate::materialized_view::MaterializedViewRegistry;

        let storage: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
        let mv_registry = MaterializedViewRegistry::new();

        // Register an MV with two pre-populated rows.
        let state = mv_registry.register(
            "mv_orders",
            vec!["region".into(), "total".into()],
            "q-test-01",
        );

        {
            let mut map = state.write().unwrap();
            map.insert(
                "eu".into(),
                Row {
                    columns: vec!["region".into(), "total".into()],
                    values: vec![Value::Text("eu".into()), Value::Int(300)],
                },
            );
            map.insert(
                "us".into(),
                Row {
                    columns: vec!["region".into(), "total".into()],
                    values: vec![Value::Text("us".into()), Value::Int(200)],
                },
            );
        }

        let result = execute_bounded_with_mv(
            "SELECT * FROM \"mv_orders\"",
            &storage,
            None,
            Some(&mv_registry),
            &[],
        )
        .await
        .unwrap();

        assert_eq!(result.rows.len(), 2);

        // Verify both expected regions are present.
        let regions: Vec<String> = result
            .rows
            .iter()
            .filter_map(|r| r.get("region"))
            .map(|v| match v {
                Value::Text(s) => s.clone(),
                _ => panic!("expected text value for region"),
            })
            .collect();
        assert!(regions.contains(&"eu".to_string()));
        assert!(regions.contains(&"us".to_string()));
    }

    #[tokio::test]
    async fn select_from_mv_with_where() {
        use crate::materialized_view::MaterializedViewRegistry;

        let storage: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
        let mv_registry = MaterializedViewRegistry::new();

        let state = mv_registry.register(
            "mv_sales",
            vec!["region".into(), "total".into()],
            "q-test-02",
        );

        {
            let mut map = state.write().unwrap();
            map.insert(
                "eu".into(),
                Row {
                    columns: vec!["region".into(), "total".into()],
                    values: vec![Value::Text("eu".into()), Value::Int(300)],
                },
            );
            map.insert(
                "us".into(),
                Row {
                    columns: vec!["region".into(), "total".into()],
                    values: vec![Value::Text("us".into()), Value::Int(200)],
                },
            );
            map.insert(
                "apac".into(),
                Row {
                    columns: vec!["region".into(), "total".into()],
                    values: vec![Value::Text("apac".into()), Value::Int(500)],
                },
            );
        }

        let result = execute_bounded_with_mv(
            "SELECT * FROM \"mv_sales\" WHERE region = 'eu'",
            &storage,
            None,
            Some(&mv_registry),
            &[],
        )
        .await
        .unwrap();

        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0].get("region"),
            Some(&Value::Text("eu".into()))
        );
    }

    #[tokio::test]
    async fn result_capped_at_max_rows() {
        let storage: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
        storage
            .create_stream(&StreamName::try_from("big").unwrap(), 0, 0)
            .await
            .unwrap();

        for i in 0..15_000u64 {
            let record = Record {
                key: None,
                value: Bytes::from(format!(r#"{{"i":{i}}}"#)),
                subject: "s".into(),
                headers: vec![],
                timestamp_ns: None,
            };
            storage
                .append(&StreamName::try_from("big").unwrap(), &record)
                .await
                .unwrap();
        }

        let result = execute_bounded(r#"SELECT * FROM "big""#, &storage)
            .await
            .unwrap();
        assert!(
            result.rows.len() <= 10_000,
            "should be capped at MAX_RESULT_ROWS, got {}",
            result.rows.len()
        );
    }

    #[tokio::test]
    async fn select_from_mv_with_limit() {
        use crate::materialized_view::MaterializedViewRegistry;

        let storage: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
        let mv_registry = MaterializedViewRegistry::new();

        let state = mv_registry.register(
            "mv_limit_test",
            vec!["id".into(), "val".into()],
            "q-test-03",
        );

        {
            let mut map = state.write().unwrap();
            for i in 0..10i64 {
                map.insert(
                    i.to_string(),
                    Row {
                        columns: vec!["id".into(), "val".into()],
                        values: vec![Value::Int(i), Value::Int(i * 10)],
                    },
                );
            }
        }

        let result = execute_bounded_with_mv(
            "SELECT * FROM \"mv_limit_test\" LIMIT 3",
            &storage,
            None,
            Some(&mv_registry),
            &[],
        )
        .await
        .unwrap();

        assert_eq!(result.rows.len(), 3);
    }

    #[tokio::test]
    async fn select_offset_desc_limit() {
        let storage = setup_test_data().await;
        let result = execute_bounded(
            r#"SELECT offset FROM "orders" ORDER BY offset DESC LIMIT 3"#,
            &storage,
        ).await.unwrap();
        assert_eq!(result.rows.len(), 3);
        let offsets: Vec<i64> = result.rows.iter()
            .filter_map(|r| r.get("offset").and_then(|v| v.to_i64()))
            .collect();
        assert_eq!(offsets, vec![4, 3, 2], "should return last 3 offsets in DESC order");
    }

    #[tokio::test]
    async fn select_offset_asc_limit_no_sort() {
        let storage = setup_test_data().await;
        let result = execute_bounded(
            r#"SELECT offset FROM "orders" ORDER BY offset ASC LIMIT 3"#,
            &storage,
        ).await.unwrap();
        assert_eq!(result.rows.len(), 3);
        let offsets: Vec<i64> = result.rows.iter()
            .filter_map(|r| r.get("offset").and_then(|v| v.to_i64()))
            .collect();
        assert_eq!(offsets, vec![0, 1, 2]);
    }
}
