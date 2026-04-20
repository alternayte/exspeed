use std::sync::Arc;
use std::time::Instant;

use exspeed_common::{Offset, StreamName};
use exspeed_streams::{StorageEngine, StoredRecord};

/// Boxed future returned by the recursive `build_operator` helper.
type BuildOperatorFuture<'a> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<Box<dyn Operator>, ExqlError>> + Send + 'a>>;

use crate::error::ExqlError;
use crate::external::connections::ConnectionRegistry;
use crate::materialized_view::MaterializedViewRegistry;
use crate::parser::ast::ExqlStatement;
use crate::planner::physical::PhysicalPlan;
use crate::runtime::operators::aggregate::AggregateOperator;
use crate::runtime::operators::filter::FilterOperator;
use crate::runtime::operators::join::HashJoinOperator;
use crate::runtime::operators::limit::LimitOperator;
use crate::runtime::operators::project::ProjectOperator;
use crate::runtime::operators::scan::ScanOperator;
use crate::runtime::operators::sort::SortOperator;
use crate::runtime::operators::Operator;
use crate::types::{ResultSet, Row, Value};

/// Execute a bounded (batch) SQL query against storage, returning a ResultSet.
pub async fn execute_bounded(
    sql: &str,
    storage: &Arc<dyn StorageEngine>,
) -> Result<ResultSet, ExqlError> {
    execute_bounded_with_mv(sql, storage, None, None).await
}

/// Execute a bounded (batch) SQL query with optional external connection support.
pub async fn execute_bounded_with_connections(
    sql: &str,
    storage: &Arc<dyn StorageEngine>,
    connections: Option<&ConnectionRegistry>,
) -> Result<ResultSet, ExqlError> {
    execute_bounded_with_mv(sql, storage, connections, None).await
}

/// Execute a bounded (batch) SQL query with optional connection and MV registry support.
pub async fn execute_bounded_with_mv(
    sql: &str,
    storage: &Arc<dyn StorageEngine>,
    connections: Option<&ConnectionRegistry>,
    mv_registry: Option<&MaterializedViewRegistry>,
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
    let mut root = build_operator(&plan, storage, connections, mv_registry).await?;

    // 5. Pull all rows from the root operator
    let columns = root.columns();
    let mut rows = Vec::new();
    while let Some(row) = root.next() {
        rows.push(row);
    }

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
) -> BuildOperatorFuture<'a> {
    Box::pin(async move {
    match plan {
        PhysicalPlan::SeqScan { stream, alias } => {
            // Check materialized views first
            if let Some(mv_reg) = mv_registry {
                if let Some((_columns, rows)) = mv_reg.get_rows(stream) {
                    return Ok(Box::new(ScanOperator::new(rows)) as Box<dyn Operator>);
                }
            }

            let stream_name = StreamName::try_from(stream.as_str()).map_err(|e| {
                ExqlError::Storage(format!("invalid stream name '{}': {}", stream, e))
            })?;

            // Load all records from storage. We read in batches to avoid
            // requesting an absurdly large single read, but still load
            // everything for v1.
            let mut all_records: Vec<StoredRecord> = Vec::new();
            let batch_size = 10_000;
            let mut from = Offset(0);
            loop {
                let batch = storage
                    .read(&stream_name, from, batch_size)
                    .await
                    .map_err(|e| ExqlError::Storage(e.to_string()))?;
                if batch.is_empty() {
                    break;
                }
                let last_offset = batch.last().unwrap().offset.0;
                all_records.extend(batch);
                from = Offset(last_offset + 1);
            }

            // Convert StoredRecord → Row
            let rows: Vec<Row> = all_records
                .iter()
                .map(|r| stored_record_to_row(r, alias.as_deref()))
                .collect();

            Ok(Box::new(ScanOperator::new(rows)) as Box<dyn Operator>)
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
            let child = build_operator(input, storage, connections, mv_registry).await?;
            Ok(Box::new(FilterOperator::new(child, predicate.clone())) as Box<dyn Operator>)
        }

        PhysicalPlan::Project { input, items } => {
            let child = build_operator(input, storage, connections, mv_registry).await?;
            Ok(Box::new(ProjectOperator::new(child, items.clone())) as Box<dyn Operator>)
        }

        PhysicalPlan::HashJoin {
            left,
            right,
            on,
            join_type,
        } => {
            let left_op = build_operator(left, storage, connections, mv_registry).await?;
            let right_op = build_operator(right, storage, connections, mv_registry).await?;
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
            let child = build_operator(input, storage, connections, mv_registry).await?;
            Ok(Box::new(AggregateOperator::new(
                child,
                group_by.clone(),
                select_items.clone(),
            )) as Box<dyn Operator>)
        }

        PhysicalPlan::Sort { input, order_by } => {
            let child = build_operator(input, storage, connections, mv_registry).await?;
            Ok(Box::new(SortOperator::new(child, order_by.clone())) as Box<dyn Operator>)
        }

        PhysicalPlan::Limit {
            input,
            limit,
            offset,
        } => {
            let child = build_operator(input, storage, connections, mv_registry).await?;
            Ok(Box::new(LimitOperator::new(
                child,
                *limit,
                offset.unwrap_or(0),
            )) as Box<dyn Operator>)
        }

        PhysicalPlan::WindowedAggregate { .. } => Err(ExqlError::Execution(
            "WindowedAggregate is not supported in bounded execution".into(),
        )),

        PhysicalPlan::StreamStreamJoin { .. } => Err(ExqlError::Execution(
            "StreamStreamJoin is not supported in bounded execution".into(),
        )),
    }
    })
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

/// Convert a StoredRecord into a Row with virtual columns.
fn stored_record_to_row(record: &StoredRecord, alias: Option<&str>) -> Row {
    let payload_json = serde_json::from_slice::<serde_json::Value>(&record.value).unwrap_or(
        serde_json::Value::String(String::from_utf8_lossy(&record.value).into()),
    );

    let headers_json = serde_json::Value::Object(
        record
            .headers
            .iter()
            .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
            .collect(),
    );

    let prefix = alias.map(|a| format!("{a}.")).unwrap_or_default();

    let base_columns = vec![
        format!("{prefix}offset"),
        format!("{prefix}timestamp"),
        format!("{prefix}key"),
        format!("{prefix}subject"),
        format!("{prefix}payload"),
        format!("{prefix}headers"),
    ];

    let base_values = vec![
        Value::Int(record.offset.0 as i64),
        Value::Timestamp(record.timestamp),
        record
            .key
            .as_ref()
            .map(|k| Value::Text(String::from_utf8_lossy(k).to_string()))
            .unwrap_or(Value::Null),
        Value::Text(record.subject.clone()),
        Value::Json(payload_json),
        Value::Json(headers_json),
    ];

    let mut columns = base_columns;
    let mut values = base_values.clone();

    // Add unprefixed columns when an alias is present so that both
    // `alias.key` and `key` resolve correctly.
    if alias.is_some() {
        columns.extend(
            [
                "offset",
                "timestamp",
                "key",
                "subject",
                "payload",
                "headers",
            ]
            .iter()
            .map(|s| s.to_string()),
        );
        values.extend(base_values);
    }

    Row { columns, values }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
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
        )
        .await
        .unwrap();

        assert_eq!(result.rows.len(), 3);
    }
}
