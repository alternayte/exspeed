use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use exspeed_common::{Offset, StreamName};
use exspeed_streams::{Record, StorageEngine, StoredRecord};
use tokio::sync::oneshot;
use tracing::{error, info};

use crate::parser::ast::*;
use crate::planner::physical::PhysicalPlan;
use crate::query_registry::QueryRegistry;
use crate::runtime::eval::eval_expr;
use crate::types::{Row, Value};

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Run a continuous query as a long-lived async task.
///
/// The function parses the SQL, sets up the pipeline, and enters a main loop
/// that reads new records from the source stream, pushes them through
/// filter/project/join operators, and appends results to the target stream.
///
/// The loop exits when a cancel signal is received via `cancel_rx`.
pub async fn run_continuous_query(
    query_id: String,
    sql: String,
    target_stream: String,
    storage: Arc<dyn StorageEngine>,
    registry: Arc<QueryRegistry>,
    mut cancel_rx: oneshot::Receiver<()>,
) {
    if let Err(e) = run_inner(
        &query_id,
        &sql,
        &target_stream,
        &storage,
        &registry,
        &mut cancel_rx,
    )
    .await
    {
        error!("continuous query '{}' failed: {}", query_id, e);
        registry.set_failed(&query_id, e);
    }
}

async fn run_inner(
    query_id: &str,
    sql: &str,
    target_stream: &str,
    storage: &Arc<dyn StorageEngine>,
    registry: &Arc<QueryRegistry>,
    cancel_rx: &mut oneshot::Receiver<()>,
) -> Result<(), String> {
    // 1. Parse SQL, extract QueryExpr from CreateStream
    let stmt = crate::parser::parse(sql).map_err(|e| format!("parse error: {e}"))?;
    let query = match stmt {
        ExqlStatement::CreateStream { query, .. } => query,
        ExqlStatement::Query(q) => q,
        _ => return Err("continuous queries require CREATE VIEW or SELECT statement".into()),
    };

    // 2. Plan the query
    let plan = crate::planner::plan(&query).map_err(|e| format!("plan error: {e}"))?;

    // 3. Extract plan components
    let pipeline = decompose_plan(&plan)?;

    // 4. Load checkpoint
    let checkpointed_offsets = registry.load_checkpoint(query_id).unwrap_or_default();

    let source_stream_name = StreamName::try_from(pipeline.source_stream.as_str())
        .map_err(|e| format!("invalid source stream name: {e}"))?;

    let mut current_offset = checkpointed_offsets
        .get(&pipeline.source_stream)
        .copied()
        .unwrap_or(0);

    // 5. Build join lookup if needed
    let join_lookup = if let Some(ref join_info) = pipeline.join {
        let start = Instant::now();
        let right_stream_name = StreamName::try_from(join_info.right_stream.as_str())
            .map_err(|e| format!("invalid join stream name: {e}"))?;

        let lookup = build_join_lookup(
            storage,
            &right_stream_name,
            &join_info.right_key_expr,
            join_info.right_alias.as_deref(),
        )?;

        let elapsed = start.elapsed().as_secs_f64();
        info!(
            "query '{}' rebuilding join lookup from '{}': {} records in {:.3}s",
            query_id,
            join_info.right_stream,
            lookup.values().map(|v| v.len()).sum::<usize>(),
            elapsed,
        );

        Some((lookup, join_info.clone()))
    } else {
        None
    };

    // 6. Ensure target stream exists
    let target_stream_name = StreamName::try_from(target_stream)
        .map_err(|e| format!("invalid target stream name: {e}"))?;
    // Ignore StreamAlreadyExists error
    let _ = storage.create_stream(&target_stream_name, 0, 0);

    // 7. Main loop
    let batch_size = 1_000;
    let mut last_checkpoint = Instant::now();

    loop {
        // Check cancel
        if cancel_rx.try_recv().is_ok() {
            info!("continuous query '{}' cancelled", query_id);
            // Final checkpoint
            let mut offsets = HashMap::new();
            offsets.insert(pipeline.source_stream.clone(), current_offset);
            let _ = registry.checkpoint(query_id, offsets);
            return Ok(());
        }

        // Read batch
        let batch = storage
            .read(&source_stream_name, Offset(current_offset), batch_size)
            .map_err(|e| format!("read error: {e}"))?;

        if batch.is_empty() {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;

            // Still checkpoint periodically even if idle
            if last_checkpoint.elapsed().as_secs() >= 10 {
                let mut offsets = HashMap::new();
                offsets.insert(pipeline.source_stream.clone(), current_offset);
                let _ = registry.checkpoint(query_id, offsets);
                last_checkpoint = Instant::now();
            }

            continue;
        }

        let last_offset = batch.last().unwrap().offset.0;

        for record in &batch {
            let mut row = stored_record_to_row(record, pipeline.source_alias.as_deref());

            // Apply join if present
            if let Some((ref lookup, ref join_info)) = join_lookup {
                let left_key = eval_expr(&join_info.left_key_expr, &row).to_string();
                match lookup.get(&left_key) {
                    Some(right_rows) => {
                        // Emit one output row per match
                        for right_row in right_rows {
                            let joined = combine_rows(&row, right_row);
                            let output = apply_filter_and_project(
                                joined,
                                pipeline.filter.as_ref(),
                                &pipeline.project_items,
                            );
                            if let Some(out_row) = output {
                                let rec = row_to_record(&out_row, target_stream);
                                storage
                                    .append(&target_stream_name, &rec)
                                    .map_err(|e| format!("append error: {e}"))?;
                            }
                        }
                        continue;
                    }
                    None => {
                        if matches!(join_info.join_type, JoinType::Left) {
                            let null_right = Row {
                                columns: join_info.right_columns.clone(),
                                values: vec![Value::Null; join_info.right_columns.len()],
                            };
                            row = combine_rows(&row, &null_right);
                        } else {
                            // Inner join: no match = skip
                            continue;
                        }
                    }
                }
            }

            // Apply filter and project
            let output = apply_filter_and_project(
                row,
                pipeline.filter.as_ref(),
                &pipeline.project_items,
            );
            if let Some(out_row) = output {
                let rec = row_to_record(&out_row, target_stream);
                storage
                    .append(&target_stream_name, &rec)
                    .map_err(|e| format!("append error: {e}"))?;
            }
        }

        current_offset = last_offset + 1;

        // Checkpoint every 10 seconds
        if last_checkpoint.elapsed().as_secs() >= 10 {
            let mut offsets = HashMap::new();
            offsets.insert(pipeline.source_stream.clone(), current_offset);
            let _ = registry.checkpoint(query_id, offsets);
            last_checkpoint = Instant::now();
        }
    }
}

// ---------------------------------------------------------------------------
// Pipeline decomposition
// ---------------------------------------------------------------------------

/// Simplified continuous pipeline: single source + optional join + filter + project.
#[derive(Debug, Clone)]
struct ContinuousPipeline {
    source_stream: String,
    source_alias: Option<String>,
    filter: Option<Expr>,
    project_items: Vec<SelectItem>,
    join: Option<JoinInfo>,
}

#[derive(Debug, Clone)]
struct JoinInfo {
    right_stream: String,
    right_alias: Option<String>,
    join_type: JoinType,
    left_key_expr: Expr,
    right_key_expr: Expr,
    right_columns: Vec<String>,
}

/// Walk the physical plan tree and extract the simplified pipeline components.
///
/// For v1, only supports: Project -> [Filter ->] [HashJoin(SeqScan, SeqScan) |] SeqScan.
fn decompose_plan(plan: &PhysicalPlan) -> Result<ContinuousPipeline, String> {
    let mut filter: Option<Expr> = None;
    let mut project_items: Vec<SelectItem> = Vec::new();
    let source_stream: Option<(String, Option<String>)>;
    let mut join: Option<JoinInfo> = None;

    // Walk the plan, peeling off layers.
    let mut current = plan;

    loop {
        match current {
            PhysicalPlan::Project { input, items } => {
                project_items = items.clone();
                current = input;
            }
            PhysicalPlan::Filter { input, predicate } => {
                filter = Some(predicate.clone());
                current = input;
            }
            PhysicalPlan::SeqScan { stream, alias } => {
                source_stream = Some((stream.clone(), alias.clone()));
                break;
            }
            PhysicalPlan::HashJoin {
                left,
                right,
                on,
                join_type,
            } => {
                // Left side = source stream, Right side = lookup stream
                let (left_stream, left_alias) = extract_seq_scan(left)?;
                let (right_stream, right_alias) = extract_seq_scan(right)?;

                let (left_key, right_key) = decompose_on_expr(on);

                // Build placeholder right columns (standard virtual columns)
                let right_prefix = right_alias.as_deref().map(|a| format!("{a}.")).unwrap_or_default();
                let right_columns: Vec<String> = vec![
                    format!("{right_prefix}offset"),
                    format!("{right_prefix}timestamp"),
                    format!("{right_prefix}key"),
                    format!("{right_prefix}subject"),
                    format!("{right_prefix}payload"),
                    format!("{right_prefix}headers"),
                ];

                join = Some(JoinInfo {
                    right_stream,
                    right_alias,
                    join_type: join_type.clone(),
                    left_key_expr: left_key,
                    right_key_expr: right_key,
                    right_columns,
                });

                source_stream = Some((left_stream, left_alias));
                break;
            }
            PhysicalPlan::Limit { input, .. } => {
                // Skip LIMIT for continuous queries (unbounded)
                current = input;
            }
            PhysicalPlan::Sort { input, .. } => {
                // Skip ORDER BY for continuous queries
                current = input;
            }
            _ => {
                return Err(format!(
                    "unsupported plan node for continuous query: {:?}",
                    std::mem::discriminant(current)
                ));
            }
        }
    }

    let (stream, alias) = source_stream.ok_or("no source stream found in plan")?;

    // If project_items is empty, use wildcard
    if project_items.is_empty() {
        project_items = vec![SelectItem {
            expr: Expr::Wildcard { table: None },
            alias: None,
        }];
    }

    Ok(ContinuousPipeline {
        source_stream: stream,
        source_alias: alias,
        filter,
        project_items,
        join,
    })
}

fn extract_seq_scan(plan: &PhysicalPlan) -> Result<(String, Option<String>), String> {
    match plan {
        PhysicalPlan::SeqScan { stream, alias } => Ok((stream.clone(), alias.clone())),
        _ => Err("continuous query joins only support direct stream scans".into()),
    }
}

fn decompose_on_expr(expr: &Expr) -> (Expr, Expr) {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => (*left.clone(), *right.clone()),
        _ => (expr.clone(), expr.clone()),
    }
}

// ---------------------------------------------------------------------------
// Join lookup builder
// ---------------------------------------------------------------------------

fn build_join_lookup(
    storage: &Arc<dyn StorageEngine>,
    stream: &StreamName,
    right_key_expr: &Expr,
    alias: Option<&str>,
) -> Result<HashMap<String, Vec<Row>>, String> {
    let mut lookup: HashMap<String, Vec<Row>> = HashMap::new();
    let batch_size = 10_000;
    let mut from = Offset(0);

    loop {
        let batch = storage
            .read(stream, from, batch_size)
            .map_err(|e| format!("read join stream: {e}"))?;
        if batch.is_empty() {
            break;
        }
        let last_offset = batch.last().unwrap().offset.0;
        for record in &batch {
            let row = stored_record_to_row(record, alias);
            let key = eval_expr(right_key_expr, &row).to_string();
            lookup.entry(key).or_default().push(row);
        }
        from = Offset(last_offset + 1);
    }

    Ok(lookup)
}

// ---------------------------------------------------------------------------
// Row conversion helpers
// ---------------------------------------------------------------------------

/// Convert a StoredRecord into a Row with virtual columns.
/// (Same logic as bounded.rs but pub for continuous use.)
fn stored_record_to_row(record: &StoredRecord, alias: Option<&str>) -> Row {
    let payload_json = serde_json::from_slice::<serde_json::Value>(&record.value)
        .unwrap_or(serde_json::Value::String(
            String::from_utf8_lossy(&record.value).into(),
        ));

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

    if alias.is_some() {
        columns.extend(
            ["offset", "timestamp", "key", "subject", "payload", "headers"]
                .iter()
                .map(|s| s.to_string()),
        );
        values.extend(base_values);
    }

    Row { columns, values }
}

/// Apply filter and project to a single row.
fn apply_filter_and_project(
    row: Row,
    filter: Option<&Expr>,
    project_items: &[SelectItem],
) -> Option<Row> {
    // Filter
    if let Some(predicate) = filter {
        if eval_expr(predicate, &row) != Value::Bool(true) {
            return None;
        }
    }

    // Project
    let mut columns = Vec::new();
    let mut values = Vec::new();

    for item in project_items {
        match &item.expr {
            Expr::Wildcard { table: None } => {
                columns.extend(row.columns.clone());
                values.extend(row.values.clone());
            }
            Expr::Wildcard { table: Some(t) } => {
                let pref = format!("{t}.");
                for (i, col) in row.columns.iter().enumerate() {
                    if col.starts_with(&pref) || !col.contains('.') {
                        columns.push(col.clone());
                        values.push(row.values[i].clone());
                    }
                }
            }
            expr => {
                let name = item.alias.clone().unwrap_or_else(|| derive_column_name(expr));
                let val = eval_expr(expr, &row);
                columns.push(name);
                values.push(val);
            }
        }
    }

    Some(Row { columns, values })
}

/// Convert an output row to a Record for appending to the target stream.
fn row_to_record(row: &Row, target_stream: &str) -> Record {
    let key = row
        .get("key")
        .and_then(|v| v.as_text())
        .map(|s| Bytes::from(s.to_string()));

    let subject = row
        .get("subject")
        .and_then(|v| v.as_text())
        .unwrap_or(target_stream)
        .to_string();

    let mut map = serde_json::Map::new();
    for (col, val) in row.columns.iter().zip(row.values.iter()) {
        map.insert(col.clone(), value_to_json(val));
    }
    let value = Bytes::from(serde_json::to_vec(&serde_json::Value::Object(map)).unwrap());

    Record {
        key,
        value,
        subject,
        headers: vec![],
    }
}

fn value_to_json(val: &Value) -> serde_json::Value {
    match val {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Int(i) => serde_json::json!(*i),
        Value::Float(f) => serde_json::json!(*f),
        Value::Text(s) => serde_json::Value::String(s.clone()),
        Value::Json(j) => j.clone(),
        Value::Timestamp(t) => serde_json::json!(*t),
    }
}

fn derive_column_name(expr: &Expr) -> String {
    match expr {
        Expr::Column { name, .. } => name.clone(),
        Expr::JsonAccess { field, .. } => field.clone(),
        Expr::Function { name, .. } => name.to_lowercase(),
        Expr::Cast { expr, .. } => derive_column_name(expr),
        _ => "?column?".into(),
    }
}

/// Combine a left row and right row into a single joined row.
fn combine_rows(left: &Row, right: &Row) -> Row {
    let mut columns = left.columns.clone();
    columns.extend(right.columns.clone());
    let mut values = left.values.clone();
    values.extend(right.values.clone());
    Row { columns, values }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use exspeed_storage::memory::MemoryStorage;
    use std::sync::Arc;

    fn setup_source_data() -> Arc<dyn StorageEngine> {
        let storage = Arc::new(MemoryStorage::new());
        storage
            .create_stream(&StreamName::try_from("orders").unwrap(), 0, 0)
            .unwrap();

        for i in 0..10 {
            let record = Record {
                key: Some(Bytes::from(format!("ord-{i}"))),
                value: Bytes::from(format!(
                    r#"{{"total": {}, "region": "{}"}}"#,
                    (i + 1) * 100,
                    if i % 2 == 0 { "eu" } else { "us" }
                )),
                subject: format!(
                    "order.{}.created",
                    if i % 2 == 0 { "eu" } else { "us" }
                ),
                headers: vec![],
            };
            storage
                .append(&StreamName::try_from("orders").unwrap(), &record)
                .unwrap();
        }

        storage
    }

    #[tokio::test]
    async fn continuous_query_filter() {
        let storage = setup_source_data();
        let dir = tempfile::tempdir().unwrap();
        let registry = Arc::new(QueryRegistry::new(dir.path().to_path_buf()));

        let query_id = "q_test_filter".to_string();
        let sql = r#"CREATE VIEW eu_orders AS SELECT * FROM "orders" WHERE payload->>'region' = 'eu'"#.to_string();
        let target = "eu_orders".to_string();

        registry.register(&query_id, &sql, &target).unwrap();

        let (cancel_tx, cancel_rx) = oneshot::channel::<()>();
        registry.set_running(&query_id, cancel_tx).unwrap();

        // Spawn the continuous query
        let storage_clone = storage.clone();
        let registry_clone = registry.clone();
        let query_id_clone = query_id.clone();
        let sql_clone = sql.clone();
        let target_clone = target.clone();

        let handle = tokio::spawn(async move {
            run_continuous_query(
                query_id_clone,
                sql_clone,
                target_clone,
                storage_clone,
                registry_clone,
                cancel_rx,
            )
            .await;
        });

        // Wait a bit for the query to process existing records
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // Stop the query
        registry.stop(&query_id).unwrap();
        let _ = handle.await;

        // Verify target stream has records
        let target_name = StreamName::try_from("eu_orders").unwrap();
        let results = storage.read(&target_name, Offset(0), 100).unwrap();

        // 10 records, half are "eu" region (indices 0,2,4,6,8)
        assert_eq!(results.len(), 5, "expected 5 eu records, got {}", results.len());

        // Verify each record has region=eu in the payload
        for r in &results {
            let json: serde_json::Value = serde_json::from_slice(&r.value).unwrap();
            if let Some(payload) = json.get("payload") {
                if let Some(region) = payload.get("region").and_then(|v| v.as_str()) {
                    assert_eq!(region, "eu");
                }
            }
        }
    }

    #[tokio::test]
    async fn continuous_query_project() {
        let storage = setup_source_data();
        let dir = tempfile::tempdir().unwrap();
        let registry = Arc::new(QueryRegistry::new(dir.path().to_path_buf()));

        let query_id = "q_test_project".to_string();
        let sql = r#"CREATE VIEW order_totals AS SELECT key, payload->>'total' AS total FROM "orders""#.to_string();
        let target = "order_totals".to_string();

        registry.register(&query_id, &sql, &target).unwrap();

        let (cancel_tx, cancel_rx) = oneshot::channel::<()>();
        registry.set_running(&query_id, cancel_tx).unwrap();

        let storage_clone = storage.clone();
        let registry_clone = registry.clone();
        let query_id_clone = query_id.clone();
        let sql_clone = sql.clone();
        let target_clone = target.clone();

        let handle = tokio::spawn(async move {
            run_continuous_query(
                query_id_clone,
                sql_clone,
                target_clone,
                storage_clone,
                registry_clone,
                cancel_rx,
            )
            .await;
        });

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        registry.stop(&query_id).unwrap();
        let _ = handle.await;

        let target_name = StreamName::try_from("order_totals").unwrap();
        let results = storage.read(&target_name, Offset(0), 100).unwrap();

        assert_eq!(results.len(), 10);

        // Verify first record has key and total columns
        let first: serde_json::Value = serde_json::from_slice(&results[0].value).unwrap();
        assert!(first.get("key").is_some(), "expected 'key' column in output");
        assert!(first.get("total").is_some(), "expected 'total' column in output");
    }

    #[tokio::test]
    async fn continuous_query_checkpoint() {
        let storage = setup_source_data();
        let dir = tempfile::tempdir().unwrap();
        let registry = Arc::new(QueryRegistry::new(dir.path().to_path_buf()));

        let query_id = "q_test_ckpt".to_string();
        let sql = r#"CREATE VIEW out AS SELECT * FROM "orders""#.to_string();
        let target = "out".to_string();

        registry.register(&query_id, &sql, &target).unwrap();

        let (cancel_tx, cancel_rx) = oneshot::channel::<()>();
        registry.set_running(&query_id, cancel_tx).unwrap();

        let storage_clone = storage.clone();
        let registry_clone = registry.clone();

        let handle = tokio::spawn(async move {
            run_continuous_query(
                query_id.clone(),
                sql,
                target,
                storage_clone,
                registry_clone,
                cancel_rx,
            )
            .await;
        });

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // Stop triggers a final checkpoint
        registry.stop("q_test_ckpt").unwrap();
        let _ = handle.await;

        // Verify checkpoint was written
        let ckpt = registry.load_checkpoint("q_test_ckpt");
        assert!(ckpt.is_some(), "checkpoint should have been saved");
        let offsets = ckpt.unwrap();
        assert!(
            offsets.get("orders").copied().unwrap_or(0) >= 10,
            "checkpoint offset should be >= 10, got {:?}",
            offsets.get("orders")
        );
    }
}
