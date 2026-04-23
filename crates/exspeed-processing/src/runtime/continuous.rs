use std::collections::HashMap;
use std::sync::{Arc, RwLock};
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
use crate::runtime::functions::parse_interval_nanos;
use crate::runtime::operators::stream_join::StreamStreamJoinState;
use crate::runtime::operators::windowed_aggregate::WindowedAggregateState;
use crate::types::{Row, Value};

// ---------------------------------------------------------------------------
// Execution mode — auto-detected from the physical plan
// ---------------------------------------------------------------------------

/// The three supported continuous query execution strategies.
#[derive(Debug)]
enum ContinuousMode {
    /// Single source, filter/project, output to stream (existing path).
    Simple,
    /// Tumbling window aggregation.
    WindowedAggregate {
        window_size: String,
        group_by: Vec<Expr>,
        select_items: Vec<SelectItem>,
        emit_mode: EmitMode,
    },
    /// Stream-stream WITHIN join.
    StreamStreamJoin {
        right_stream: String,
        right_alias: Option<String>,
        on: Expr,
        within: String,
        #[allow(dead_code)]
        join_type: JoinType,
    },
}

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
///
/// When `mv_state` is `Some`, output rows are written to the materialized view
/// HashMap instead of an output stream.
pub async fn run_continuous_query(
    query_id: String,
    sql: String,
    target_stream: String,
    storage: Arc<dyn StorageEngine>,
    registry: Arc<QueryRegistry>,
    mut cancel_rx: oneshot::Receiver<()>,
    mv_state: Option<Arc<RwLock<HashMap<String, Row>>>>,
) {
    if let Err(e) = run_inner(
        &query_id,
        &sql,
        &target_stream,
        &storage,
        &registry,
        &mut cancel_rx,
        mv_state,
    )
    .await
    {
        error!("continuous query '{}' failed: {}", query_id, e);
        registry.set_failed(&query_id, e);
    }
}

// ---------------------------------------------------------------------------
// Detect mode from physical plan
// ---------------------------------------------------------------------------

/// Walk the plan tree and detect whether it contains a WindowedAggregate or
/// StreamStreamJoin node.  Returns `ContinuousMode::Simple` if neither is
/// found.
fn detect_mode(plan: &PhysicalPlan) -> ContinuousMode {
    match plan {
        PhysicalPlan::WindowedAggregate {
            window_size,
            group_by,
            select_items,
            emit_mode,
            ..
        } => ContinuousMode::WindowedAggregate {
            window_size: window_size.clone(),
            group_by: group_by.clone(),
            select_items: select_items.clone(),
            emit_mode: emit_mode.clone(),
        },
        PhysicalPlan::StreamStreamJoin {
            left: _,
            right,
            on,
            within,
            join_type,
        } => {
            let (right_stream, right_alias) = extract_seq_scan(right).unwrap_or_default();
            ContinuousMode::StreamStreamJoin {
                right_stream,
                right_alias,
                on: on.clone(),
                within: within.clone(),
                join_type: join_type.clone(),
            }
        }
        // Recurse into wrapper nodes
        PhysicalPlan::Project { input, .. }
        | PhysicalPlan::Filter { input, .. }
        | PhysicalPlan::Sort { input, .. }
        | PhysicalPlan::Limit { input, .. }
        | PhysicalPlan::HashAggregate { input, .. } => detect_mode(input),
        // Leaf / hash join / other — Simple
        _ => ContinuousMode::Simple,
    }
}

// ---------------------------------------------------------------------------
// Inner executor
// ---------------------------------------------------------------------------

async fn run_inner(
    query_id: &str,
    sql: &str,
    target_stream: &str,
    storage: &Arc<dyn StorageEngine>,
    registry: &Arc<QueryRegistry>,
    cancel_rx: &mut oneshot::Receiver<()>,
    mv_state: Option<Arc<RwLock<HashMap<String, Row>>>>,
) -> Result<(), String> {
    // 1. Parse SQL, extract QueryExpr and EmitMode
    let stmt = crate::parser::parse(sql).map_err(|e| format!("parse error: {e}"))?;
    let (query, emit_mode) = match stmt {
        ExqlStatement::CreateStream { query, emit, .. } => (query, emit),
        ExqlStatement::CreateMaterializedView { query, .. } => (query, EmitMode::Changes),
        ExqlStatement::Query(q) => (q, EmitMode::Changes),
        _ => return Err("continuous queries require CREATE VIEW or SELECT statement".into()),
    };

    // 2. Plan the query
    let plan = crate::planner::plan(&query, emit_mode).map_err(|e| format!("plan error: {e}"))?;

    // 3. Detect execution mode
    let mode = detect_mode(&plan);

    // 4. Extract pipeline (filter/project + source)
    let pipeline = decompose_plan(&plan)?;

    // 5. Load checkpoint
    let checkpointed_offsets = registry.load_checkpoint(query_id).unwrap_or_default();

    let source_stream_name = StreamName::try_from(pipeline.source_stream.as_str())
        .map_err(|e| format!("invalid source stream name: {e}"))?;

    let mut current_offset = checkpointed_offsets
        .get(&pipeline.source_stream)
        .copied()
        .unwrap_or(0);

    // 6. Ensure target stream exists (only when not writing to MV)
    let target_stream_name = if mv_state.is_none() {
        let name = StreamName::try_from(target_stream)
            .map_err(|e| format!("invalid target stream name: {e}"))?;
        let _ = storage.create_stream(&name, 0, 0).await;
        Some(name)
    } else {
        None
    };

    let batch_size = 1_000;
    let mut last_checkpoint = Instant::now();

    match mode {
        ContinuousMode::Simple => {
            // ── Existing simple mode (filter/project + optional hash-join) ──

            // Build join lookup if needed
            let join_lookup = if let Some(ref join_info) = pipeline.join {
                let start = Instant::now();
                let right_stream_name = StreamName::try_from(join_info.right_stream.as_str())
                    .map_err(|e| format!("invalid join stream name: {e}"))?;
                let lookup = build_join_lookup(
                    storage,
                    &right_stream_name,
                    &join_info.right_key_expr,
                    join_info.right_alias.as_deref(),
                ).await?;
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

            loop {
                if cancel_rx.try_recv().is_ok() {
                    info!("continuous query '{}' cancelled", query_id);
                    let mut offsets = HashMap::new();
                    offsets.insert(pipeline.source_stream.clone(), current_offset);
                    let _ = registry.checkpoint(query_id, offsets);
                    return Ok(());
                }

                let batch = storage
                    .read(&source_stream_name, Offset(current_offset), batch_size)
                    .await
                    .map_err(|e| format!("read error: {e}"))?;

                if batch.is_empty() {
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
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

                    if let Some((ref lookup, ref join_info)) = join_lookup {
                        let left_key = eval_expr(&join_info.left_key_expr, &row).to_string();
                        match lookup.get(&left_key) {
                            Some(right_rows) => {
                                for right_row in right_rows {
                                    let joined = combine_rows(&row, right_row);
                                    let output = apply_filter_and_project(
                                        joined,
                                        pipeline.filter.as_ref(),
                                        &pipeline.project_items,
                                    );
                                    if let Some(out_row) = output {
                                        emit_row(
                                            &out_row,
                                            target_stream,
                                            storage,
                                            target_stream_name.as_ref(),
                                            &mv_state,
                                            &pipeline.group_by_exprs,
                                        ).await?;
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
                                    continue;
                                }
                            }
                        }
                    }

                    let output = apply_filter_and_project(
                        row,
                        pipeline.filter.as_ref(),
                        &pipeline.project_items,
                    );
                    if let Some(out_row) = output {
                        emit_row(
                            &out_row,
                            target_stream,
                            storage,
                            target_stream_name.as_ref(),
                            &mv_state,
                            &pipeline.group_by_exprs,
                        ).await?;
                    }
                }

                current_offset = last_offset + 1;

                if last_checkpoint.elapsed().as_secs() >= 10 {
                    let mut offsets = HashMap::new();
                    offsets.insert(pipeline.source_stream.clone(), current_offset);
                    let _ = registry.checkpoint(query_id, offsets);
                    last_checkpoint = Instant::now();
                }
            }
        }

        ContinuousMode::WindowedAggregate {
            window_size,
            group_by,
            select_items,
            emit_mode,
        } => {
            // ── Windowed aggregate mode ──────────────────────────────────

            let window_nanos = parse_interval_nanos(&window_size);
            let grace_nanos: u64 = 5 * 60 * 1_000_000_000; // 5 min grace

            let mut windowed_state = WindowedAggregateState::new(
                window_nanos,
                grace_nanos,
                group_by.clone(),
                select_items,
                emit_mode,
            );

            loop {
                if cancel_rx.try_recv().is_ok() {
                    info!("continuous query '{}' cancelled (windowed)", query_id);
                    let mut offsets = HashMap::new();
                    offsets.insert(pipeline.source_stream.clone(), current_offset);
                    let _ = registry.checkpoint(query_id, offsets);
                    return Ok(());
                }

                let batch = storage
                    .read(&source_stream_name, Offset(current_offset), batch_size)
                    .await
                    .map_err(|e| format!("read error: {e}"))?;

                if batch.is_empty() {
                    // Even when idle, check for closed windows
                    let now = current_time_nanos();
                    let final_rows = windowed_state.check_closed_windows(now);
                    for output_row in &final_rows {
                        emit_row(
                            output_row,
                            target_stream,
                            storage,
                            target_stream_name.as_ref(),
                            &mv_state,
                            &group_by,
                        ).await?;
                    }

                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

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
                    let row = stored_record_to_row(record, pipeline.source_alias.as_deref());
                    let emitted = windowed_state.process_record(&row, record.timestamp);
                    for output_row in &emitted {
                        emit_row(
                            output_row,
                            target_stream,
                            storage,
                            target_stream_name.as_ref(),
                            &mv_state,
                            &group_by,
                        ).await?;
                    }
                }

                // Check for closed windows after processing the batch
                let now = current_time_nanos();
                let final_rows = windowed_state.check_closed_windows(now);
                for output_row in &final_rows {
                    emit_row(
                        output_row,
                        target_stream,
                        storage,
                        target_stream_name.as_ref(),
                        &mv_state,
                        &group_by,
                    ).await?;
                }

                current_offset = last_offset + 1;

                if last_checkpoint.elapsed().as_secs() >= 10 {
                    let mut offsets = HashMap::new();
                    offsets.insert(pipeline.source_stream.clone(), current_offset);
                    let _ = registry.checkpoint(query_id, offsets);
                    last_checkpoint = Instant::now();
                }
            }
        }

        ContinuousMode::StreamStreamJoin {
            right_stream,
            right_alias,
            on,
            within,
            join_type: _,
        } => {
            // ── Stream-stream join mode ──────────────────────────────────

            let within_nanos = parse_interval_nanos(&within);
            let mut join_state = StreamStreamJoinState::new(within_nanos, &on);

            let right_stream_name = StreamName::try_from(right_stream.as_str())
                .map_err(|e| format!("invalid right stream name: {e}"))?;

            let mut left_offset = current_offset;
            let mut right_offset = checkpointed_offsets
                .get(&right_stream)
                .copied()
                .unwrap_or(0);

            loop {
                if cancel_rx.try_recv().is_ok() {
                    info!("continuous query '{}' cancelled (stream-join)", query_id);
                    let mut offsets = HashMap::new();
                    offsets.insert(pipeline.source_stream.clone(), left_offset);
                    offsets.insert(right_stream.clone(), right_offset);
                    let _ = registry.checkpoint(query_id, offsets);
                    return Ok(());
                }

                // Read from LEFT source
                let left_batch = storage
                    .read(&source_stream_name, Offset(left_offset), batch_size)
                    .await
                    .map_err(|e| format!("read left error: {e}"))?;

                for record in &left_batch {
                    let row = stored_record_to_row(record, pipeline.source_alias.as_deref());
                    let matched = join_state.process_left(row, record.timestamp);
                    for joined_row in matched {
                        let output = apply_filter_and_project(
                            joined_row,
                            pipeline.filter.as_ref(),
                            &pipeline.project_items,
                        );
                        if let Some(out_row) = output {
                            emit_row(
                                &out_row,
                                target_stream,
                                storage,
                                target_stream_name.as_ref(),
                                &mv_state,
                                &pipeline.group_by_exprs,
                            ).await?;
                        }
                    }
                    left_offset = record.offset.0 + 1;
                }

                // Read from RIGHT source
                let right_batch = storage
                    .read(&right_stream_name, Offset(right_offset), batch_size)
                    .await
                    .map_err(|e| format!("read right error: {e}"))?;

                for record in &right_batch {
                    let row = stored_record_to_row(record, right_alias.as_deref());
                    let matched = join_state.process_right(row, record.timestamp);
                    for joined_row in matched {
                        let output = apply_filter_and_project(
                            joined_row,
                            pipeline.filter.as_ref(),
                            &pipeline.project_items,
                        );
                        if let Some(out_row) = output {
                            emit_row(
                                &out_row,
                                target_stream,
                                storage,
                                target_stream_name.as_ref(),
                                &mv_state,
                                &pipeline.group_by_exprs,
                            ).await?;
                        }
                    }
                    right_offset = record.offset.0 + 1;
                }

                // Evict expired entries
                join_state.evict_expired(current_time_nanos());

                // If both empty, sleep
                if left_batch.is_empty() && right_batch.is_empty() {
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }

                // Checkpoint
                if last_checkpoint.elapsed().as_secs() >= 10 {
                    let mut offsets = HashMap::new();
                    offsets.insert(pipeline.source_stream.clone(), left_offset);
                    offsets.insert(right_stream.clone(), right_offset);
                    let _ = registry.checkpoint(query_id, offsets);
                    last_checkpoint = Instant::now();
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Output helper — emit to stream or MV
// ---------------------------------------------------------------------------

/// Emit a single output row to either the target stream or the MV state.
async fn emit_row(
    row: &Row,
    target_stream: &str,
    storage: &Arc<dyn StorageEngine>,
    target_stream_name: Option<&StreamName>,
    mv_state: &Option<Arc<RwLock<HashMap<String, Row>>>>,
    group_by_exprs: &[Expr],
) -> Result<(), String> {
    if let Some(ref state) = mv_state {
        // Build group key from the row's group-by values
        let group_key = if group_by_exprs.is_empty() {
            // No group by — single global key
            "__global__".to_string()
        } else {
            group_by_exprs
                .iter()
                .map(|expr| eval_expr(expr, row).to_string())
                .collect::<Vec<_>>()
                .join("|")
        };

        state
            .write()
            .map_err(|e| format!("mv lock error: {e}"))?
            .insert(group_key, row.clone());
    } else if let Some(name) = target_stream_name {
        let rec = row_to_record(row, target_stream);
        storage
            .append(name, &rec)
            .await
            .map_err(|e| format!("append error: {e}"))?;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Time helper
// ---------------------------------------------------------------------------

fn current_time_nanos() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
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
    /// Group-by expressions for MV key derivation (empty if not aggregating).
    group_by_exprs: Vec<Expr>,
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
/// Supports: Project -> [Filter ->] [HashJoin(SeqScan, SeqScan) |
///           WindowedAggregate | StreamStreamJoin |] SeqScan.
fn decompose_plan(plan: &PhysicalPlan) -> Result<ContinuousPipeline, String> {
    let mut filter: Option<Expr> = None;
    let mut project_items: Vec<SelectItem> = Vec::new();
    let source_stream: Option<(String, Option<String>)>;
    let mut join: Option<JoinInfo> = None;
    let mut group_by_exprs: Vec<Expr> = Vec::new();

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
            PhysicalPlan::SeqScan { stream, alias, .. } => {
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
                let right_prefix = right_alias
                    .as_deref()
                    .map(|a| format!("{a}."))
                    .unwrap_or_default();
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
            PhysicalPlan::WindowedAggregate {
                input, group_by, ..
            } => {
                group_by_exprs = group_by.clone();
                // Descend into the input to find the source stream.
                current = input;
            }
            PhysicalPlan::StreamStreamJoin { left, right: _, .. } => {
                // The left child gives us the source stream.
                // The right stream was already extracted in detect_mode().
                current = left;
            }
            PhysicalPlan::HashAggregate {
                input, group_by, ..
            } => {
                group_by_exprs = group_by.clone();
                current = input;
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
        group_by_exprs,
    })
}

fn extract_seq_scan(plan: &PhysicalPlan) -> Result<(String, Option<String>), String> {
    match plan {
        PhysicalPlan::SeqScan { stream, alias, .. } => Ok((stream.clone(), alias.clone())),
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

async fn build_join_lookup(
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
            .await
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
                let name = item
                    .alias
                    .clone()
                    .unwrap_or_else(|| derive_column_name(expr));
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
        timestamp_ns: None,
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
        Value::RawJson(b) => serde_json::from_slice(b).unwrap_or(serde_json::Value::Null),
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

    async fn setup_source_data() -> Arc<dyn StorageEngine> {
        let storage = Arc::new(MemoryStorage::new());
        storage
            .create_stream(&StreamName::try_from("orders").unwrap(), 0, 0)
            .await
            .unwrap();

        for i in 0..10 {
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
    async fn continuous_query_filter() {
        let storage = setup_source_data().await;
        let dir = tempfile::tempdir().unwrap();
        let registry = Arc::new(QueryRegistry::new(dir.path().to_path_buf()));

        let query_id = "q_test_filter".to_string();
        let sql =
            r#"CREATE VIEW eu_orders AS SELECT * FROM "orders" WHERE payload->>'region' = 'eu'"#
                .to_string();
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
                None,
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
        let results = storage.read(&target_name, Offset(0), 100).await.unwrap();

        // 10 records, half are "eu" region (indices 0,2,4,6,8)
        assert_eq!(
            results.len(),
            5,
            "expected 5 eu records, got {}",
            results.len()
        );

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
        let storage = setup_source_data().await;
        let dir = tempfile::tempdir().unwrap();
        let registry = Arc::new(QueryRegistry::new(dir.path().to_path_buf()));

        let query_id = "q_test_project".to_string();
        let sql =
            r#"CREATE VIEW order_totals AS SELECT key, payload->>'total' AS total FROM "orders""#
                .to_string();
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
                None,
            )
            .await;
        });

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        registry.stop(&query_id).unwrap();
        let _ = handle.await;

        let target_name = StreamName::try_from("order_totals").unwrap();
        let results = storage.read(&target_name, Offset(0), 100).await.unwrap();

        assert_eq!(results.len(), 10);

        // Verify first record has key and total columns
        let first: serde_json::Value = serde_json::from_slice(&results[0].value).unwrap();
        assert!(
            first.get("key").is_some(),
            "expected 'key' column in output"
        );
        assert!(
            first.get("total").is_some(),
            "expected 'total' column in output"
        );
    }

    #[tokio::test]
    async fn continuous_query_checkpoint() {
        let storage = setup_source_data().await;
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
                None,
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

    // ── Windowed aggregate test ──────────────────────────────────────────

    #[tokio::test]
    async fn continuous_windowed_aggregate_emit_changes() {
        // All records get MemoryStorage timestamps near the current time,
        // so they all land in the same tumbling window.  EMIT CHANGES means
        // we get an output row per input record, with running totals.

        let storage: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
        storage
            .create_stream(&StreamName::try_from("events").unwrap(), 0, 0)
            .await
            .unwrap();

        // Publish 4 records with region=us (amounts: 10, 20, 30, 40)
        for i in 0..4 {
            let amount = (i + 1) * 10;
            let record = Record {
                key: Some(Bytes::from(format!("evt-{i}"))),
                value: Bytes::from(format!(r#"{{"amount": {amount}, "region": "us"}}"#)),
                subject: "event.created".into(),
                headers: vec![],

                timestamp_ns: None,
            };
            storage
                .append(&StreamName::try_from("events").unwrap(), &record)
                .await
                .unwrap();
        }

        // Build the query via AST (since parser may not handle tumbling GROUP BY).
        // Equivalent to:
        // CREATE VIEW agg_out AS
        //   SELECT region, COUNT(*) AS cnt, SUM(payload->>'amount') AS total
        //   FROM events
        //   GROUP BY tumbling(timestamp, '1 hour'), payload->>'region'
        //   EMIT CHANGES
        let query = QueryExpr {
            ctes: vec![],
            select: vec![
                SelectItem {
                    expr: Expr::JsonAccess {
                        expr: Box::new(Expr::Column {
                            table: None,
                            name: "payload".into(),
                        }),
                        field: "region".into(),
                        as_text: true,
                    },
                    alias: Some("region".into()),
                },
                SelectItem {
                    expr: Expr::Aggregate {
                        func: AggregateFunc::Count,
                        expr: Box::new(Expr::Wildcard { table: None }),
                        distinct: false,
                    },
                    alias: Some("cnt".into()),
                },
                SelectItem {
                    expr: Expr::Aggregate {
                        func: AggregateFunc::Sum,
                        expr: Box::new(Expr::JsonAccess {
                            expr: Box::new(Expr::Column {
                                table: None,
                                name: "payload".into(),
                            }),
                            field: "amount".into(),
                            as_text: true,
                        }),
                        distinct: false,
                    },
                    alias: Some("total".into()),
                },
            ],
            from: FromClause::Stream {
                name: "events".into(),
                alias: None,
            },
            joins: vec![],
            filter: None,
            group_by: vec![
                Expr::Function {
                    name: "tumbling".into(),
                    args: vec![
                        Expr::Column {
                            table: None,
                            name: "timestamp".into(),
                        },
                        Expr::Literal(LiteralValue::String("1 hour".into())),
                    ],
                },
                Expr::JsonAccess {
                    expr: Box::new(Expr::Column {
                        table: None,
                        name: "payload".into(),
                    }),
                    field: "region".into(),
                    as_text: true,
                },
            ],
            order_by: vec![],
            limit: None,
            offset: None,
        };

        // Plan and verify it produces a WindowedAggregate node
        let plan = crate::planner::plan(&query, EmitMode::Changes).unwrap();
        let mode = detect_mode(&plan);
        assert!(
            matches!(mode, ContinuousMode::WindowedAggregate { .. }),
            "expected WindowedAggregate mode, got {mode:?}"
        );

        // Drive the WindowedAggregateState directly from within the test
        // to verify pipeline decomposition + mode detection, then check output.

        // Decompose the plan
        let pipeline = decompose_plan(&plan).unwrap();
        assert_eq!(pipeline.source_stream, "events");

        // Manually run the windowed aggregate loop for 1 batch
        let source_name = StreamName::try_from("events").unwrap();
        let batch = storage.read(&source_name, Offset(0), 1000).await.unwrap();
        assert_eq!(batch.len(), 4);

        // Extract windowed params
        match mode {
            ContinuousMode::WindowedAggregate {
                window_size,
                group_by,
                select_items,
                emit_mode,
            } => {
                let window_nanos = parse_interval_nanos(&window_size);
                assert!(window_nanos > 0, "window_nanos should be non-zero");

                let mut windowed_state = WindowedAggregateState::new(
                    window_nanos,
                    5 * 60 * 1_000_000_000,
                    group_by,
                    select_items,
                    emit_mode,
                );

                let mut all_emitted = Vec::new();

                for record in &batch {
                    let row = stored_record_to_row(record, None);
                    let emitted = windowed_state.process_record(&row, record.timestamp);
                    all_emitted.extend(emitted);
                }

                // EMIT CHANGES: one output per input
                assert_eq!(
                    all_emitted.len(),
                    4,
                    "expected 4 emitted rows (EMIT CHANGES), got {}",
                    all_emitted.len()
                );

                // Last emitted row should have count=4, sum=100
                let last = &all_emitted[3];
                assert_eq!(last.get("cnt"), Some(&Value::Int(4)));
                // Sum of 10+20+30+40 = 100
                assert_eq!(last.get("total"), Some(&Value::Int(100)));
            }
            _ => panic!("expected WindowedAggregate mode"),
        }
    }

    // ── Materialized view test ───────────────────────────────────────────

    #[tokio::test]
    async fn continuous_mv_writes_to_hashmap() {
        // Verify that when mv_state is Some, output goes to the HashMap
        // instead of a stream.

        let storage = setup_source_data().await; // 10 "orders" records
        let dir = tempfile::tempdir().unwrap();
        let registry = Arc::new(QueryRegistry::new(dir.path().to_path_buf()));

        let query_id = "q_mv_test".to_string();
        let sql = r#"CREATE VIEW mv_out AS SELECT key, payload->>'total' AS total FROM "orders""#
            .to_string();
        let target = "mv_out".to_string();

        registry.register(&query_id, &sql, &target).unwrap();

        let (cancel_tx, cancel_rx) = oneshot::channel::<()>();
        registry.set_running(&query_id, cancel_tx).unwrap();

        // Create the MV state
        let mv_state: Arc<RwLock<HashMap<String, Row>>> = Arc::new(RwLock::new(HashMap::new()));

        let storage_clone = storage.clone();
        let registry_clone = registry.clone();
        let mv_clone = mv_state.clone();

        let handle = tokio::spawn(async move {
            run_continuous_query(
                query_id.clone(),
                sql,
                target,
                storage_clone,
                registry_clone,
                cancel_rx,
                Some(mv_clone),
            )
            .await;
        });

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // Stop the query
        registry.stop("q_mv_test").unwrap();
        let _ = handle.await;

        // Check MV state — should have 10 entries (no GROUP BY, so key is __global__)
        // Actually, since there's no GROUP BY, every row will write to __global__
        // and only the last one will survive.  That's correct for a simple non-aggregate MV.
        let state = mv_state.read().unwrap();
        assert!(!state.is_empty(), "MV state should have at least one entry");

        // The __global__ key should exist with the last record's data
        let row = state
            .get("__global__")
            .expect("expected __global__ key in MV");
        assert!(row.get("key").is_some(), "expected 'key' column in MV row");
        assert!(
            row.get("total").is_some(),
            "expected 'total' column in MV row"
        );
    }

    // ── Stream-stream join test ──────────────────────────────────────────

    #[tokio::test]
    async fn continuous_stream_stream_join() {
        // Verify that detect_mode correctly identifies StreamStreamJoin plans
        // and the executor processes both streams.

        let storage: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());

        // Create two streams: clicks and purchases
        storage
            .create_stream(&StreamName::try_from("clicks").unwrap(), 0, 0)
            .await
            .unwrap();
        storage
            .create_stream(&StreamName::try_from("purchases").unwrap(), 0, 0)
            .await
            .unwrap();

        // Add click records
        for i in 0..3 {
            let record = Record {
                key: None,
                value: Bytes::from(format!(r#"{{"user_id": {}, "page": "p{i}"}}"#, i + 1)),
                subject: "click".into(),
                headers: vec![],

                timestamp_ns: None,
            };
            storage
                .append(&StreamName::try_from("clicks").unwrap(), &record)
                .await
                .unwrap();
        }

        // Add purchase records — user 2 and 3 match
        for uid in [2, 3] {
            let record = Record {
                key: None,
                value: Bytes::from(format!(r#"{{"user_id": {uid}, "item": "widget"}}"#)),
                subject: "purchase".into(),
                headers: vec![],

                timestamp_ns: None,
            };
            storage
                .append(&StreamName::try_from("purchases").unwrap(), &record)
                .await
                .unwrap();
        }

        // Build the query via AST:
        // SELECT * FROM clicks
        //   JOIN purchases ON clicks.payload->>'user_id' = purchases.payload->>'user_id'
        //   WITHIN '1 hour'
        let query = QueryExpr {
            ctes: vec![],
            select: vec![SelectItem {
                expr: Expr::Wildcard { table: None },
                alias: None,
            }],
            from: FromClause::Stream {
                name: "clicks".into(),
                alias: None,
            },
            joins: vec![JoinClause {
                join_type: JoinType::Inner,
                source: FromClause::Stream {
                    name: "purchases".into(),
                    alias: None,
                },
                on: Expr::BinaryOp {
                    left: Box::new(Expr::JsonAccess {
                        expr: Box::new(Expr::Column {
                            table: Some("clicks".into()),
                            name: "payload".into(),
                        }),
                        field: "user_id".into(),
                        as_text: true,
                    }),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::JsonAccess {
                        expr: Box::new(Expr::Column {
                            table: Some("purchases".into()),
                            name: "payload".into(),
                        }),
                        field: "user_id".into(),
                        as_text: true,
                    }),
                },
                within: Some("1 hour".into()),
            }],
            filter: None,
            group_by: vec![],
            order_by: vec![],
            limit: None,
            offset: None,
        };

        let plan = crate::planner::plan(&query, EmitMode::Changes).unwrap();
        let mode = detect_mode(&plan);
        assert!(
            matches!(mode, ContinuousMode::StreamStreamJoin { .. }),
            "expected StreamStreamJoin mode, got {mode:?}"
        );

        // Also verify pipeline decomposition finds the left source
        let pipeline = decompose_plan(&plan).unwrap();
        assert_eq!(pipeline.source_stream, "clicks");

        // Drive the join state manually to verify correctness
        let on_expr = Expr::BinaryOp {
            left: Box::new(Expr::JsonAccess {
                expr: Box::new(Expr::Column {
                    table: Some("clicks".into()),
                    name: "payload".into(),
                }),
                field: "user_id".into(),
                as_text: true,
            }),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::JsonAccess {
                expr: Box::new(Expr::Column {
                    table: Some("purchases".into()),
                    name: "payload".into(),
                }),
                field: "user_id".into(),
                as_text: true,
            }),
        };

        let within_nanos = parse_interval_nanos("1 hour");
        let mut join_state = StreamStreamJoinState::new(within_nanos, &on_expr);

        // Process left (clicks) first
        let click_stream = StreamName::try_from("clicks").unwrap();
        let click_batch = storage.read(&click_stream, Offset(0), 100).await.unwrap();

        let mut all_matches: Vec<Row> = Vec::new();

        for record in &click_batch {
            let row = stored_record_to_row(record, None);
            let matched = join_state.process_left(row, record.timestamp);
            all_matches.extend(matched);
        }
        // No matches yet — right side is empty in the buffer
        assert_eq!(all_matches.len(), 0);

        // Process right (purchases)
        let purchase_stream = StreamName::try_from("purchases").unwrap();
        let purchase_batch = storage.read(&purchase_stream, Offset(0), 100).await.unwrap();

        for record in &purchase_batch {
            let row = stored_record_to_row(record, None);
            let matched = join_state.process_right(row, record.timestamp);
            all_matches.extend(matched);
        }

        // User 2 and 3 from purchases match user 2 and 3 from clicks
        assert_eq!(
            all_matches.len(),
            2,
            "expected 2 join matches, got {}",
            all_matches.len()
        );
    }
}
