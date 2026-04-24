pub mod column_set;
pub mod logical;
pub mod physical;

use crate::parser::ast::*;
use crate::parser::error::ParseError;
use logical::LogicalPlan;
use physical::PhysicalPlan;

/// Convert an ExQL query AST into a physical execution plan.
pub fn plan(query: &QueryExpr, emit_mode: EmitMode) -> Result<PhysicalPlan, ParseError> {
    let logical = build_logical(query, emit_mode)?;
    let physical = to_physical(logical);
    let optimized = push_down_filters(physical);
    let optimized = optimize_sort_limit(optimized);
    let optimized = extract_timestamp_bounds(optimized);
    let optimized = extract_key_eq_filter(optimized);
    Ok(annotate_scans(optimized))
}

// ---------------------------------------------------------------------------
// Logical plan construction
// ---------------------------------------------------------------------------

fn build_logical(query: &QueryExpr, emit_mode: EmitMode) -> Result<LogicalPlan, ParseError> {
    // 1. FROM → base scan (with CTE expansion)
    let mut plan = from_to_logical(&query.from, &query.ctes, emit_mode.clone())?;

    // 2. JOINs
    for join in &query.joins {
        let right = from_to_logical(&join.source, &query.ctes, emit_mode.clone())?;
        if let Some(ref within) = join.within {
            plan = LogicalPlan::StreamStreamJoin {
                left: Box::new(plan),
                right: Box::new(right),
                on: join.on.clone(),
                within: within.clone(),
                join_type: join.join_type.clone(),
            };
        } else {
            plan = LogicalPlan::Join {
                left: Box::new(plan),
                right: Box::new(right),
                on: join.on.clone(),
                join_type: join.join_type.clone(),
            };
        }
    }

    // 3. WHERE
    if let Some(ref predicate) = query.filter {
        plan = LogicalPlan::Filter {
            input: Box::new(plan),
            predicate: predicate.clone(),
        };
    }

    // 4. GROUP BY / aggregates — detect tumbling() window function
    let has_aggregates = query.select.iter().any(|si| expr_has_aggregate(&si.expr));
    if has_aggregates || !query.group_by.is_empty() {
        // Look for tumbling() in GROUP BY
        let tumbling_info = query.group_by.iter().find_map(|expr| {
            if let Expr::Function { name, args } = expr {
                if name.to_uppercase() == "TUMBLING" && args.len() >= 2 {
                    if let Expr::Literal(LiteralValue::String(ref size)) = args[1] {
                        return Some(size.clone());
                    }
                }
            }
            None
        });

        if let Some(window_size) = tumbling_info {
            // Remove the tumbling() call from group_by — remaining exprs are real keys
            let real_group_by: Vec<Expr> = query
                .group_by
                .iter()
                .filter(|expr| {
                    !matches!(expr, Expr::Function { name, .. } if name.to_uppercase() == "TUMBLING")
                })
                .cloned()
                .collect();

            plan = LogicalPlan::WindowedAggregate {
                input: Box::new(plan),
                window_size,
                group_by: real_group_by,
                select_items: query.select.clone(),
                emit_mode,
            };
        } else {
            plan = LogicalPlan::Aggregate {
                input: Box::new(plan),
                group_by: query.group_by.clone(),
                select_items: query.select.clone(),
            };
        }
    }

    // 5. SELECT (project)
    plan = LogicalPlan::Project {
        input: Box::new(plan),
        items: query.select.clone(),
    };

    // 6. ORDER BY
    if !query.order_by.is_empty() {
        plan = LogicalPlan::Sort {
            input: Box::new(plan),
            order_by: query.order_by.clone(),
        };
    }

    // 7. LIMIT / OFFSET
    if query.limit.is_some() || query.offset.is_some() {
        plan = LogicalPlan::Limit {
            input: Box::new(plan),
            limit: query.limit,
            offset: query.offset,
        };
    }

    Ok(plan)
}

/// Convert a FROM clause into a logical scan node, expanding CTEs inline.
fn from_to_logical(
    from: &FromClause,
    ctes: &[Cte],
    emit_mode: EmitMode,
) -> Result<LogicalPlan, ParseError> {
    match from {
        FromClause::Stream { name, alias } => {
            // Check if this name matches a CTE — expand inline.
            if let Some(cte) = ctes.iter().find(|c| c.name == *name) {
                return build_logical(&cte.query, emit_mode);
            }
            Ok(LogicalPlan::Scan {
                stream: name.clone(),
                alias: alias.clone(),
            })
        }
        FromClause::Subquery { query, alias: _ } => build_logical(query, emit_mode),
        FromClause::External {
            connection,
            table,
            alias,
            driver,
        } => Ok(LogicalPlan::ExternalScan {
            connection: connection.clone(),
            table: table.clone(),
            alias: alias.clone().unwrap_or_else(|| table.clone()),
            driver: driver.clone(),
        }),
    }
}

/// Recursively check whether an expression contains an Aggregate node.
fn expr_has_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::Aggregate { .. } => true,
        Expr::BinaryOp { left, right, .. } => expr_has_aggregate(left) || expr_has_aggregate(right),
        Expr::UnaryOp { expr, .. } => expr_has_aggregate(expr),
        Expr::Function { args, .. } => args.iter().any(expr_has_aggregate),
        Expr::JsonAccess { expr, .. } => expr_has_aggregate(expr),
        Expr::Cast { expr, .. } => expr_has_aggregate(expr),
        Expr::Case {
            conditions,
            else_val,
        } => {
            conditions
                .iter()
                .any(|(c, v)| expr_has_aggregate(c) || expr_has_aggregate(v))
                || else_val.as_ref().is_some_and(|e| expr_has_aggregate(e))
        }
        Expr::IsNull { expr, .. } => expr_has_aggregate(expr),
        Expr::InList { expr, list, .. } => {
            expr_has_aggregate(expr) || list.iter().any(expr_has_aggregate)
        }
        Expr::Between {
            expr, low, high, ..
        } => expr_has_aggregate(expr) || expr_has_aggregate(low) || expr_has_aggregate(high),
        // Leaf nodes
        Expr::Column { .. }
        | Expr::Literal(_)
        | Expr::Wildcard { .. }
        | Expr::Subquery(_)
        | Expr::Interval { .. } => false,
    }
}

// ---------------------------------------------------------------------------
// Physical plan construction (v1: 1:1 mapping, no optimizer)
// ---------------------------------------------------------------------------

fn to_physical(logical: LogicalPlan) -> PhysicalPlan {
    match logical {
        LogicalPlan::Scan { stream, alias } => PhysicalPlan::SeqScan {
            stream,
            alias,
            required_columns: crate::planner::column_set::ColumnSet::default(),
            predicate: None,
            reverse_limit: None,
            timestamp_lower_bound: None,
            key_eq_filter: None,
        },
        LogicalPlan::ExternalScan {
            connection,
            table,
            alias,
            driver,
        } => PhysicalPlan::ExternalScan {
            connection,
            table,
            alias,
            driver,
        },
        LogicalPlan::Filter { input, predicate } => PhysicalPlan::Filter {
            input: Box::new(to_physical(*input)),
            predicate,
        },
        LogicalPlan::Project { input, items } => PhysicalPlan::Project {
            input: Box::new(to_physical(*input)),
            items,
        },
        LogicalPlan::Join {
            left,
            right,
            on,
            join_type,
        } => PhysicalPlan::HashJoin {
            left: Box::new(to_physical(*left)),
            right: Box::new(to_physical(*right)),
            on,
            join_type,
        },
        LogicalPlan::Aggregate {
            input,
            group_by,
            select_items,
        } => PhysicalPlan::HashAggregate {
            input: Box::new(to_physical(*input)),
            group_by,
            select_items,
        },
        LogicalPlan::WindowedAggregate {
            input,
            window_size,
            group_by,
            select_items,
            emit_mode,
        } => PhysicalPlan::WindowedAggregate {
            input: Box::new(to_physical(*input)),
            window_size,
            group_by,
            select_items,
            emit_mode,
        },
        LogicalPlan::StreamStreamJoin {
            left,
            right,
            on,
            within,
            join_type,
        } => PhysicalPlan::StreamStreamJoin {
            left: Box::new(to_physical(*left)),
            right: Box::new(to_physical(*right)),
            on,
            within,
            join_type,
        },
        LogicalPlan::Sort { input, order_by } => PhysicalPlan::Sort {
            input: Box::new(to_physical(*input)),
            order_by,
        },
        LogicalPlan::Limit {
            input,
            limit,
            offset,
        } => PhysicalPlan::Limit {
            input: Box::new(to_physical(*input)),
            limit,
            offset,
        },
    }
}

// ---------------------------------------------------------------------------
// Annotation pass: populate SeqScan::required_columns
// ---------------------------------------------------------------------------

use crate::planner::column_set::{collect_columns, collect_from_projection, ColumnSet};

/// Walk the plan tree and populate each `SeqScan`'s `required_columns` with
/// the columns that the tree above it actually references.
pub fn annotate_scans(plan: PhysicalPlan) -> PhysicalPlan {
    annotate_with_seed(plan, ColumnSet::default())
}

fn annotate_with_seed(plan: PhysicalPlan, seed: ColumnSet) -> PhysicalPlan {
    match plan {
        PhysicalPlan::SeqScan { stream, alias, predicate, reverse_limit, timestamp_lower_bound, key_eq_filter, .. } => {
            let mut cs = seed;
            if let Some(ref pred) = predicate {
                collect_columns(pred, &mut cs);
            }
            PhysicalPlan::SeqScan { stream, alias, required_columns: cs, predicate, reverse_limit, timestamp_lower_bound, key_eq_filter }
        }
        PhysicalPlan::ExternalScan { .. } => plan,
        PhysicalPlan::Filter { input, predicate } => {
            let mut cs = seed;
            collect_columns(&predicate, &mut cs);
            PhysicalPlan::Filter {
                input: Box::new(annotate_with_seed(*input, cs)),
                predicate,
            }
        }
        PhysicalPlan::Project { input, items } => {
            // Above a Project the scan's columns aren't visible. Discard the
            // seed and start fresh from this projection's references.
            let mut cs = ColumnSet::default();
            collect_from_projection(&items, &mut cs);
            PhysicalPlan::Project {
                input: Box::new(annotate_with_seed(*input, cs)),
                items,
            }
        }
        PhysicalPlan::Sort { input, order_by } => {
            let mut cs = seed;
            for key in &order_by {
                collect_columns(&key.expr, &mut cs);
            }
            PhysicalPlan::Sort {
                input: Box::new(annotate_with_seed(*input, cs)),
                order_by,
            }
        }
        PhysicalPlan::Limit { input, limit, offset } => {
            PhysicalPlan::Limit {
                input: Box::new(annotate_with_seed(*input, seed)),
                limit,
                offset,
            }
        }
        PhysicalPlan::HashAggregate { input, group_by, select_items } => {
            let mut cs = ColumnSet::default();
            for g in &group_by { collect_columns(g, &mut cs); }
            collect_from_projection(&select_items, &mut cs);
            PhysicalPlan::HashAggregate {
                input: Box::new(annotate_with_seed(*input, cs)),
                group_by,
                select_items,
            }
        }
        PhysicalPlan::WindowedAggregate {
            input, window_size, group_by, select_items, emit_mode,
        } => {
            let mut cs = ColumnSet::default();
            for g in &group_by { collect_columns(g, &mut cs); }
            collect_from_projection(&select_items, &mut cs);
            PhysicalPlan::WindowedAggregate {
                input: Box::new(annotate_with_seed(*input, cs)),
                window_size, group_by, select_items, emit_mode,
            }
        }
        PhysicalPlan::HashJoin { left, right, on, join_type } => {
            let mut cs = ColumnSet::default();
            collect_columns(&on, &mut cs);
            PhysicalPlan::HashJoin {
                left: Box::new(annotate_with_seed(*left, cs.clone())),
                right: Box::new(annotate_with_seed(*right, cs)),
                on,
                join_type,
            }
        }
        PhysicalPlan::StreamStreamJoin { left, right, on, within, join_type } => {
            let mut cs = ColumnSet::default();
            collect_columns(&on, &mut cs);
            PhysicalPlan::StreamStreamJoin {
                left: Box::new(annotate_with_seed(*left, cs.clone())),
                right: Box::new(annotate_with_seed(*right, cs)),
                on, within, join_type,
            }
        }
        PhysicalPlan::TopN { input, order_by, limit } => {
            let mut cs = seed;
            for key in &order_by {
                collect_columns(&key.expr, &mut cs);
            }
            PhysicalPlan::TopN {
                input: Box::new(annotate_with_seed(*input, cs)),
                order_by,
                limit,
            }
        }
        PhysicalPlan::IndexScan { .. } => plan, // indexes manage their own columns
    }
}

// ---------------------------------------------------------------------------
// Optimisation: predicate pushdown
// ---------------------------------------------------------------------------

/// When a `Filter` node sits directly on a `SeqScan`, absorb the predicate
/// into the scan so rows are filtered during batch conversion instead of
/// after materialisation.
fn push_down_filters(plan: PhysicalPlan) -> PhysicalPlan {
    match plan {
        PhysicalPlan::Filter { input, predicate } => {
            match *input {
                PhysicalPlan::SeqScan { stream, alias, required_columns, predicate: None, reverse_limit, timestamp_lower_bound, key_eq_filter } => {
                    PhysicalPlan::SeqScan { stream, alias, required_columns, predicate: Some(predicate), reverse_limit, timestamp_lower_bound, key_eq_filter }
                }
                other => PhysicalPlan::Filter {
                    input: Box::new(push_down_filters(other)),
                    predicate,
                },
            }
        }
        PhysicalPlan::Project { input, items } => PhysicalPlan::Project {
            input: Box::new(push_down_filters(*input)),
            items,
        },
        PhysicalPlan::Sort { input, order_by } => PhysicalPlan::Sort {
            input: Box::new(push_down_filters(*input)),
            order_by,
        },
        PhysicalPlan::Limit { input, limit, offset } => PhysicalPlan::Limit {
            input: Box::new(push_down_filters(*input)),
            limit,
            offset,
        },
        PhysicalPlan::HashAggregate { input, group_by, select_items } => PhysicalPlan::HashAggregate {
            input: Box::new(push_down_filters(*input)),
            group_by,
            select_items,
        },
        PhysicalPlan::WindowedAggregate { input, window_size, group_by, select_items, emit_mode } => PhysicalPlan::WindowedAggregate {
            input: Box::new(push_down_filters(*input)),
            window_size, group_by, select_items, emit_mode,
        },
        PhysicalPlan::HashJoin { left, right, on, join_type } => PhysicalPlan::HashJoin {
            left: Box::new(push_down_filters(*left)),
            right: Box::new(push_down_filters(*right)),
            on, join_type,
        },
        PhysicalPlan::StreamStreamJoin { left, right, on, within, join_type } => PhysicalPlan::StreamStreamJoin {
            left: Box::new(push_down_filters(*left)),
            right: Box::new(push_down_filters(*right)),
            on, within, join_type,
        },
        PhysicalPlan::TopN { input, order_by, limit } => PhysicalPlan::TopN {
            input: Box::new(push_down_filters(*input)),
            order_by, limit,
        },
        PhysicalPlan::IndexScan { .. } => plan,
        other => other,
    }
}

// ---------------------------------------------------------------------------
// Optimisation: sort/limit fusion
// ---------------------------------------------------------------------------

/// Detect `Limit(Sort(…))` patterns and replace with either:
/// - Eliminated sort (ORDER BY offset ASC on a scan that's already ordered)
/// - Reverse-tail scan (ORDER BY offset DESC LIMIT N)
/// - TopN heap (any other ORDER BY + LIMIT)
fn optimize_sort_limit(plan: PhysicalPlan) -> PhysicalPlan {
    match plan {
        PhysicalPlan::Limit { input, limit, offset } => {
            match *input {
                PhysicalPlan::Sort { input: sort_input, order_by } => {
                    if let Some(lim) = limit {
                        // ORDER BY offset DESC LIMIT N → reverse-tail scan
                        if is_single_offset_order(&order_by) {
                            if order_by[0].descending {
                                return PhysicalPlan::Limit {
                                    input: Box::new(set_reverse_limit(
                                        optimize_sort_limit(*sort_input), lim,
                                    )),
                                    limit: Some(lim),
                                    offset,
                                };
                            } else if subtree_preserves_offset_order(&sort_input) {
                                // ORDER BY offset ASC on a naturally-ordered subtree → drop sort
                                return PhysicalPlan::Limit {
                                    input: Box::new(optimize_sort_limit(*sort_input)),
                                    limit: Some(lim),
                                    offset,
                                };
                            }
                        }
                        // Generic ORDER BY + LIMIT → TopN heap
                        let effective_limit = lim + offset.unwrap_or(0);
                        return PhysicalPlan::Limit {
                            input: Box::new(PhysicalPlan::TopN {
                                input: Box::new(optimize_sort_limit(*sort_input)),
                                order_by,
                                limit: effective_limit,
                            }),
                            limit: Some(lim),
                            offset,
                        };
                    }
                    // No limit → keep Sort intact, recurse into child
                    PhysicalPlan::Limit {
                        input: Box::new(PhysicalPlan::Sort {
                            input: Box::new(optimize_sort_limit(*sort_input)),
                            order_by,
                        }),
                        limit,
                        offset,
                    }
                }
                other => PhysicalPlan::Limit {
                    input: Box::new(optimize_sort_limit(other)),
                    limit,
                    offset,
                },
            }
        }
        PhysicalPlan::Sort { input, order_by } => {
            // Standalone ORDER BY offset ASC on a naturally-ordered tree → no-op
            if is_single_offset_order(&order_by) && !order_by[0].descending
                && subtree_preserves_offset_order(&input)
            {
                return optimize_sort_limit(*input);
            }
            PhysicalPlan::Sort {
                input: Box::new(optimize_sort_limit(*input)),
                order_by,
            }
        }
        PhysicalPlan::Project { input, items } => PhysicalPlan::Project {
            input: Box::new(optimize_sort_limit(*input)), items,
        },
        PhysicalPlan::Filter { input, predicate } => PhysicalPlan::Filter {
            input: Box::new(optimize_sort_limit(*input)), predicate,
        },
        PhysicalPlan::TopN { input, order_by, limit } => PhysicalPlan::TopN {
            input: Box::new(optimize_sort_limit(*input)), order_by, limit,
        },
        PhysicalPlan::HashAggregate { input, group_by, select_items } => PhysicalPlan::HashAggregate {
            input: Box::new(optimize_sort_limit(*input)), group_by, select_items,
        },
        PhysicalPlan::HashJoin { left, right, on, join_type } => PhysicalPlan::HashJoin {
            left: Box::new(optimize_sort_limit(*left)),
            right: Box::new(optimize_sort_limit(*right)),
            on, join_type,
        },
        PhysicalPlan::StreamStreamJoin { left, right, on, within, join_type } => PhysicalPlan::StreamStreamJoin {
            left: Box::new(optimize_sort_limit(*left)),
            right: Box::new(optimize_sort_limit(*right)),
            on, within, join_type,
        },
        PhysicalPlan::WindowedAggregate { input, window_size, group_by, select_items, emit_mode } => PhysicalPlan::WindowedAggregate {
            input: Box::new(optimize_sort_limit(*input)),
            window_size, group_by, select_items, emit_mode,
        },
        PhysicalPlan::IndexScan { .. } => plan,
        other => other,
    }
}

/// True when `order_by` is a single `ORDER BY offset` clause.
fn is_single_offset_order(order_by: &[OrderByItem]) -> bool {
    order_by.len() == 1
        && matches!(&order_by[0].expr, Expr::Column { name, .. } if name == "offset")
}

/// True when the subtree is guaranteed to emit rows in ascending offset order
/// (i.e. a `SeqScan` with only order-preserving nodes above it).
fn subtree_preserves_offset_order(plan: &PhysicalPlan) -> bool {
    match plan {
        PhysicalPlan::SeqScan { .. } => true,
        PhysicalPlan::Project { input, .. } | PhysicalPlan::Filter { input, .. } => {
            subtree_preserves_offset_order(input)
        }
        _ => false,
    }
}

/// Push `reverse_limit` down through order-preserving nodes into the `SeqScan`.
fn set_reverse_limit(plan: PhysicalPlan, limit: u64) -> PhysicalPlan {
    match plan {
        PhysicalPlan::SeqScan { stream, alias, required_columns, predicate, timestamp_lower_bound, key_eq_filter, .. } => {
            PhysicalPlan::SeqScan { stream, alias, required_columns, predicate, reverse_limit: Some(limit), timestamp_lower_bound, key_eq_filter }
        }
        PhysicalPlan::Project { input, items } => PhysicalPlan::Project {
            input: Box::new(set_reverse_limit(*input, limit)), items,
        },
        PhysicalPlan::Filter { input, predicate } => PhysicalPlan::Filter {
            input: Box::new(set_reverse_limit(*input, limit)), predicate,
        },
        other => other,
    }
}

// ---------------------------------------------------------------------------
// Optimisation: timestamp bound extraction
// ---------------------------------------------------------------------------

/// Walk the plan tree and extract timestamp lower bounds from SeqScan
/// predicates. When a WHERE clause contains `timestamp > <literal>` or
/// `timestamp >= <literal>`, the bound is lifted into the SeqScan so the
/// storage layer can skip segments via the TimeIndex.
fn extract_timestamp_bounds(plan: PhysicalPlan) -> PhysicalPlan {
    match plan {
        PhysicalPlan::SeqScan { stream, alias, required_columns, predicate, reverse_limit, key_eq_filter, .. } => {
            let bound = predicate.as_ref().and_then(extract_timestamp_lower_bound);
            PhysicalPlan::SeqScan {
                stream, alias, required_columns, predicate, reverse_limit,
                timestamp_lower_bound: bound, key_eq_filter,
            }
        }
        PhysicalPlan::Project { input, items } => PhysicalPlan::Project {
            input: Box::new(extract_timestamp_bounds(*input)), items,
        },
        PhysicalPlan::Filter { input, predicate } => PhysicalPlan::Filter {
            input: Box::new(extract_timestamp_bounds(*input)), predicate,
        },
        PhysicalPlan::Limit { input, limit, offset } => PhysicalPlan::Limit {
            input: Box::new(extract_timestamp_bounds(*input)), limit, offset,
        },
        PhysicalPlan::TopN { input, order_by, limit } => PhysicalPlan::TopN {
            input: Box::new(extract_timestamp_bounds(*input)), order_by, limit,
        },
        PhysicalPlan::Sort { input, order_by } => PhysicalPlan::Sort {
            input: Box::new(extract_timestamp_bounds(*input)), order_by,
        },
        PhysicalPlan::HashAggregate { input, group_by, select_items } => PhysicalPlan::HashAggregate {
            input: Box::new(extract_timestamp_bounds(*input)), group_by, select_items,
        },
        PhysicalPlan::HashJoin { left, right, on, join_type } => PhysicalPlan::HashJoin {
            left: Box::new(extract_timestamp_bounds(*left)),
            right: Box::new(extract_timestamp_bounds(*right)),
            on, join_type,
        },
        PhysicalPlan::StreamStreamJoin { left, right, on, within, join_type } => PhysicalPlan::StreamStreamJoin {
            left: Box::new(extract_timestamp_bounds(*left)),
            right: Box::new(extract_timestamp_bounds(*right)),
            on, within, join_type,
        },
        PhysicalPlan::WindowedAggregate { input, window_size, group_by, select_items, emit_mode } => PhysicalPlan::WindowedAggregate {
            input: Box::new(extract_timestamp_bounds(*input)),
            window_size, group_by, select_items, emit_mode,
        },
        PhysicalPlan::IndexScan { .. } => plan,
        other => other,
    }
}

/// Extract a timestamp lower bound from a predicate expression.
///
/// Recognises patterns like `timestamp > N`, `timestamp >= N`, and
/// `N < timestamp`, `N <= timestamp`. When multiple bounds appear in
/// an AND tree, the tightest (maximum) bound is returned.
fn extract_timestamp_lower_bound(expr: &Expr) -> Option<u64> {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            match op {
                BinaryOperator::Gt | BinaryOperator::Gte => {
                    if matches!(left.as_ref(), Expr::Column { name, .. } if name == "timestamp") {
                        if let Expr::Literal(LiteralValue::Int(n)) = right.as_ref() {
                            return Some(*n as u64);
                        }
                    }
                }
                BinaryOperator::Lt | BinaryOperator::Lte => {
                    if matches!(right.as_ref(), Expr::Column { name, .. } if name == "timestamp") {
                        if let Expr::Literal(LiteralValue::Int(n)) = left.as_ref() {
                            return Some(*n as u64);
                        }
                    }
                }
                BinaryOperator::And => {
                    let l = extract_timestamp_lower_bound(left);
                    let r = extract_timestamp_lower_bound(right);
                    return l.into_iter().chain(r).max();
                }
                _ => {}
            }
            None
        }
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Optimisation: key equality extraction (bloom filter hint)
// ---------------------------------------------------------------------------

/// Walk the plan tree and extract `key = '<literal>'` predicates from SeqScan
/// nodes. When a WHERE clause contains an equality comparison on the `key`
/// column against a string literal, the value is lifted into the SeqScan's
/// `key_eq_filter` so the storage layer can skip segments via bloom filters.
fn extract_key_eq_filter(plan: PhysicalPlan) -> PhysicalPlan {
    match plan {
        PhysicalPlan::SeqScan { stream, alias, required_columns, predicate, reverse_limit, timestamp_lower_bound, .. } => {
            let key_filter = predicate.as_ref().and_then(find_key_equality);
            PhysicalPlan::SeqScan {
                stream, alias, required_columns, predicate, reverse_limit,
                timestamp_lower_bound, key_eq_filter: key_filter,
            }
        }
        PhysicalPlan::Project { input, items } => PhysicalPlan::Project {
            input: Box::new(extract_key_eq_filter(*input)), items,
        },
        PhysicalPlan::Filter { input, predicate } => PhysicalPlan::Filter {
            input: Box::new(extract_key_eq_filter(*input)), predicate,
        },
        PhysicalPlan::Limit { input, limit, offset } => PhysicalPlan::Limit {
            input: Box::new(extract_key_eq_filter(*input)), limit, offset,
        },
        PhysicalPlan::TopN { input, order_by, limit } => PhysicalPlan::TopN {
            input: Box::new(extract_key_eq_filter(*input)), order_by, limit,
        },
        PhysicalPlan::Sort { input, order_by } => PhysicalPlan::Sort {
            input: Box::new(extract_key_eq_filter(*input)), order_by,
        },
        PhysicalPlan::HashAggregate { input, group_by, select_items } => PhysicalPlan::HashAggregate {
            input: Box::new(extract_key_eq_filter(*input)), group_by, select_items,
        },
        PhysicalPlan::HashJoin { left, right, on, join_type } => PhysicalPlan::HashJoin {
            left: Box::new(extract_key_eq_filter(*left)),
            right: Box::new(extract_key_eq_filter(*right)),
            on, join_type,
        },
        PhysicalPlan::StreamStreamJoin { left, right, on, within, join_type } => PhysicalPlan::StreamStreamJoin {
            left: Box::new(extract_key_eq_filter(*left)),
            right: Box::new(extract_key_eq_filter(*right)),
            on, within, join_type,
        },
        PhysicalPlan::WindowedAggregate { input, window_size, group_by, select_items, emit_mode } => PhysicalPlan::WindowedAggregate {
            input: Box::new(extract_key_eq_filter(*input)),
            window_size, group_by, select_items, emit_mode,
        },
        PhysicalPlan::IndexScan { .. } => plan,
        other => other,
    }
}

/// Extract a key equality value from a predicate expression.
///
/// Recognises `key = '<literal>'` and `'<literal>' = key`, including when
/// nested inside an AND tree (returns the first match).
fn find_key_equality(expr: &Expr) -> Option<String> {
    match expr {
        Expr::BinaryOp { left, op: BinaryOperator::Eq, right } => {
            if matches!(left.as_ref(), Expr::Column { name, .. } if name == "key") {
                if let Expr::Literal(LiteralValue::String(s)) = right.as_ref() {
                    return Some(s.clone());
                }
            }
            if matches!(right.as_ref(), Expr::Column { name, .. } if name == "key") {
                if let Expr::Literal(LiteralValue::String(s)) = left.as_ref() {
                    return Some(s.clone());
                }
            }
            None
        }
        Expr::BinaryOp { left, op: BinaryOperator::And, right } => {
            find_key_equality(left).or_else(|| find_key_equality(right))
        }
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_simple_select() {
        let stmt = crate::parser::parse(r#"SELECT * FROM "orders""#).unwrap();
        match stmt {
            ExqlStatement::Query(q) => {
                let p = plan(&q, EmitMode::Changes).unwrap();
                // Outermost should be Project wrapping a SeqScan.
                match p {
                    PhysicalPlan::Project { input, items } => {
                        assert_eq!(items.len(), 1);
                        assert!(matches!(items[0].expr, Expr::Wildcard { .. }));
                        assert!(
                            matches!(*input, PhysicalPlan::SeqScan { ref stream, .. } if stream == "orders")
                        );
                    }
                    other => panic!("expected Project, got {other:?}"),
                }
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn plan_with_filter() {
        let stmt = crate::parser::parse(r#"SELECT * FROM "orders" WHERE key = 'a'"#).unwrap();
        match stmt {
            ExqlStatement::Query(q) => {
                let p = plan(&q, EmitMode::Changes).unwrap();
                // After predicate pushdown: Project → SeqScan { predicate: Some(..) }
                match p {
                    PhysicalPlan::Project { input, .. } => match *input {
                        PhysicalPlan::SeqScan { ref predicate, ref stream, .. } => {
                            assert!(predicate.is_some(), "predicate should be pushed into scan");
                            assert_eq!(stream, "orders");
                        }
                        other => panic!("expected SeqScan with pushed predicate, got {other:?}"),
                    },
                    other => panic!("expected Project, got {other:?}"),
                }
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn plan_with_join() {
        let stmt = crate::parser::parse(r#"SELECT * FROM "a" JOIN "b" ON a.key = b.key"#).unwrap();
        match stmt {
            ExqlStatement::Query(q) => {
                let p = plan(&q, EmitMode::Changes).unwrap();
                // Project → HashJoin(SeqScan, SeqScan)
                match p {
                    PhysicalPlan::Project { input, .. } => match *input {
                        PhysicalPlan::HashJoin {
                            ref left,
                            ref right,
                            ref join_type,
                            ..
                        } => {
                            assert!(matches!(join_type, JoinType::Inner));
                            assert!(
                                matches!(left.as_ref(), PhysicalPlan::SeqScan { ref stream, .. } if stream == "a")
                            );
                            assert!(
                                matches!(right.as_ref(), PhysicalPlan::SeqScan { ref stream, .. } if stream == "b")
                            );
                        }
                        other => panic!("expected HashJoin, got {other:?}"),
                    },
                    other => panic!("expected Project, got {other:?}"),
                }
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn plan_with_aggregate() {
        let stmt = crate::parser::parse(r#"SELECT COUNT(*) FROM "orders" GROUP BY key"#).unwrap();
        match stmt {
            ExqlStatement::Query(q) => {
                let p = plan(&q, EmitMode::Changes).unwrap();
                // Project → HashAggregate → SeqScan
                match p {
                    PhysicalPlan::Project { input, .. } => match *input {
                        PhysicalPlan::HashAggregate {
                            ref input,
                            ref group_by,
                            ..
                        } => {
                            assert_eq!(group_by.len(), 1);
                            assert!(
                                matches!(input.as_ref(), PhysicalPlan::SeqScan { ref stream, .. } if stream == "orders")
                            );
                        }
                        other => panic!("expected HashAggregate, got {other:?}"),
                    },
                    other => panic!("expected Project, got {other:?}"),
                }
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn plan_with_sort_limit() {
        let stmt =
            crate::parser::parse(r#"SELECT * FROM "orders" ORDER BY timestamp DESC LIMIT 10"#)
                .unwrap();
        match stmt {
            ExqlStatement::Query(q) => {
                let p = plan(&q, EmitMode::Changes).unwrap();
                // After optimize_sort_limit: Limit → TopN → Project → SeqScan
                match p {
                    PhysicalPlan::Limit {
                        input,
                        limit,
                        offset,
                    } => {
                        assert_eq!(limit, Some(10));
                        assert!(offset.is_none());
                        match *input {
                            PhysicalPlan::TopN {
                                input: inner,
                                ref order_by,
                                limit: topn_limit,
                            } => {
                                assert_eq!(order_by.len(), 1);
                                assert!(order_by[0].descending);
                                assert_eq!(topn_limit, 10);
                                assert!(matches!(*inner, PhysicalPlan::Project { .. }));
                            }
                            other => panic!("expected TopN, got {other:?}"),
                        }
                    }
                    other => panic!("expected Limit, got {other:?}"),
                }
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn plan_with_tumbling_window() {
        // Build a query with tumbling() in GROUP BY directly via AST,
        // since the parser may not yet handle tumbling() in GROUP BY.
        let query = QueryExpr {
            ctes: vec![],
            select: vec![
                SelectItem {
                    expr: Expr::Column {
                        table: None,
                        name: "region".into(),
                    },
                    alias: None,
                },
                SelectItem {
                    expr: Expr::Aggregate {
                        func: AggregateFunc::Count,
                        expr: Box::new(Expr::Wildcard { table: None }),
                        distinct: false,
                    },
                    alias: Some("cnt".into()),
                },
            ],
            from: FromClause::Stream {
                name: "orders".into(),
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
                Expr::Column {
                    table: None,
                    name: "region".into(),
                },
            ],
            order_by: vec![],
            limit: None,
            offset: None,
        };

        let p = plan(&query, EmitMode::Final).unwrap();
        // Project → WindowedAggregate → SeqScan
        match p {
            PhysicalPlan::Project { input, .. } => match *input {
                PhysicalPlan::WindowedAggregate {
                    ref window_size,
                    ref group_by,
                    ref emit_mode,
                    ref input,
                    ..
                } => {
                    assert_eq!(window_size, "1 hour");
                    // tumbling() removed; only "region" remains
                    assert_eq!(group_by.len(), 1);
                    assert!(matches!(&group_by[0], Expr::Column { name, .. } if name == "region"));
                    assert_eq!(*emit_mode, EmitMode::Final);
                    assert!(
                        matches!(input.as_ref(), PhysicalPlan::SeqScan { ref stream, .. } if stream == "orders")
                    );
                }
                other => panic!("expected WindowedAggregate, got {other:?}"),
            },
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn plan_without_tumbling_gives_hash_aggregate() {
        // Regular GROUP BY without tumbling → HashAggregate
        let query = QueryExpr {
            ctes: vec![],
            select: vec![SelectItem {
                expr: Expr::Aggregate {
                    func: AggregateFunc::Count,
                    expr: Box::new(Expr::Wildcard { table: None }),
                    distinct: false,
                },
                alias: Some("cnt".into()),
            }],
            from: FromClause::Stream {
                name: "orders".into(),
                alias: None,
            },
            joins: vec![],
            filter: None,
            group_by: vec![Expr::Column {
                table: None,
                name: "key".into(),
            }],
            order_by: vec![],
            limit: None,
            offset: None,
        };

        let p = plan(&query, EmitMode::Changes).unwrap();
        match p {
            PhysicalPlan::Project { input, .. } => {
                assert!(
                    matches!(*input, PhysicalPlan::HashAggregate { .. }),
                    "expected HashAggregate, got {:?}",
                    *input
                );
            }
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn plan_join_with_within() {
        // Build a query with JOIN ... WITHIN via AST
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
                    left: Box::new(Expr::Column {
                        table: Some("clicks".into()),
                        name: "user_id".into(),
                    }),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::Column {
                        table: Some("purchases".into()),
                        name: "user_id".into(),
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

        let p = plan(&query, EmitMode::Changes).unwrap();
        // Project → StreamStreamJoin(SeqScan, SeqScan)
        match p {
            PhysicalPlan::Project { input, .. } => match *input {
                PhysicalPlan::StreamStreamJoin {
                    ref left,
                    ref right,
                    ref within,
                    ref join_type,
                    ..
                } => {
                    assert_eq!(within, "1 hour");
                    assert!(matches!(join_type, JoinType::Inner));
                    assert!(
                        matches!(left.as_ref(), PhysicalPlan::SeqScan { ref stream, .. } if stream == "clicks")
                    );
                    assert!(
                        matches!(right.as_ref(), PhysicalPlan::SeqScan { ref stream, .. } if stream == "purchases")
                    );
                }
                other => panic!("expected StreamStreamJoin, got {other:?}"),
            },
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn plan_join_without_within() {
        // JOIN without WITHIN → regular HashJoin
        let query = QueryExpr {
            ctes: vec![],
            select: vec![SelectItem {
                expr: Expr::Wildcard { table: None },
                alias: None,
            }],
            from: FromClause::Stream {
                name: "a".into(),
                alias: None,
            },
            joins: vec![JoinClause {
                join_type: JoinType::Inner,
                source: FromClause::Stream {
                    name: "b".into(),
                    alias: None,
                },
                on: Expr::BinaryOp {
                    left: Box::new(Expr::Column {
                        table: Some("a".into()),
                        name: "key".into(),
                    }),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::Column {
                        table: Some("b".into()),
                        name: "key".into(),
                    }),
                },
                within: None,
            }],
            filter: None,
            group_by: vec![],
            order_by: vec![],
            limit: None,
            offset: None,
        };

        let p = plan(&query, EmitMode::Changes).unwrap();
        match p {
            PhysicalPlan::Project { input, .. } => {
                assert!(
                    matches!(*input, PhysicalPlan::HashJoin { .. }),
                    "expected HashJoin, got {:?}",
                    *input
                );
            }
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn pushdown_filter_into_scan() {
        let stmt = crate::parser::parse(r#"SELECT * FROM "orders" WHERE key = 'a'"#).unwrap();
        match stmt {
            ExqlStatement::Query(q) => {
                let p = plan(&q, EmitMode::Changes).unwrap();
                match p {
                    PhysicalPlan::Project { input, .. } => {
                        match *input {
                            PhysicalPlan::SeqScan { ref predicate, ref stream, .. } => {
                                assert!(predicate.is_some(), "predicate should be pushed into scan");
                                assert_eq!(stream, "orders");
                            }
                            other => panic!("expected SeqScan with pushed predicate, got {other:?}"),
                        }
                    }
                    other => panic!("expected Project, got {other:?}"),
                }
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn no_pushdown_filter_above_join() {
        let stmt = crate::parser::parse(
            r#"SELECT * FROM "a" JOIN "b" ON a.key = b.key WHERE a.key = 'x'"#
        ).unwrap();
        match stmt {
            ExqlStatement::Query(q) => {
                let p = plan(&q, EmitMode::Changes).unwrap();
                match p {
                    PhysicalPlan::Project { input, .. } => {
                        assert!(
                            matches!(*input, PhysicalPlan::Filter { .. }),
                            "filter above join should NOT be pushed down, got {:?}", *input
                        );
                    }
                    other => panic!("expected Project, got {other:?}"),
                }
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn plan_offset_asc_eliminates_sort() {
        let stmt = crate::parser::parse(r#"SELECT * FROM "orders" ORDER BY offset ASC LIMIT 10"#).unwrap();
        match stmt {
            ExqlStatement::Query(q) => {
                let p = plan(&q, EmitMode::Changes).unwrap();
                let debug = format!("{p:?}");
                assert!(!debug.contains("Sort"), "Sort should be eliminated for ORDER BY offset ASC, got: {debug}");
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn plan_offset_desc_limit_uses_reverse_scan() {
        let stmt = crate::parser::parse(r#"SELECT * FROM "orders" ORDER BY offset DESC LIMIT 5"#).unwrap();
        match stmt {
            ExqlStatement::Query(q) => {
                let p = plan(&q, EmitMode::Changes).unwrap();
                let debug = format!("{p:?}");
                assert!(!debug.contains("Sort"), "Sort should be eliminated, got: {debug}");
                assert!(debug.contains("reverse_limit: Some(5)"), "should set reverse_limit, got: {debug}");
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn plan_timestamp_desc_limit_uses_topn() {
        let stmt = crate::parser::parse(r#"SELECT * FROM "orders" ORDER BY timestamp DESC LIMIT 10"#).unwrap();
        match stmt {
            ExqlStatement::Query(q) => {
                let p = plan(&q, EmitMode::Changes).unwrap();
                let debug = format!("{p:?}");
                assert!(debug.contains("TopN"), "should use TopN for non-offset ORDER BY + LIMIT, got: {debug}");
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn plan_unbounded_sort_preserved() {
        let stmt = crate::parser::parse(r#"SELECT * FROM "orders" ORDER BY key ASC"#).unwrap();
        match stmt {
            ExqlStatement::Query(q) => {
                let p = plan(&q, EmitMode::Changes).unwrap();
                let debug = format!("{p:?}");
                assert!(debug.contains("Sort"), "unbounded ORDER BY should keep Sort, got: {debug}");
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn plan_timestamp_filter_extracts_bound() {
        let stmt = crate::parser::parse(
            r#"SELECT * FROM "orders" WHERE timestamp > 1700000000000"#
        ).unwrap();
        match stmt {
            ExqlStatement::Query(q) => {
                let p = plan(&q, EmitMode::Changes).unwrap();
                let debug = format!("{p:?}");
                assert!(
                    debug.contains("timestamp_lower_bound: Some(1700000000000)"),
                    "should extract timestamp bound, got: {debug}"
                );
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn plan_key_eq_extracts_filter() {
        let stmt = crate::parser::parse(
            r#"SELECT * FROM "orders" WHERE key = 'order-123'"#
        ).unwrap();
        match stmt {
            ExqlStatement::Query(q) => {
                let p = plan(&q, EmitMode::Changes).unwrap();
                let debug = format!("{p:?}");
                assert!(
                    debug.contains(r#"key_eq_filter: Some("order-123")"#),
                    "should extract key equality, got: {debug}"
                );
            }
            _ => panic!("expected Query"),
        }
    }
}

#[cfg(test)]
mod annotate_tests {
    use super::*;
    use crate::parser::parse;
    use crate::parser::ast::{EmitMode, ExqlStatement};
    use crate::planner::column_set::ColumnSet;
    use crate::planner::physical::PhysicalPlan;

    // Note: `plan()` returns `Result<PhysicalPlan, ParseError>` (not ExqlError).
    fn plan_for(sql: &str) -> PhysicalPlan {
        let stmt = parse(sql).expect("parse");
        let q = match stmt {
            ExqlStatement::Query(q) => q,
            _ => panic!("expected SELECT"),
        };
        plan(&q, EmitMode::Changes).expect("plan")
    }

    fn find_scan(plan: &PhysicalPlan) -> &ColumnSet {
        match plan {
            PhysicalPlan::SeqScan { required_columns, .. }
            | PhysicalPlan::IndexScan { required_columns, .. } => required_columns,
            PhysicalPlan::Filter { input, .. }
            | PhysicalPlan::Project { input, .. }
            | PhysicalPlan::Sort { input, .. }
            | PhysicalPlan::Limit { input, .. }
            | PhysicalPlan::HashAggregate { input, .. }
            | PhysicalPlan::WindowedAggregate { input, .. }
            | PhysicalPlan::TopN { input, .. } => find_scan(input),
            _ => panic!("no SeqScan reachable"),
        }
    }

    #[test]
    fn select_star_marks_everything() {
        let p = plan_for("SELECT * FROM events LIMIT 5");
        let cs = find_scan(&p);
        assert!(cs.select_star);
        assert!(cs.payload_referenced);
    }

    #[test]
    fn select_offset_only_skips_payload() {
        let p = plan_for("SELECT offset FROM events");
        let cs = find_scan(&p);
        assert!(!cs.payload_referenced);
        assert!(cs.virtual_cols.contains("offset"));
    }

    #[test]
    fn where_on_payload_marks_payload() {
        let p = plan_for("SELECT offset FROM events WHERE payload->>'status' = 'ok'");
        let cs = find_scan(&p);
        assert!(cs.payload_referenced);
        assert!(cs.virtual_cols.contains("offset"));
    }

    #[test]
    fn group_by_collects_only_group_keys() {
        let p = plan_for("SELECT key, count(*) FROM events GROUP BY key");
        let cs = find_scan(&p);
        assert!(cs.virtual_cols.contains("key"));
        assert!(!cs.payload_referenced);
    }

    #[test]
    fn aggregate_on_payload_field_marks_payload() {
        // SUM over a payload path should flip payload_referenced despite being in
        // an aggregate. count(*) must stay harmless.
        let p = plan_for("SELECT key, SUM(payload->>'amount') FROM events GROUP BY key");
        let cs = find_scan(&p);
        assert!(cs.payload_referenced, "SUM(payload->>...) should mark payload");
        assert!(cs.virtual_cols.contains("key"));
    }
}
