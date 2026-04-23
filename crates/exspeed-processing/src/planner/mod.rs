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
        PhysicalPlan::SeqScan { stream, alias, predicate, .. } => {
            let mut cs = seed;
            if let Some(ref pred) = predicate {
                collect_columns(pred, &mut cs);
            }
            PhysicalPlan::SeqScan { stream, alias, required_columns: cs, predicate }
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
                PhysicalPlan::SeqScan { stream, alias, required_columns, predicate: None } => {
                    PhysicalPlan::SeqScan { stream, alias, required_columns, predicate: Some(predicate) }
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
        other => other,
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
                // Limit → Sort → Project → SeqScan
                match p {
                    PhysicalPlan::Limit {
                        input,
                        limit,
                        offset,
                    } => {
                        assert_eq!(limit, Some(10));
                        assert!(offset.is_none());
                        match *input {
                            PhysicalPlan::Sort {
                                input: inner,
                                ref order_by,
                            } => {
                                assert_eq!(order_by.len(), 1);
                                assert!(order_by[0].descending);
                                assert!(matches!(*inner, PhysicalPlan::Project { .. }));
                            }
                            other => panic!("expected Sort, got {other:?}"),
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
            PhysicalPlan::SeqScan { required_columns, .. } => required_columns,
            PhysicalPlan::Filter { input, .. }
            | PhysicalPlan::Project { input, .. }
            | PhysicalPlan::Sort { input, .. }
            | PhysicalPlan::Limit { input, .. }
            | PhysicalPlan::HashAggregate { input, .. }
            | PhysicalPlan::WindowedAggregate { input, .. } => find_scan(input),
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
