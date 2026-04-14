pub mod logical;
pub mod physical;

use crate::parser::ast::*;
use crate::parser::error::ParseError;
use logical::LogicalPlan;
use physical::PhysicalPlan;

/// Convert an ExQL query AST into a physical execution plan.
pub fn plan(query: &QueryExpr) -> Result<PhysicalPlan, ParseError> {
    let logical = build_logical(query)?;
    let physical = to_physical(logical);
    Ok(physical)
}

// ---------------------------------------------------------------------------
// Logical plan construction
// ---------------------------------------------------------------------------

fn build_logical(query: &QueryExpr) -> Result<LogicalPlan, ParseError> {
    // 1. FROM → base scan (with CTE expansion)
    let mut plan = from_to_logical(&query.from, &query.ctes)?;

    // 2. JOINs
    for join in &query.joins {
        let right = from_to_logical(&join.source, &query.ctes)?;
        plan = LogicalPlan::Join {
            left: Box::new(plan),
            right: Box::new(right),
            on: join.on.clone(),
            join_type: join.join_type.clone(),
        };
    }

    // 3. WHERE
    if let Some(ref predicate) = query.filter {
        plan = LogicalPlan::Filter {
            input: Box::new(plan),
            predicate: predicate.clone(),
        };
    }

    // 4. GROUP BY / aggregates
    let has_aggregates = query.select.iter().any(|si| expr_has_aggregate(&si.expr));
    if has_aggregates || !query.group_by.is_empty() {
        plan = LogicalPlan::Aggregate {
            input: Box::new(plan),
            group_by: query.group_by.clone(),
            select_items: query.select.clone(),
        };
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
fn from_to_logical(from: &FromClause, ctes: &[Cte]) -> Result<LogicalPlan, ParseError> {
    match from {
        FromClause::Stream { name, alias } => {
            // Check if this name matches a CTE — expand inline.
            if let Some(cte) = ctes.iter().find(|c| c.name == *name) {
                return build_logical(&cte.query);
            }
            Ok(LogicalPlan::Scan {
                stream: name.clone(),
                alias: alias.clone(),
            })
        }
        FromClause::Subquery { query, alias: _ } => build_logical(query),
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
        LogicalPlan::Scan { stream, alias } => PhysicalPlan::SeqScan { stream, alias },
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
                let p = plan(&q).unwrap();
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
                let p = plan(&q).unwrap();
                // Project → Filter → SeqScan
                match p {
                    PhysicalPlan::Project { input, .. } => match *input {
                        PhysicalPlan::Filter {
                            input: inner,
                            predicate,
                        } => {
                            assert!(matches!(predicate, Expr::BinaryOp { .. }));
                            assert!(
                                matches!(*inner, PhysicalPlan::SeqScan { ref stream, .. } if stream == "orders")
                            );
                        }
                        other => panic!("expected Filter, got {other:?}"),
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
                let p = plan(&q).unwrap();
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
                let p = plan(&q).unwrap();
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
                let p = plan(&q).unwrap();
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
}
