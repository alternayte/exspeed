pub mod ast;
pub mod dialect;
pub mod error;
pub mod transform;

pub use ast::ExqlStatement;
pub use error::ParseError;

use dialect::ExspeedDialect;
use sqlparser::parser::Parser;

/// Parse a SQL string into an ExQL AST statement.
pub fn parse(sql: &str) -> Result<ExqlStatement, ParseError> {
    let dialect = ExspeedDialect;
    let statements =
        Parser::parse_sql(&dialect, sql).map_err(|e| ParseError::Sql(e.to_string()))?;
    if statements.is_empty() {
        return Err(ParseError::Sql("empty SQL".into()));
    }
    if statements.len() > 1 {
        return Err(ParseError::Unsupported("multiple statements".into()));
    }
    transform::transform_statement(statements.into_iter().next().unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::ast::*;

    #[test]
    fn parse_simple_select() {
        let stmt = parse(r#"SELECT * FROM "orders""#).unwrap();
        match stmt {
            ExqlStatement::Query(q) => {
                assert_eq!(q.select.len(), 1);
                assert!(matches!(q.select[0].expr, Expr::Wildcard { table: None }));
                match &q.from {
                    FromClause::Stream { name, alias } => {
                        assert_eq!(name, "orders");
                        assert!(alias.is_none());
                    }
                    other => panic!("expected Stream, got {other:?}"),
                }
                assert!(q.filter.is_none());
                assert!(q.joins.is_empty());
                assert!(q.group_by.is_empty());
                assert!(q.order_by.is_empty());
                assert!(q.limit.is_none());
            }
            other => panic!("expected Query, got {other:?}"),
        }
    }

    #[test]
    fn parse_select_with_where() {
        let stmt = parse(r#"SELECT key FROM "orders" WHERE timestamp > 1000"#).unwrap();
        match stmt {
            ExqlStatement::Query(q) => {
                assert_eq!(q.select.len(), 1);
                match &q.select[0].expr {
                    Expr::Column { table, name } => {
                        assert!(table.is_none());
                        assert_eq!(name, "key");
                    }
                    other => panic!("expected Column, got {other:?}"),
                }
                assert!(q.filter.is_some());
                let filter = q.filter.unwrap();
                match &filter {
                    Expr::BinaryOp { left, op, right } => {
                        assert!(matches!(op, BinaryOperator::Gt));
                        match left.as_ref() {
                            Expr::Column { name, .. } => {
                                assert_eq!(name, "timestamp");
                            }
                            other => panic!("expected Column, got {other:?}"),
                        }
                        match right.as_ref() {
                            Expr::Literal(LiteralValue::Int(1000)) => {}
                            other => panic!("expected Int(1000), got {other:?}"),
                        }
                    }
                    other => panic!("expected BinaryOp, got {other:?}"),
                }
            }
            other => panic!("expected Query, got {other:?}"),
        }
    }

    #[test]
    fn parse_select_with_json() {
        let stmt = parse(r#"SELECT payload->>'name' FROM "orders""#).unwrap();
        match stmt {
            ExqlStatement::Query(q) => {
                assert_eq!(q.select.len(), 1);
                match &q.select[0].expr {
                    Expr::JsonAccess {
                        expr,
                        field,
                        as_text,
                    } => {
                        assert!(
                            matches!(expr.as_ref(), Expr::Column { name, .. } if name == "payload")
                        );
                        assert_eq!(field, "name");
                        assert!(*as_text);
                    }
                    other => panic!("expected JsonAccess, got {other:?}"),
                }
            }
            other => panic!("expected Query, got {other:?}"),
        }
    }

    #[test]
    fn parse_aggregate() {
        let stmt = parse(r#"SELECT COUNT(*) FROM "orders" GROUP BY key"#).unwrap();
        match stmt {
            ExqlStatement::Query(q) => {
                assert_eq!(q.select.len(), 1);
                match &q.select[0].expr {
                    Expr::Aggregate {
                        func,
                        expr,
                        distinct,
                    } => {
                        assert!(matches!(func, AggregateFunc::Count));
                        assert!(matches!(expr.as_ref(), Expr::Wildcard { .. }));
                        assert!(!distinct);
                    }
                    other => panic!("expected Aggregate, got {other:?}"),
                }
                assert_eq!(q.group_by.len(), 1);
                match &q.group_by[0] {
                    Expr::Column { table, name } => {
                        assert!(table.is_none());
                        assert_eq!(name, "key");
                    }
                    other => panic!("expected Column, got {other:?}"),
                }
            }
            other => panic!("expected Query, got {other:?}"),
        }
    }

    #[test]
    fn parse_join() {
        let stmt = parse(r#"SELECT * FROM "a" JOIN "b" ON a.key = b.key"#).unwrap();
        match stmt {
            ExqlStatement::Query(q) => {
                match &q.from {
                    FromClause::Stream { name, .. } => {
                        assert_eq!(name, "a");
                    }
                    other => panic!("expected Stream, got {other:?}"),
                }
                assert_eq!(q.joins.len(), 1);
                let join = &q.joins[0];
                assert!(matches!(join.join_type, JoinType::Inner));
                match &join.source {
                    FromClause::Stream { name, .. } => {
                        assert_eq!(name, "b");
                    }
                    other => panic!("expected Stream, got {other:?}"),
                }
                // Verify ON clause
                match &join.on {
                    Expr::BinaryOp { left, op, right } => {
                        assert!(matches!(op, BinaryOperator::Eq));
                        match left.as_ref() {
                            Expr::Column { table, name } => {
                                assert_eq!(table.as_deref(), Some("a"));
                                assert_eq!(name, "key");
                            }
                            other => panic!("expected Column, got {other:?}"),
                        }
                        match right.as_ref() {
                            Expr::Column { table, name } => {
                                assert_eq!(table.as_deref(), Some("b"));
                                assert_eq!(name, "key");
                            }
                            other => panic!("expected Column, got {other:?}"),
                        }
                    }
                    other => panic!("expected BinaryOp, got {other:?}"),
                }
            }
            other => panic!("expected Query, got {other:?}"),
        }
    }

    #[test]
    fn parse_order_limit() {
        let stmt = parse(r#"SELECT * FROM "orders" ORDER BY timestamp DESC LIMIT 10"#).unwrap();
        match stmt {
            ExqlStatement::Query(q) => {
                assert_eq!(q.order_by.len(), 1);
                assert!(q.order_by[0].descending);
                match &q.order_by[0].expr {
                    Expr::Column { name, .. } => {
                        assert_eq!(name, "timestamp");
                    }
                    other => panic!("expected Column, got {other:?}"),
                }
                assert_eq!(q.limit, Some(10));
                assert!(q.offset.is_none());
            }
            other => panic!("expected Query, got {other:?}"),
        }
    }

    #[test]
    fn parse_create_stream() {
        let stmt =
            parse(r#"CREATE VIEW high_value AS SELECT * FROM "orders" WHERE key = 'x'"#).unwrap();
        match stmt {
            ExqlStatement::CreateStream { name, query, emit } => {
                assert_eq!(name, "high_value");
                assert!(matches!(emit, EmitMode::Changes));
                assert!(query.filter.is_some());
                match &query.from {
                    FromClause::Stream {
                        name: stream_name, ..
                    } => {
                        assert_eq!(stream_name, "orders");
                    }
                    other => panic!("expected Stream, got {other:?}"),
                }
            }
            other => panic!("expected CreateStream, got {other:?}"),
        }
    }

    #[test]
    fn parse_cast_expression() {
        let stmt = parse(r#"SELECT key::int FROM "orders""#).unwrap();
        match stmt {
            ExqlStatement::Query(q) => {
                assert_eq!(q.select.len(), 1);
                match &q.select[0].expr {
                    Expr::Cast { expr, to_type } => {
                        assert!(
                            matches!(expr.as_ref(), Expr::Column { name, .. } if name == "key")
                        );
                        assert!(to_type.to_lowercase().contains("int"));
                    }
                    other => panic!("expected Cast, got {other:?}"),
                }
            }
            other => panic!("expected Query, got {other:?}"),
        }
    }

    #[test]
    fn parse_subquery_in_from() {
        let stmt = parse(r#"SELECT x FROM (SELECT key AS x FROM "orders") AS sub"#).unwrap();
        match stmt {
            ExqlStatement::Query(q) => match &q.from {
                FromClause::Subquery { query, alias } => {
                    assert_eq!(alias, "sub");
                    assert_eq!(query.select.len(), 1);
                }
                other => panic!("expected Subquery, got {other:?}"),
            },
            other => panic!("expected Query, got {other:?}"),
        }
    }

    #[test]
    fn parse_cte() {
        let stmt =
            parse(r#"WITH recent AS (SELECT * FROM "orders" LIMIT 100) SELECT * FROM recent"#)
                .unwrap();
        match stmt {
            ExqlStatement::Query(q) => {
                assert_eq!(q.ctes.len(), 1);
                assert_eq!(q.ctes[0].name, "recent");
                assert_eq!(q.ctes[0].query.limit, Some(100));
            }
            other => panic!("expected Query, got {other:?}"),
        }
    }

    #[test]
    fn parse_case_when() {
        let stmt = parse(r#"SELECT CASE WHEN key = 'a' THEN 1 ELSE 0 END FROM "orders""#).unwrap();
        match stmt {
            ExqlStatement::Query(q) => match &q.select[0].expr {
                Expr::Case {
                    conditions,
                    else_val,
                } => {
                    assert_eq!(conditions.len(), 1);
                    assert!(else_val.is_some());
                }
                other => panic!("expected Case, got {other:?}"),
            },
            other => panic!("expected Query, got {other:?}"),
        }
    }

    #[test]
    fn parse_is_null() {
        let stmt = parse(r#"SELECT * FROM "orders" WHERE key IS NOT NULL"#).unwrap();
        match stmt {
            ExqlStatement::Query(q) => match &q.filter.unwrap() {
                Expr::IsNull { expr, negated } => {
                    assert!(*negated);
                    assert!(matches!(expr.as_ref(), Expr::Column { name, .. } if name == "key"));
                }
                other => panic!("expected IsNull, got {other:?}"),
            },
            other => panic!("expected Query, got {other:?}"),
        }
    }

    #[test]
    fn parse_unsupported_returns_error() {
        let result = parse("INSERT INTO orders VALUES (1, 2, 3)");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            format!("{err}").contains("unsupported"),
            "error should mention unsupported: {err}"
        );
    }
}
