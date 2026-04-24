pub mod ast;
pub mod dialect;
pub mod error;
pub mod transform;

pub use ast::ExqlStatement;
pub use error::ParseError;

use ast::EmitMode;
use dialect::ExspeedDialect;
use sqlparser::parser::Parser;

/// Parse a SQL string into an ExQL AST statement.
///
/// Handles Exspeed-specific extensions before delegating to sqlparser:
/// - `EMIT CHANGES` / `EMIT FINAL` suffix on CREATE VIEW statements
/// - `WITHIN '<duration>'` on JOIN clauses (for stream-stream joins)
pub fn parse(sql: &str) -> Result<ExqlStatement, ParseError> {
    let trimmed = sql.trim();
    let upper = trimmed.to_uppercase();

    // Handle CREATE INDEX before sqlparser (not standard SQL)
    if upper.starts_with("CREATE INDEX") {
        return parse_create_index(trimmed);
    }
    if upper.starts_with("DROP INDEX") {
        return parse_drop_index(trimmed);
    }

    // --- Pre-process: extract EMIT mode ---
    let (sql_no_emit, emit_mode) = extract_emit_mode(sql);

    // --- Pre-process: extract WITHIN clauses from JOINs ---
    let (clean_sql, within_values) = extract_within_clauses(&sql_no_emit);

    let dialect = ExspeedDialect;
    let statements =
        Parser::parse_sql(&dialect, &clean_sql).map_err(|e| {
            let (message, line, column) = error::extract_position(&e.to_string());
            ParseError::Sql { message, line, column }
        })?;
    if statements.is_empty() {
        return Err(ParseError::Sql { message: "empty SQL".into(), line: 1, column: 0 });
    }
    if statements.len() > 1 {
        return Err(ParseError::Unsupported {
            feature: "multiple statements".into(),
            hint: "ExQL supports a single SELECT, CREATE VIEW, CREATE MATERIALIZED VIEW, or DROP STREAM per query".into(),
        });
    }

    let mut stmt = transform::transform_statement(statements.into_iter().next().unwrap())?;

    // --- Post-process: apply extracted EMIT mode ---
    if let Some(mode) = emit_mode {
        if let ExqlStatement::CreateStream { ref mut emit, .. } = stmt {
            *emit = mode;
        }
    }

    // --- Post-process: apply extracted WITHIN values ---
    if !within_values.is_empty() {
        apply_within_values(&mut stmt, &within_values);
    }

    Ok(stmt)
}

/// Extract `EMIT CHANGES` or `EMIT FINAL` from the end of a SQL string.
/// Returns the cleaned SQL and the detected emit mode (if any).
fn extract_emit_mode(sql: &str) -> (String, Option<EmitMode>) {
    let trimmed = sql.trim();

    // Case-insensitive check for trailing EMIT FINAL or EMIT CHANGES
    let upper = trimmed.to_uppercase();
    if upper.ends_with("EMIT FINAL") {
        let cleaned = trimmed[..trimmed.len() - "EMIT FINAL".len()]
            .trim()
            .to_string();
        (cleaned, Some(EmitMode::Final))
    } else if upper.ends_with("EMIT CHANGES") {
        let cleaned = trimmed[..trimmed.len() - "EMIT CHANGES".len()]
            .trim()
            .to_string();
        (cleaned, Some(EmitMode::Changes))
    } else {
        (trimmed.to_string(), None)
    }
}

/// Extract `WITHIN '<duration>'` patterns from JOIN clauses.
/// Returns the cleaned SQL and a vector of extracted WITHIN values in order.
fn extract_within_clauses(sql: &str) -> (String, Vec<String>) {
    let mut within_values = Vec::new();
    let mut result = sql.to_string();

    // Match WITHIN '<value>' or WITHIN "<value>" patterns (case-insensitive)
    // This regex finds WITHIN followed by a quoted string
    loop {
        // Find case-insensitive WITHIN followed by a quoted string
        let upper = result.to_uppercase();
        if let Some(within_pos) = upper.find("WITHIN") {
            let after_within = &result[within_pos + 6..].trim_start();
            if after_within.starts_with('\'') || after_within.starts_with('"') {
                let quote_char = after_within.chars().next().unwrap();
                if let Some(end_quote) = after_within[1..].find(quote_char) {
                    let value = after_within[1..1 + end_quote].to_string();
                    within_values.push(value);

                    // Calculate the full span to remove: from WITHIN to closing quote
                    let after_within_start = within_pos + 6;
                    let trimmed_offset = result[after_within_start..].len() - after_within.len();
                    let value_end = after_within_start + trimmed_offset + 1 + end_quote + 1;
                    result = format!(
                        "{}{}",
                        &result[..within_pos].trim_end(),
                        &result[value_end..]
                    );
                    continue;
                }
            }
        }
        break;
    }

    (result, within_values)
}

/// Apply extracted WITHIN values to JoinClauses in the statement.
fn apply_within_values(stmt: &mut ExqlStatement, within_values: &[String]) {
    let query = match stmt {
        ExqlStatement::Query(ref mut q) => q,
        ExqlStatement::CreateStream { ref mut query, .. } => query,
        ExqlStatement::CreateMaterializedView { ref mut query, .. } => query,
        ExqlStatement::DropStream(_)
        | ExqlStatement::CreateIndex { .. }
        | ExqlStatement::DropIndex(_) => return,
    };

    for (i, join) in query.joins.iter_mut().enumerate() {
        if let Some(val) = within_values.get(i) {
            join.within = Some(val.clone());
        }
    }
}

fn parse_create_index(sql: &str) -> Result<ExqlStatement, ParseError> {
    // CREATE INDEX name ON stream(payload->>'field')
    // CREATE INDEX name ON "stream"(payload->>'field')
    let tokens: Vec<&str> = sql.split_whitespace().collect();
    if tokens.len() < 5 {
        return Err(ParseError::Sql {
            message: "CREATE INDEX requires: CREATE INDEX <name> ON <stream>(<expr>)".into(),
            line: 1,
            column: 0,
        });
    }
    // tokens[0] = CREATE, tokens[1] = INDEX, tokens[2] = name, tokens[3] = ON
    let name = tokens[2].to_string();
    if tokens[3].to_uppercase() != "ON" {
        return Err(ParseError::Sql {
            message: format!("expected ON after index name, found '{}'", tokens[3]),
            line: 1,
            column: 0,
        });
    }
    // Rejoin remaining tokens to handle quoted stream names
    let rest = tokens[4..].join(" ");
    let paren_start = rest.find('(').ok_or_else(|| ParseError::Sql {
        message: "expected (<field_expr>) after stream name".into(),
        line: 1,
        column: 0,
    })?;
    let paren_end = rest.rfind(')').ok_or_else(|| ParseError::Sql {
        message: "expected closing ) in field expression".into(),
        line: 1,
        column: 0,
    })?;
    let stream = rest[..paren_start].trim().trim_matches('"').to_string();
    let field_expr = rest[paren_start + 1..paren_end].trim();

    // Extract field path from expressions like payload->>'field_name'
    let field_path = if field_expr.contains("->>") {
        field_expr
            .split("->>")
            .last()
            .map(|s| s.trim().trim_matches('\'').trim_matches('"').to_string())
            .unwrap_or_else(|| field_expr.to_string())
    } else {
        field_expr.trim_matches('\'').trim_matches('"').to_string()
    };

    Ok(ExqlStatement::CreateIndex {
        name,
        stream,
        field_path,
    })
}

fn parse_drop_index(sql: &str) -> Result<ExqlStatement, ParseError> {
    let tokens: Vec<&str> = sql.split_whitespace().collect();
    if tokens.len() < 3 {
        return Err(ParseError::Sql {
            message: "DROP INDEX requires: DROP INDEX <name>".into(),
            line: 1,
            column: 0,
        });
    }
    Ok(ExqlStatement::DropIndex(
        tokens[2].trim_matches('"').to_string(),
    ))
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

    #[test]
    fn parse_create_materialized_view() {
        let stmt = parse(
            r#"CREATE MATERIALIZED VIEW stats AS SELECT COUNT(*) AS cnt FROM "orders" GROUP BY key"#,
        )
        .unwrap();
        match stmt {
            ExqlStatement::CreateMaterializedView { name, query } => {
                assert_eq!(name, "stats");
                assert_eq!(query.select.len(), 1);
                assert!(matches!(
                    &query.select[0].expr,
                    Expr::Aggregate {
                        func: AggregateFunc::Count,
                        ..
                    }
                ));
                assert_eq!(query.select[0].alias.as_deref(), Some("cnt"));
                assert_eq!(query.group_by.len(), 1);
                match &query.from {
                    FromClause::Stream {
                        name: stream_name, ..
                    } => {
                        assert_eq!(stream_name, "orders");
                    }
                    other => panic!("expected Stream, got {other:?}"),
                }
            }
            other => panic!("expected CreateMaterializedView, got {other:?}"),
        }
    }

    #[test]
    fn parse_emit_final() {
        let stmt = parse(
            r#"CREATE VIEW windowed AS SELECT COUNT(*) FROM "orders" GROUP BY key EMIT FINAL"#,
        )
        .unwrap();
        match stmt {
            ExqlStatement::CreateStream { name, emit, .. } => {
                assert_eq!(name, "windowed");
                assert_eq!(emit, EmitMode::Final);
            }
            other => panic!("expected CreateStream, got {other:?}"),
        }
    }

    #[test]
    fn parse_emit_changes_explicit() {
        let stmt = parse(r#"CREATE VIEW output AS SELECT * FROM "orders" EMIT CHANGES"#).unwrap();
        match stmt {
            ExqlStatement::CreateStream { name, emit, .. } => {
                assert_eq!(name, "output");
                assert_eq!(emit, EmitMode::Changes);
            }
            other => panic!("expected CreateStream, got {other:?}"),
        }
    }

    #[test]
    fn parse_join_within() {
        // WITHIN parsing is supported via pre-processing: the WITHIN clause
        // is extracted from the SQL before passing to sqlparser, then applied
        // to the corresponding JoinClause in the AST.
        let stmt = parse(r#"SELECT * FROM "a" JOIN "b" ON a.key = b.key WITHIN '1 hour'"#).unwrap();
        match stmt {
            ExqlStatement::Query(q) => {
                assert_eq!(q.joins.len(), 1);
                assert_eq!(q.joins[0].within.as_deref(), Some("1 hour"));
            }
            other => panic!("expected Query, got {other:?}"),
        }
    }

    #[test]
    fn parse_join_without_within() {
        // JOIN without WITHIN should have within: None
        let stmt = parse(r#"SELECT * FROM "a" JOIN "b" ON a.key = b.key"#).unwrap();
        match stmt {
            ExqlStatement::Query(q) => {
                assert_eq!(q.joins.len(), 1);
                assert!(q.joins[0].within.is_none());
            }
            other => panic!("expected Query, got {other:?}"),
        }
    }

    #[test]
    fn parse_create_index() {
        let stmt = parse(r#"CREATE INDEX idx_article ON orders(payload->>'article_id')"#).unwrap();
        match stmt {
            ExqlStatement::CreateIndex {
                name,
                stream,
                field_path,
            } => {
                assert_eq!(name, "idx_article");
                assert_eq!(stream, "orders");
                assert_eq!(field_path, "article_id");
            }
            other => panic!("expected CreateIndex, got {other:?}"),
        }
    }

    #[test]
    fn parse_create_index_quoted_stream() {
        let stmt =
            parse(r#"CREATE INDEX idx_region ON "my-stream"(payload->>'region')"#).unwrap();
        match stmt {
            ExqlStatement::CreateIndex {
                name,
                stream,
                field_path,
            } => {
                assert_eq!(name, "idx_region");
                assert_eq!(stream, "my-stream");
                assert_eq!(field_path, "region");
            }
            other => panic!("expected CreateIndex, got {other:?}"),
        }
    }

    #[test]
    fn parse_drop_index() {
        let stmt = parse("DROP INDEX idx_article").unwrap();
        match stmt {
            ExqlStatement::DropIndex(name) => {
                assert_eq!(name, "idx_article");
            }
            other => panic!("expected DropIndex, got {other:?}"),
        }
    }
}
