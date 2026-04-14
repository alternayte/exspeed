//! Transforms sqlparser-rs AST into ExQL AST.
//!
//! This is a recursive tree transformer that walks the sqlparser AST and
//! produces the Exspeed-owned ExQL AST types defined in `ast.rs`.

use sqlparser::ast as sp;

use super::ast::*;
use super::error::ParseError;

// ---------------------------------------------------------------------------
// Top-level entry point
// ---------------------------------------------------------------------------

pub fn transform_statement(stmt: sp::Statement) -> Result<ExqlStatement, ParseError> {
    match stmt {
        sp::Statement::Query(q) => {
            let query = transform_query(*q)?;
            Ok(ExqlStatement::Query(query))
        }
        sp::Statement::CreateView(cv) => {
            let name = object_name_to_string(&cv.name);
            let query = transform_query(*cv.query)?;
            Ok(ExqlStatement::CreateStream {
                name,
                query,
                emit: EmitMode::Changes,
            })
        }
        other => Err(ParseError::Unsupported(format!(
            "statement type: {}",
            short_stmt_name(&other)
        ))),
    }
}

// ---------------------------------------------------------------------------
// Query / Select
// ---------------------------------------------------------------------------

fn transform_query(query: sp::Query) -> Result<QueryExpr, ParseError> {
    // Extract CTEs
    let ctes = match query.with {
        Some(with) => with
            .cte_tables
            .into_iter()
            .map(transform_cte)
            .collect::<Result<Vec<_>, _>>()?,
        None => vec![],
    };

    // Extract ORDER BY
    let order_by_items = match query.order_by {
        Some(ob) => match ob.kind {
            sp::OrderByKind::Expressions(exprs) => exprs
                .into_iter()
                .map(transform_order_by_expr)
                .collect::<Result<Vec<_>, _>>()?,
            sp::OrderByKind::All(_) => {
                return Err(ParseError::Unsupported("ORDER BY ALL".into()));
            }
        },
        None => vec![],
    };

    // Extract LIMIT / OFFSET
    let (limit, offset) = match query.limit_clause {
        Some(sp::LimitClause::LimitOffset { limit, offset, .. }) => {
            let l = limit.map(expr_to_u64).transpose()?;
            let o = offset.map(|o| expr_to_u64(o.value)).transpose()?;
            (l, o)
        }
        Some(sp::LimitClause::OffsetCommaLimit {
            offset: off_expr,
            limit: lim_expr,
        }) => {
            let l = Some(expr_to_u64(lim_expr)?);
            let o = Some(expr_to_u64(off_expr)?);
            (l, o)
        }
        None => (None, None),
    };

    // Transform the body (SELECT, set operations, etc.)
    match *query.body {
        sp::SetExpr::Select(select) => {
            transform_select(*select, ctes, order_by_items, limit, offset)
        }
        sp::SetExpr::Query(inner_query) => {
            // Parenthesized subquery — recurse
            let mut result = transform_query(*inner_query)?;
            // Merge outer CTEs, order_by, limit, offset if present
            if !ctes.is_empty() {
                result.ctes = ctes;
            }
            if !order_by_items.is_empty() {
                result.order_by = order_by_items;
            }
            if limit.is_some() {
                result.limit = limit;
            }
            if offset.is_some() {
                result.offset = offset;
            }
            Ok(result)
        }
        other => Err(ParseError::Unsupported(format!("query body type: {other}"))),
    }
}

fn transform_select(
    select: sp::Select,
    ctes: Vec<Cte>,
    order_by: Vec<OrderByItem>,
    limit: Option<u64>,
    offset: Option<u64>,
) -> Result<QueryExpr, ParseError> {
    // Projection
    let select_items = select
        .projection
        .into_iter()
        .map(transform_select_item)
        .collect::<Result<Vec<_>, _>>()?;

    // FROM + JOINs
    let (from, joins) = if select.from.is_empty() {
        // No FROM clause — use a dummy stream name
        (
            FromClause::Stream {
                name: "__dual".into(),
                alias: None,
            },
            vec![],
        )
    } else {
        // We support a single FROM item (potentially with joins)
        let mut from_iter = select.from.into_iter();
        let first = from_iter.next().unwrap();
        let (from_clause, join_clauses) = transform_from(first)?;

        // Additional FROM items become implicit CROSS JOINs — unsupported for now,
        // but we can support them later. For simplicity, treat additional FROM items
        // as an error if they have joins, or merge them.
        if let Some(extra) = from_iter.next() {
            return Err(ParseError::Unsupported(format!(
                "multiple FROM items (implicit cross join): {extra}"
            )));
        }

        (from_clause, join_clauses)
    };

    // WHERE
    let filter = select.selection.map(transform_expr).transpose()?;

    // GROUP BY
    let group_by = match select.group_by {
        sp::GroupByExpr::Expressions(exprs, _modifiers) => exprs
            .into_iter()
            .map(transform_expr)
            .collect::<Result<Vec<_>, _>>()?,
        sp::GroupByExpr::All(_) => {
            return Err(ParseError::Unsupported("GROUP BY ALL".into()));
        }
    };

    Ok(QueryExpr {
        ctes,
        select: select_items,
        from,
        joins,
        filter,
        group_by,
        order_by,
        limit,
        offset,
    })
}

// ---------------------------------------------------------------------------
// CTE
// ---------------------------------------------------------------------------

fn transform_cte(cte: sp::Cte) -> Result<Cte, ParseError> {
    let name = cte.alias.name.value;
    let query = transform_query(*cte.query)?;
    Ok(Cte {
        name,
        query: Box::new(query),
    })
}

// ---------------------------------------------------------------------------
// SELECT items
// ---------------------------------------------------------------------------

fn transform_select_item(item: sp::SelectItem) -> Result<SelectItem, ParseError> {
    match item {
        sp::SelectItem::UnnamedExpr(expr) => {
            let e = transform_expr(expr)?;
            Ok(SelectItem {
                expr: e,
                alias: None,
            })
        }
        sp::SelectItem::ExprWithAlias { expr, alias } => {
            let e = transform_expr(expr)?;
            Ok(SelectItem {
                expr: e,
                alias: Some(alias.value),
            })
        }
        sp::SelectItem::Wildcard(_options) => Ok(SelectItem {
            expr: Expr::Wildcard { table: None },
            alias: None,
        }),
        sp::SelectItem::QualifiedWildcard(kind, _options) => {
            let table = match kind {
                sp::SelectItemQualifiedWildcardKind::ObjectName(name) => {
                    Some(object_name_to_string(&name))
                }
                sp::SelectItemQualifiedWildcardKind::Expr(_) => {
                    return Err(ParseError::Unsupported("expression wildcard".into()));
                }
            };
            Ok(SelectItem {
                expr: Expr::Wildcard { table },
                alias: None,
            })
        }
    }
}

// ---------------------------------------------------------------------------
// FROM clause
// ---------------------------------------------------------------------------

fn transform_from(twj: sp::TableWithJoins) -> Result<(FromClause, Vec<JoinClause>), ParseError> {
    let from = transform_table_factor(twj.relation)?;
    let joins = twj
        .joins
        .into_iter()
        .map(transform_join)
        .collect::<Result<Vec<_>, _>>()?;
    Ok((from, joins))
}

fn transform_table_factor(tf: sp::TableFactor) -> Result<FromClause, ParseError> {
    match tf {
        sp::TableFactor::Table {
            name, alias, args, ..
        } => {
            // Check if this is a table-valued function (e.g., external(), postgres(), mssql())
            if let Some(table_fn_args) = args {
                return transform_table_function(&name, alias, table_fn_args);
            }

            let table_name = object_name_to_string(&name);
            let alias_str = alias.map(|a| a.name.value);
            Ok(FromClause::Stream {
                name: table_name,
                alias: alias_str,
            })
        }
        sp::TableFactor::Derived {
            subquery, alias, ..
        } => {
            let query = transform_query(*subquery)?;
            let alias_name = alias
                .map(|a| a.name.value)
                .unwrap_or_else(|| "__subquery".into());
            Ok(FromClause::Subquery {
                query: Box::new(query),
                alias: alias_name,
            })
        }
        sp::TableFactor::TableFunction { expr, alias } => {
            // TABLE(<expr>) syntax — try to extract function call
            transform_table_function_expr(expr, alias)
        }
        sp::TableFactor::Function {
            name, args, alias, ..
        } => {
            // LATERAL FLATTEN or similar — try to map as external
            let fn_name = object_name_to_string(&name).to_lowercase();
            let alias_str = alias.map(|a| a.name.value);
            match fn_name.as_str() {
                "external" | "postgres" | "mssql" => {
                    let driver = match fn_name.as_str() {
                        "postgres" | "mssql" => Some(fn_name.clone()),
                        _ => None,
                    };
                    let fn_args = extract_fn_args_from_vec(args)?;
                    if fn_args.len() < 2 {
                        return Err(ParseError::Transform(format!(
                            "{fn_name}() requires at least 2 arguments"
                        )));
                    }
                    let connection = expr_to_string_literal(&fn_args[0])?;
                    let table = expr_to_string_literal(&fn_args[1])?;
                    Ok(FromClause::External {
                        connection,
                        table,
                        alias: alias_str,
                        driver,
                    })
                }
                _ => Err(ParseError::Unsupported(format!(
                    "table function: {fn_name}"
                ))),
            }
        }
        other => Err(ParseError::Unsupported(format!(
            "FROM clause type: {other}"
        ))),
    }
}

fn transform_table_function(
    name: &sp::ObjectName,
    alias: Option<sp::TableAlias>,
    table_fn_args: sp::TableFunctionArgs,
) -> Result<FromClause, ParseError> {
    let fn_name = object_name_to_string(name).to_lowercase();
    let alias_str = alias.map(|a| a.name.value);

    match fn_name.as_str() {
        "external" | "postgres" | "mssql" => {
            let driver = match fn_name.as_str() {
                "postgres" | "mssql" => Some(fn_name.clone()),
                _ => None,
            };
            let args = extract_fn_args_from_table_fn_args(table_fn_args)?;
            if args.len() < 2 {
                return Err(ParseError::Transform(format!(
                    "{fn_name}() requires at least 2 arguments"
                )));
            }
            let connection = expr_to_string_literal(&args[0])?;
            let table = expr_to_string_literal(&args[1])?;
            Ok(FromClause::External {
                connection,
                table,
                alias: alias_str,
                driver,
            })
        }
        _ => Err(ParseError::Unsupported(format!(
            "table function: {fn_name}"
        ))),
    }
}

fn transform_table_function_expr(
    expr: sp::Expr,
    alias: Option<sp::TableAlias>,
) -> Result<FromClause, ParseError> {
    // TABLE(fn(...)) — extract the function call
    if let sp::Expr::Function(func) = expr {
        let fn_name = object_name_to_string(&func.name).to_lowercase();
        let alias_str = alias.map(|a| a.name.value);

        match fn_name.as_str() {
            "external" | "postgres" | "mssql" => {
                let driver = match fn_name.as_str() {
                    "postgres" | "mssql" => Some(fn_name.clone()),
                    _ => None,
                };
                let args = extract_function_call_args(func.args)?;
                if args.len() < 2 {
                    return Err(ParseError::Transform(format!(
                        "{fn_name}() requires at least 2 arguments"
                    )));
                }
                let connection = expr_to_string_literal(&args[0])?;
                let table = expr_to_string_literal(&args[1])?;
                Ok(FromClause::External {
                    connection,
                    table,
                    alias: alias_str,
                    driver,
                })
            }
            _ => Err(ParseError::Unsupported(format!(
                "TABLE function: {fn_name}"
            ))),
        }
    } else {
        Err(ParseError::Unsupported(
            "TABLE() with non-function expression".into(),
        ))
    }
}

// ---------------------------------------------------------------------------
// JOIN
// ---------------------------------------------------------------------------

fn transform_join(join: sp::Join) -> Result<JoinClause, ParseError> {
    let (join_type, constraint) = match join.join_operator {
        sp::JoinOperator::Inner(c) => (JoinType::Inner, c),
        sp::JoinOperator::Join(c) => (JoinType::Inner, c),
        sp::JoinOperator::Left(c) => (JoinType::Left, c),
        sp::JoinOperator::LeftOuter(c) => (JoinType::Left, c),
        other => {
            return Err(ParseError::Unsupported(format!("join type: {other:?}")));
        }
    };

    let on_expr = match constraint {
        sp::JoinConstraint::On(expr) => transform_expr(expr)?,
        sp::JoinConstraint::Using(_) => {
            return Err(ParseError::Unsupported("JOIN USING".into()));
        }
        sp::JoinConstraint::Natural => {
            return Err(ParseError::Unsupported("NATURAL JOIN".into()));
        }
        sp::JoinConstraint::None => {
            return Err(ParseError::Transform("JOIN without ON clause".into()));
        }
    };

    let source = transform_table_factor(join.relation)?;

    Ok(JoinClause {
        join_type,
        source,
        on: on_expr,
    })
}

// ---------------------------------------------------------------------------
// ORDER BY
// ---------------------------------------------------------------------------

fn transform_order_by_expr(obe: sp::OrderByExpr) -> Result<OrderByItem, ParseError> {
    let expr = transform_expr(obe.expr)?;
    let descending = match obe.options.asc {
        Some(true) => false,
        Some(false) => true,
        None => false, // default ASC
    };
    Ok(OrderByItem { expr, descending })
}

// ---------------------------------------------------------------------------
// Expression transformer (recursive)
// ---------------------------------------------------------------------------

fn transform_expr(expr: sp::Expr) -> Result<Expr, ParseError> {
    match expr {
        // Simple identifier → Column
        sp::Expr::Identifier(ident) => Ok(Expr::Column {
            table: None,
            name: ident.value,
        }),

        // Qualified identifier → Column with table
        sp::Expr::CompoundIdentifier(parts) => {
            if parts.len() == 2 {
                Ok(Expr::Column {
                    table: Some(parts[0].value.clone()),
                    name: parts[1].value.clone(),
                })
            } else if parts.len() == 1 {
                Ok(Expr::Column {
                    table: None,
                    name: parts[0].value.clone(),
                })
            } else {
                // schema.table.column — join last two, use the rest as context
                let name = parts.last().unwrap().value.clone();
                let table = parts[parts.len() - 2].value.clone();
                Ok(Expr::Column {
                    table: Some(table),
                    name,
                })
            }
        }

        // Literal value
        sp::Expr::Value(val_with_span) => transform_value(val_with_span.value),

        // Binary operation
        sp::Expr::BinaryOp { left, op, right } => {
            // Handle JSON operators specially
            match op {
                sp::BinaryOperator::Arrow => {
                    // -> operator: JSON field access (returns JSON)
                    let base = transform_expr(*left)?;
                    let field = extract_json_field(*right)?;
                    Ok(Expr::JsonAccess {
                        expr: Box::new(base),
                        field,
                        as_text: false,
                    })
                }
                sp::BinaryOperator::LongArrow => {
                    // ->> operator: JSON field access (returns text)
                    let base = transform_expr(*left)?;
                    let field = extract_json_field(*right)?;
                    Ok(Expr::JsonAccess {
                        expr: Box::new(base),
                        field,
                        as_text: true,
                    })
                }
                _ => {
                    let l = transform_expr(*left)?;
                    let r = transform_expr(*right)?;
                    let binop = transform_binary_op(op)?;
                    Ok(Expr::BinaryOp {
                        left: Box::new(l),
                        op: binop,
                        right: Box::new(r),
                    })
                }
            }
        }

        // Unary operation
        sp::Expr::UnaryOp { op, expr } => {
            let e = transform_expr(*expr)?;
            let uop = transform_unary_op(op)?;
            Ok(Expr::UnaryOp {
                op: uop,
                expr: Box::new(e),
            })
        }

        // Function call
        sp::Expr::Function(func) => transform_function(func),

        // LIKE
        sp::Expr::Like {
            negated,
            expr,
            pattern,
            ..
        } => {
            let left = transform_expr(*expr)?;
            let right = transform_expr(*pattern)?;
            let like_expr = Expr::BinaryOp {
                left: Box::new(left),
                op: BinaryOperator::Like,
                right: Box::new(right),
            };
            if negated {
                Ok(Expr::UnaryOp {
                    op: UnaryOperator::Not,
                    expr: Box::new(like_expr),
                })
            } else {
                Ok(like_expr)
            }
        }

        // IS NULL / IS NOT NULL
        sp::Expr::IsNull(e) => {
            let inner = transform_expr(*e)?;
            Ok(Expr::IsNull {
                expr: Box::new(inner),
                negated: false,
            })
        }
        sp::Expr::IsNotNull(e) => {
            let inner = transform_expr(*e)?;
            Ok(Expr::IsNull {
                expr: Box::new(inner),
                negated: true,
            })
        }

        // IN list
        sp::Expr::InList {
            expr,
            list,
            negated,
        } => {
            let e = transform_expr(*expr)?;
            let items = list
                .into_iter()
                .map(transform_expr)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Expr::InList {
                expr: Box::new(e),
                list: items,
                negated,
            })
        }

        // BETWEEN
        sp::Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let e = transform_expr(*expr)?;
            let lo = transform_expr(*low)?;
            let hi = transform_expr(*high)?;
            Ok(Expr::Between {
                expr: Box::new(e),
                low: Box::new(lo),
                high: Box::new(hi),
                negated,
            })
        }

        // CAST (including :: double-colon casts)
        sp::Expr::Cast {
            expr, data_type, ..
        } => {
            let e = transform_expr(*expr)?;
            let to_type = format!("{data_type}");
            Ok(Expr::Cast {
                expr: Box::new(e),
                to_type,
            })
        }

        // CASE / WHEN
        sp::Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if operand.is_some() {
                return Err(ParseError::Unsupported(
                    "simple CASE (CASE <operand> WHEN ...)".into(),
                ));
            }
            let conds = conditions
                .into_iter()
                .map(|cw| {
                    let cond = transform_expr(cw.condition)?;
                    let result = transform_expr(cw.result)?;
                    Ok((cond, result))
                })
                .collect::<Result<Vec<_>, ParseError>>()?;
            let else_val = else_result
                .map(|e| transform_expr(*e))
                .transpose()?
                .map(Box::new);
            Ok(Expr::Case {
                conditions: conds,
                else_val,
            })
        }

        // Subquery expression
        sp::Expr::Subquery(q) => {
            let query = transform_query(*q)?;
            Ok(Expr::Subquery(Box::new(query)))
        }

        // Nested (parenthesized) expression — unwrap
        sp::Expr::Nested(inner) => transform_expr(*inner),

        // Wildcard
        sp::Expr::Wildcard(_) => Ok(Expr::Wildcard { table: None }),

        // Qualified wildcard (table.*)
        sp::Expr::QualifiedWildcard(name, _) => Ok(Expr::Wildcard {
            table: Some(object_name_to_string(&name)),
        }),

        // Interval
        sp::Expr::Interval(interval) => {
            let val = format!("{interval}");
            Ok(Expr::Interval { value: val })
        }

        // JsonAccess (Snowflake-style path access)
        sp::Expr::JsonAccess { value, path } => {
            let base = transform_expr(*value)?;
            // Convert path to string representation
            let field = format!("{path}");
            Ok(Expr::JsonAccess {
                expr: Box::new(base),
                field,
                as_text: false,
            })
        }

        // CompoundFieldAccess (array/map subscript access)
        sp::Expr::CompoundFieldAccess { root, access_chain } => {
            let base = transform_expr(*root)?;
            // Convert access chain to string — best-effort
            let field = access_chain
                .iter()
                .map(|a| format!("{a}"))
                .collect::<Vec<_>>()
                .join("");
            Ok(Expr::JsonAccess {
                expr: Box::new(base),
                field,
                as_text: false,
            })
        }

        // Anything else
        other => Err(ParseError::Unsupported(format!("expression type: {other}"))),
    }
}

// ---------------------------------------------------------------------------
// Function calls (including aggregates)
// ---------------------------------------------------------------------------

fn transform_function(func: sp::Function) -> Result<Expr, ParseError> {
    let fn_name = object_name_to_string(&func.name);
    let fn_name_upper = fn_name.to_uppercase();

    // Check for aggregates
    let agg_func = match fn_name_upper.as_str() {
        "COUNT" => Some(AggregateFunc::Count),
        "SUM" => Some(AggregateFunc::Sum),
        "AVG" => Some(AggregateFunc::Avg),
        "MIN" => Some(AggregateFunc::Min),
        "MAX" => Some(AggregateFunc::Max),
        _ => None,
    };

    let (args, distinct) = extract_function_call_args_with_distinct(func.args)?;

    if let Some(agg) = agg_func {
        // Aggregate function
        let expr = if args.is_empty() {
            // COUNT(*) etc.
            Expr::Wildcard { table: None }
        } else {
            args.into_iter().next().unwrap()
        };
        return Ok(Expr::Aggregate {
            func: agg,
            expr: Box::new(expr),
            distinct,
        });
    }

    // Regular function
    Ok(Expr::Function {
        name: fn_name,
        args,
    })
}

// ---------------------------------------------------------------------------
// Binary / Unary operator mapping
// ---------------------------------------------------------------------------

fn transform_binary_op(op: sp::BinaryOperator) -> Result<BinaryOperator, ParseError> {
    match op {
        sp::BinaryOperator::Eq => Ok(BinaryOperator::Eq),
        sp::BinaryOperator::NotEq => Ok(BinaryOperator::Neq),
        sp::BinaryOperator::Lt => Ok(BinaryOperator::Lt),
        sp::BinaryOperator::Gt => Ok(BinaryOperator::Gt),
        sp::BinaryOperator::LtEq => Ok(BinaryOperator::Lte),
        sp::BinaryOperator::GtEq => Ok(BinaryOperator::Gte),
        sp::BinaryOperator::And => Ok(BinaryOperator::And),
        sp::BinaryOperator::Or => Ok(BinaryOperator::Or),
        sp::BinaryOperator::Plus => Ok(BinaryOperator::Add),
        sp::BinaryOperator::Minus => Ok(BinaryOperator::Sub),
        sp::BinaryOperator::Multiply => Ok(BinaryOperator::Mul),
        sp::BinaryOperator::Divide => Ok(BinaryOperator::Div),
        sp::BinaryOperator::Modulo => Ok(BinaryOperator::Mod),
        other => Err(ParseError::Unsupported(format!("binary operator: {other}"))),
    }
}

fn transform_unary_op(op: sp::UnaryOperator) -> Result<UnaryOperator, ParseError> {
    match op {
        sp::UnaryOperator::Not => Ok(UnaryOperator::Not),
        sp::UnaryOperator::Minus => Ok(UnaryOperator::Neg),
        other => Err(ParseError::Unsupported(format!("unary operator: {other}"))),
    }
}

// ---------------------------------------------------------------------------
// Value transformation
// ---------------------------------------------------------------------------

fn transform_value(val: sp::Value) -> Result<Expr, ParseError> {
    match val {
        sp::Value::Number(s, _long) => {
            let s_str = s.to_string();
            // Try integer first, then float
            if let Ok(i) = s_str.parse::<i64>() {
                Ok(Expr::Literal(LiteralValue::Int(i)))
            } else if let Ok(f) = s_str.parse::<f64>() {
                Ok(Expr::Literal(LiteralValue::Float(f)))
            } else {
                Err(ParseError::Transform(format!(
                    "invalid number literal: {s_str}"
                )))
            }
        }
        sp::Value::SingleQuotedString(s) => Ok(Expr::Literal(LiteralValue::String(s))),
        sp::Value::DoubleQuotedString(s) => Ok(Expr::Literal(LiteralValue::String(s))),
        sp::Value::Boolean(b) => Ok(Expr::Literal(LiteralValue::Bool(b))),
        sp::Value::Null => Ok(Expr::Literal(LiteralValue::Null)),
        other => Err(ParseError::Unsupported(format!("value type: {other}"))),
    }
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

/// Convert an ObjectName to a plain string (joining parts with `.`).
fn object_name_to_string(name: &sp::ObjectName) -> String {
    name.0
        .iter()
        .map(|part| match part {
            sp::ObjectNamePart::Identifier(ident) => ident.value.clone(),
            sp::ObjectNamePart::Function(f) => f.name.value.clone(),
        })
        .collect::<Vec<_>>()
        .join(".")
}

/// Extract a u64 from a simple integer expression.
fn expr_to_u64(expr: sp::Expr) -> Result<u64, ParseError> {
    match expr {
        sp::Expr::Value(vws) => match vws.value {
            sp::Value::Number(s, _) => {
                let s_str = s.to_string();
                s_str.parse::<u64>().map_err(|_| {
                    ParseError::Transform(format!(
                        "expected integer for LIMIT/OFFSET, got: {s_str}"
                    ))
                })
            }
            other => Err(ParseError::Transform(format!(
                "expected number for LIMIT/OFFSET, got: {other}"
            ))),
        },
        other => Err(ParseError::Transform(format!(
            "expected literal for LIMIT/OFFSET, got: {other}"
        ))),
    }
}

/// Extract a string literal from an ExQL Expr.
fn expr_to_string_literal(expr: &Expr) -> Result<String, ParseError> {
    match expr {
        Expr::Literal(LiteralValue::String(s)) => Ok(s.clone()),
        other => Err(ParseError::Transform(format!(
            "expected string literal, got: {other:?}"
        ))),
    }
}

/// Extract the JSON field name from an expression (used with -> and ->>).
fn extract_json_field(expr: sp::Expr) -> Result<String, ParseError> {
    match expr {
        sp::Expr::Value(vws) => match vws.value {
            sp::Value::SingleQuotedString(s) => Ok(s),
            sp::Value::Number(n, _) => Ok(n.to_string()),
            other => Err(ParseError::Transform(format!(
                "expected string/number for JSON field, got: {other}"
            ))),
        },
        sp::Expr::Identifier(ident) => Ok(ident.value),
        other => Err(ParseError::Transform(format!(
            "expected simple JSON field key, got: {other}"
        ))),
    }
}

/// Extract function arguments from FunctionArguments, returning transformed
/// ExQL expressions and a flag for DISTINCT.
fn extract_function_call_args_with_distinct(
    args: sp::FunctionArguments,
) -> Result<(Vec<Expr>, bool), ParseError> {
    match args {
        sp::FunctionArguments::None => Ok((vec![], false)),
        sp::FunctionArguments::Subquery(q) => {
            let query = transform_query(*q)?;
            Ok((vec![Expr::Subquery(Box::new(query))], false))
        }
        sp::FunctionArguments::List(list) => {
            let distinct = matches!(
                list.duplicate_treatment,
                Some(sp::DuplicateTreatment::Distinct)
            );
            let exprs = list
                .args
                .into_iter()
                .map(|arg| match arg {
                    sp::FunctionArg::Unnamed(fae) => transform_function_arg_expr(fae),
                    sp::FunctionArg::Named { arg, .. } => transform_function_arg_expr(arg),
                    sp::FunctionArg::ExprNamed { arg, .. } => transform_function_arg_expr(arg),
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok((exprs, distinct))
        }
    }
}

/// Extract function arguments without distinct flag.
fn extract_function_call_args(args: sp::FunctionArguments) -> Result<Vec<Expr>, ParseError> {
    let (exprs, _) = extract_function_call_args_with_distinct(args)?;
    Ok(exprs)
}

fn transform_function_arg_expr(fae: sp::FunctionArgExpr) -> Result<Expr, ParseError> {
    match fae {
        sp::FunctionArgExpr::Expr(e) => transform_expr(e),
        sp::FunctionArgExpr::Wildcard => Ok(Expr::Wildcard { table: None }),
        sp::FunctionArgExpr::QualifiedWildcard(name) => Ok(Expr::Wildcard {
            table: Some(object_name_to_string(&name)),
        }),
    }
}

/// Extract ExQL Exprs from TableFunctionArgs.
fn extract_fn_args_from_table_fn_args(tfa: sp::TableFunctionArgs) -> Result<Vec<Expr>, ParseError> {
    tfa.args
        .into_iter()
        .map(|arg| match arg {
            sp::FunctionArg::Unnamed(fae) => transform_function_arg_expr(fae),
            sp::FunctionArg::Named { arg, .. } => transform_function_arg_expr(arg),
            sp::FunctionArg::ExprNamed { arg, .. } => transform_function_arg_expr(arg),
        })
        .collect()
}

/// Extract ExQL Exprs from a Vec<FunctionArg> (used in TableFactor::Function).
fn extract_fn_args_from_vec(args: Vec<sp::FunctionArg>) -> Result<Vec<Expr>, ParseError> {
    args.into_iter()
        .map(|arg| match arg {
            sp::FunctionArg::Unnamed(fae) => transform_function_arg_expr(fae),
            sp::FunctionArg::Named { arg, .. } => transform_function_arg_expr(arg),
            sp::FunctionArg::ExprNamed { arg, .. } => transform_function_arg_expr(arg),
        })
        .collect()
}

/// Short description of a statement for error messages.
fn short_stmt_name(stmt: &sp::Statement) -> &'static str {
    match stmt {
        sp::Statement::Query(_) => "Query",
        sp::Statement::Insert(_) => "Insert",
        sp::Statement::Update { .. } => "Update",
        sp::Statement::Delete(_) => "Delete",
        sp::Statement::CreateTable(_) => "CreateTable",
        sp::Statement::CreateView(_) => "CreateView",
        sp::Statement::Drop { .. } => "Drop",
        _ => "Other",
    }
}
