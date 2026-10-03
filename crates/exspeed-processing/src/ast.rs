//! Parsing and AST rewrites applied before DataFusion plans a query.

use std::ops::ControlFlow;

use datafusion::execution::session_state::SessionState;
use datafusion::logical_expr::LogicalPlan;
use datafusion::sql::parser::{DFParser, Statement as DFStatement};
use datafusion::sql::sqlparser::ast::{visit_expressions_mut, BinaryOperator};
use datafusion::sql::sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, Ident, OrderByKind,
    SelectItem, SetExpr, Statement as SQLStatement,
};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;

use crate::error::ExqlError;
use crate::udfs::{WEND, WSTART};

/// Aggregates and math functions whose JSON-text argument is read as a
/// number (planned before type coercion would reject `avg(Utf8)`).
const NUMERIC_FUNCS: &[&str] = &[
    "sum",
    "avg",
    "mean",
    "min",
    "max",
    "stddev",
    "stddev_pop",
    "stddev_samp",
    "var",
    "var_pop",
    "var_samp",
    "variance",
    "median",
    "approx_median",
    "abs",
    "round",
    "ceil",
    "floor",
    "sqrt",
    "cbrt",
    "power",
    "pow",
    "ln",
    "log",
    "log2",
    "log10",
    "exp",
    "signum",
    "trunc",
];

fn parse_expr(sql: &str) -> Result<Expr, ExqlError> {
    Ok(Parser::new(&GenericDialect {})
        .try_with_sql(sql)?
        .parse_expr()?)
}

/// Parse one (already normalized) statement with DataFusion's parser.
pub fn parse_one(sql: &str) -> Result<DFStatement, ExqlError> {
    let mut stmts = DFParser::parse_sql_with_dialect(sql, &GenericDialect {})?;
    if stmts.len() != 1 {
        return Err(ExqlError::parse("expected exactly one statement"));
    }
    Ok(stmts.pop_front().unwrap())
}

fn is_arrow(e: &Expr) -> bool {
    match e {
        Expr::Nested(inner) => is_arrow(inner),
        Expr::BinaryOp { op, .. } => {
            matches!(op, BinaryOperator::Arrow | BinaryOperator::LongArrow)
        }
        _ => false,
    }
}

fn sql_statements(stmt: &mut DFStatement) -> Vec<&mut SQLStatement> {
    match stmt {
        DFStatement::Statement(s) => vec![s.as_mut()],
        DFStatement::Explain(e) => sql_statements(e.statement.as_mut()),
        _ => vec![],
    }
}

/// `AVG(payload->>'x')` → `AVG(TRY_CAST(payload->>'x' AS DOUBLE))`, and the
/// same for operands of `+ - * / %`.
pub fn rewrite_json_numeric_args(stmt: &mut DFStatement) -> Result<(), ExqlError> {
    let mut err: Option<ExqlError> = None;
    for s in sql_statements(stmt) {
        let _ = visit_expressions_mut(s, |e: &mut Expr| {
            if let Expr::BinaryOp { left, op, right } = e {
                // Arithmetic is type-checked while planning: cast JSON text
                // operands to numbers up front.
                if matches!(
                    op,
                    BinaryOperator::Plus
                        | BinaryOperator::Minus
                        | BinaryOperator::Multiply
                        | BinaryOperator::Divide
                        | BinaryOperator::Modulo
                ) {
                    for side in [left, right] {
                        if is_arrow(side) {
                            match parse_expr(&format!("TRY_CAST({side} AS DOUBLE)")) {
                                Ok(new) => **side = new,
                                Err(e) => err = Some(e),
                            }
                        }
                    }
                }
                return ControlFlow::<()>::Continue(());
            }
            let Expr::Function(f) = e else {
                return ControlFlow::<()>::Continue(());
            };
            let name = f
                .name
                .0
                .last()
                .map(|p| p.to_string().to_ascii_lowercase())
                .unwrap_or_default();
            if !NUMERIC_FUNCS.contains(&name.as_str()) {
                return ControlFlow::Continue(());
            }
            let FunctionArguments::List(list) = &mut f.args else {
                return ControlFlow::Continue(());
            };
            if let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(arg))) = list.args.first_mut() {
                if is_arrow(arg) {
                    match parse_expr(&format!("TRY_CAST({arg} AS DOUBLE)")) {
                        Ok(new) => *arg = new,
                        Err(e) => err = Some(e),
                    }
                }
            }
            ControlFlow::Continue(())
        });
    }
    match err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// `SELECT DISTINCT e … ORDER BY e` → `ORDER BY <position of e>`.
/// DataFusion can't match the (aliased) JSON operator expressions in ORDER
/// BY back to a DISTINCT select list.
pub fn rewrite_distinct_order_by(stmt: &mut DFStatement) -> Result<(), ExqlError> {
    for s in sql_statements(stmt) {
        let SQLStatement::Query(q) = s else {
            continue;
        };
        let Some(ob) = q.order_by.as_mut() else {
            continue;
        };
        let SetExpr::Select(sel) = q.body.as_ref() else {
            continue;
        };
        if sel.distinct.is_none() {
            continue;
        }
        let OrderByKind::Expressions(exprs) = &mut ob.kind else {
            continue;
        };
        for o in exprs.iter_mut() {
            let key = o.expr.to_string();
            let pos = sel.projection.iter().position(|item| match item {
                SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
                    e.to_string() == key
                }
                _ => false,
            });
            if let Some(i) = pos {
                o.expr = parse_expr(&(i + 1).to_string())?;
            }
        }
    }
    Ok(())
}

fn unnest(e: &Expr) -> &Expr {
    match e {
        Expr::Nested(inner) => unnest(inner),
        other => other,
    }
}

/// Whether a JSON-operator chain starts at a qualified column (`a.payload`).
fn qualified_arrow(e: &Expr) -> bool {
    match unnest(e) {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Arrow | BinaryOperator::LongArrow,
            ..
        } => match unnest(left) {
            Expr::CompoundIdentifier(_) => true,
            other => qualified_arrow(other),
        },
        _ => false,
    }
}

/// Name unaliased `a.payload->>'x'` select items `a.payload ->> 'x'`, so
/// `SELECT a.payload->>'id', b.payload->>'id'` doesn't produce two columns
/// with the same name.
pub fn alias_qualified_json(stmt: &mut DFStatement) {
    for s in sql_statements(stmt) {
        let SQLStatement::Query(q) = s else {
            continue;
        };
        let SetExpr::Select(sel) = q.body.as_mut() else {
            continue;
        };
        for item in sel.projection.iter_mut() {
            if let SelectItem::UnnamedExpr(e) = item {
                if qualified_arrow(e) {
                    let alias = Ident::with_quote('"', unnest(e).to_string());
                    *item = SelectItem::ExprWithAlias {
                        expr: e.clone(),
                        alias,
                    };
                }
            }
        }
    }
}

fn marker_for(ident: &Ident) -> Option<&'static str> {
    if ident.quote_style.is_some() {
        return None;
    }
    match ident.value.to_ascii_lowercase().as_str() {
        "window_start" | "windowstart" => Some(WSTART),
        "window_end" | "windowend" => Some(WEND),
        _ => None,
    }
}

fn marker_call(name: &str) -> Result<Expr, ExqlError> {
    parse_expr(&format!("{name}()"))
}

/// For windowed continuous queries: turn `window_start` / `window_end`
/// into marker calls, alias them in the select list, and make both part of
/// the GROUP BY.
pub fn rewrite_window_markers(stmt: &mut DFStatement) -> Result<(), ExqlError> {
    let DFStatement::Statement(s) = stmt else {
        return Err(ExqlError::Plan("expected a SELECT".into()));
    };
    let SQLStatement::Query(q) = s.as_mut() else {
        return Err(ExqlError::Plan("expected a SELECT".into()));
    };
    let SetExpr::Select(select) = q.body.as_mut() else {
        return Err(ExqlError::unsupported(
            "WINDOW on a set operation",
            "apply WINDOW to a plain SELECT",
        ));
    };
    // Select items that are exactly `window_start` keep that name.
    for item in select.projection.iter_mut() {
        if let SelectItem::UnnamedExpr(Expr::Identifier(id)) = item {
            if let Some(m) = marker_for(id) {
                let alias = Ident::new(id.value.to_ascii_lowercase());
                *item = SelectItem::ExprWithAlias {
                    expr: marker_call(m)?,
                    alias,
                };
            }
        }
    }
    let start = marker_call(WSTART)?;
    let end = marker_call(WEND)?;
    let _ = visit_expressions_mut(select.as_mut(), |e: &mut Expr| {
        if let Expr::Identifier(id) = e {
            if let Some(m) = marker_for(id) {
                *e = if m == WSTART {
                    start.clone()
                } else {
                    end.clone()
                };
            }
        }
        ControlFlow::<()>::Continue(())
    });
    let markers = [start.to_string(), end.to_string()];
    match &mut select.group_by {
        GroupByExpr::Expressions(exprs, mods) => {
            if !mods.is_empty() {
                return Err(ExqlError::unsupported(
                    "GROUP BY modifiers in windowed queries",
                    "",
                ));
            }
            exprs.retain(|e| !markers.contains(&e.to_string()));
            exprs.insert(0, end.clone());
            exprs.insert(0, start.clone());
        }
        GroupByExpr::All(_) => {
            return Err(ExqlError::unsupported(
                "GROUP BY ALL",
                "list the grouping columns",
            ));
        }
    }
    Ok(())
}

/// Plan a statement and run the analyzer (type coercion etc.).
pub async fn plan(state: &SessionState, stmt: DFStatement) -> Result<LogicalPlan, ExqlError> {
    let plan = state.statement_to_plan(stmt).await?;
    Ok(plan)
}

/// Analyzed (but not optimized) logical plan.
pub fn analyze(state: &SessionState, plan: LogicalPlan) -> Result<LogicalPlan, ExqlError> {
    Ok(state
        .analyzer()
        .execute_and_check(plan, state.config_options(), |_, _| {})?)
}
