//! Numeric semantics for JSON text.
//!
//! `payload->>'amount'` is text. Left alone, `payload->>'amount' > 250`
//! would compare strings and `SUM(payload->>'amount')` would fail. This
//! analyzer rule runs before type coercion and casts JSON text to a number
//! (`TRY_CAST(… AS DOUBLE)`, so non-numeric values become NULL) where the
//! context is numeric:
//!
//! - comparisons, `BETWEEN` and `IN` against a numeric operand;
//! - `<`, `<=`, `>`, `>=` between two JSON texts: numerically when both are
//!   numbers, as text otherwise (`=` / `<>` between two JSON texts compare
//!   the text, so they stay usable as join keys);
//! - arithmetic (`+ - * / %`);
//! - numeric aggregates (`SUM`, `AVG`, `MIN`, `MAX`, `STDDEV`, …, also as
//!   window functions) and math functions (`ABS`, `ROUND`, …);
//! - `ORDER BY`: values sort numerically first, then as text, so numbers
//!   order correctly and non-numbers still sort deterministically.
//!
//! Comparing JSON text with a boolean casts to boolean. An explicit
//! `CAST(payload->>'x' AS VARCHAR)` opts out (the cast hides the JSON
//! origin).

use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{DFSchema, Result as DFResult};
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::expr::{
    AggregateFunction, Between, InList, ScalarFunction, Sort as SortExpr,
    WindowFunctionDefinition,
};
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::utils::merge_schema;
use datafusion::logical_expr::{
    BinaryExpr, Expr, ExprSchemable, LogicalPlan, Operator, Sort, TryCast,
};
use datafusion::optimizer::AnalyzerRule;

const NUMERIC_AGGS: &[&str] = &[
    "sum", "avg", "mean", "min", "max", "stddev", "stddev_pop", "stddev_samp", "var",
    "var_pop", "var_samp", "variance", "median", "approx_median", "corr", "covar",
    "covar_pop", "covar_samp",
];

const MATH_FUNCS: &[&str] = &[
    "abs", "round", "ceil", "floor", "sqrt", "cbrt", "power", "pow", "ln", "log", "log2",
    "log10", "exp", "signum", "trunc",
];

#[derive(Debug, Default)]
pub struct JsonNumericRule;

fn func_name(e: &Expr) -> Option<&str> {
    match e {
        Expr::ScalarFunction(f) => Some(f.func.name()),
        _ => None,
    }
}

/// Whether `e` is (directly) a JSON text extraction.
fn is_json_call(e: &Expr) -> bool {
    match e {
        Expr::Alias(a) => is_json_call(&a.expr),
        _ => matches!(func_name(e), Some("json_as_text" | "json_get")),
    }
}

/// Whether `e` is JSON text, following column references through
/// projections, aliases and pass-through nodes of `inputs`.
fn is_json_text(e: &Expr, inputs: &[&LogicalPlan]) -> bool {
    match e {
        Expr::Alias(a) => is_json_text(&a.expr, inputs),
        Expr::Column(c) => inputs.iter().any(|p| column_is_json(c, p, 0)),
        _ => is_json_call(e),
    }
}

fn column_is_json(c: &datafusion::common::Column, plan: &LogicalPlan, depth: usize) -> bool {
    if depth > 16 {
        return false;
    }
    let Some(idx) = plan.schema().maybe_index_of_column(c) else {
        return false;
    };
    let field_col = |p: &LogicalPlan, i: usize| -> Option<datafusion::common::Column> {
        let (q, f) = p.schema().qualified_field(i);
        Some(datafusion::common::Column::new(q.cloned(), f.name()))
    };
    match plan {
        LogicalPlan::Projection(p) => p.expr.get(idx).is_some_and(|e| match e {
            Expr::Column(inner) => column_is_json(inner, &p.input, depth + 1),
            Expr::Alias(a) => match a.expr.as_ref() {
                Expr::Column(inner) => column_is_json(inner, &p.input, depth + 1),
                other => is_json_call(other),
            },
            other => is_json_call(other),
        }),
        LogicalPlan::SubqueryAlias(s) => {
            field_col(&s.input, idx).is_some_and(|ic| column_is_json(&ic, &s.input, depth + 1))
        }
        LogicalPlan::Filter(_)
        | LogicalPlan::Sort(_)
        | LogicalPlan::Limit(_)
        | LogicalPlan::Distinct(_) => plan
            .inputs()
            .first()
            .is_some_and(|i| column_is_json(c, i, depth + 1)),
        LogicalPlan::Aggregate(a) => {
            idx < a.group_expr.len() && is_json_text(&a.group_expr[idx], &[a.input.as_ref()])
        }
        _ => false,
    }
}

fn to_double(e: Expr) -> Expr {
    Expr::TryCast(TryCast::new(Box::new(e), DataType::Float64))
}

/// Numeric ORDER BY key for JSON text.
fn sort_key(e: Expr) -> Expr {
    let udf = Arc::new(crate::udfs::json_num());
    let arg = Expr::Cast(datafusion::logical_expr::Cast::new(Box::new(e), DataType::Utf8));
    Expr::ScalarFunction(ScalarFunction::new_udf(udf, vec![arg]))
}

fn numeric(t: &DataType) -> bool {
    t.is_numeric()
}

fn is_cmp(op: &Operator) -> bool {
    matches!(
        op,
        Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq
            | Operator::IsDistinctFrom
            | Operator::IsNotDistinctFrom
    )
}

fn is_arith(op: &Operator) -> bool {
    matches!(
        op,
        Operator::Plus | Operator::Minus | Operator::Multiply | Operator::Divide | Operator::Modulo
    )
}

fn rewrite_expr(e: Expr, schema: &DFSchema, inputs: &[&LogicalPlan]) -> DFResult<Transformed<Expr>> {
    let ty = |x: &Expr| x.get_type(schema).ok();
    match e {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) if is_cmp(&op) || is_arith(&op) => {
            let lj = is_json_text(&left, inputs);
            let rj = is_json_text(&right, inputs);
            if !lj && !rj {
                return Ok(Transformed::no(Expr::BinaryExpr(BinaryExpr { left, op, right })));
            }
            let lt = ty(&left);
            let rt = ty(&right);
            if lj && rj && matches!(op, Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq) {
                // Both sides JSON text: numbers compare as numbers, anything
                // else as text.
                let (l, r) = (*left, *right);
                let (ln, rn) = (sort_key(l.clone()), sort_key(r.clone()));
                let cond = ln.clone().is_not_null().and(rn.clone().is_not_null());
                let then = Expr::BinaryExpr(BinaryExpr::new(Box::new(ln), op, Box::new(rn)));
                let otherwise = Expr::BinaryExpr(BinaryExpr::new(Box::new(l), op, Box::new(r)));
                return Ok(Transformed::yes(Expr::Case(datafusion::logical_expr::expr::Case::new(
                    None,
                    vec![(Box::new(cond), Box::new(then))],
                    Some(Box::new(otherwise)),
                ))));
            }
            let (l, r) = if is_arith(&op) {
                (
                    if lj { to_double(*left) } else { *left },
                    if rj { to_double(*right) } else { *right },
                )
            } else if lj && !rj && rt.as_ref().is_some_and(numeric) {
                (to_double(*left), *right)
            } else if rj && !lj && lt.as_ref().is_some_and(numeric) {
                (*left, to_double(*right))
            } else if lj && !rj && rt == Some(DataType::Boolean) {
                (Expr::TryCast(TryCast::new(left, DataType::Boolean)), *right)
            } else if rj && !lj && lt == Some(DataType::Boolean) {
                (*left, Expr::TryCast(TryCast::new(right, DataType::Boolean)))
            } else {
                return Ok(Transformed::no(Expr::BinaryExpr(BinaryExpr { left, op, right })));
            };
            Ok(Transformed::yes(Expr::BinaryExpr(BinaryExpr::new(
                Box::new(l),
                op,
                Box::new(r),
            ))))
        }
        Expr::Between(b)
            if is_json_text(&b.expr, inputs)
                && ty(&b.low).as_ref().is_some_and(numeric)
                && ty(&b.high).as_ref().is_some_and(numeric) =>
        {
            Ok(Transformed::yes(Expr::Between(Between::new(
                Box::new(to_double(*b.expr)),
                b.negated,
                b.low,
                b.high,
            ))))
        }
        Expr::InList(il)
            if is_json_text(&il.expr, inputs)
                && !il.list.is_empty()
                && il.list.iter().all(|x| ty(x).as_ref().is_some_and(numeric)) =>
        {
            Ok(Transformed::yes(Expr::InList(InList::new(
                Box::new(to_double(*il.expr)),
                il.list,
                il.negated,
            ))))
        }
        Expr::AggregateFunction(mut af)
            if NUMERIC_AGGS.contains(&af.func.name())
                && af.params.args.iter().any(|a| is_json_text(a, inputs)) =>
        {
            af.params.args = std::mem::take(&mut af.params.args)
                .into_iter()
                .map(|a| if is_json_text(&a, inputs) { to_double(a) } else { a })
                .collect();
            Ok(Transformed::yes(Expr::AggregateFunction(AggregateFunction {
                func: af.func,
                params: af.params,
            })))
        }
        Expr::WindowFunction(mut wf) => {
            let numeric_agg = match &wf.fun {
                WindowFunctionDefinition::AggregateUDF(u) => NUMERIC_AGGS.contains(&u.name()),
                _ => false,
            };
            if numeric_agg && wf.params.args.iter().any(|a| is_json_text(a, inputs)) {
                wf.params.args = std::mem::take(&mut wf.params.args)
                    .into_iter()
                    .map(|a| if is_json_text(&a, inputs) { to_double(a) } else { a })
                    .collect();
                Ok(Transformed::yes(Expr::WindowFunction(wf)))
            } else {
                Ok(Transformed::no(Expr::WindowFunction(wf)))
            }
        }
        Expr::ScalarFunction(sf)
            if MATH_FUNCS.contains(&sf.func.name())
                && sf.args.first().is_some_and(|a| is_json_text(a, inputs)) =>
        {
            let mut args = sf.args;
            let first = args.remove(0);
            args.insert(0, to_double(first));
            Ok(Transformed::yes(Expr::ScalarFunction(ScalarFunction::new_udf(
                sf.func, args,
            ))))
        }
        other => Ok(Transformed::no(other)),
    }
}

impl AnalyzerRule for JsonNumericRule {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> DFResult<LogicalPlan> {
        let out = plan.transform_up_with_subqueries(|plan| {
            if let LogicalPlan::Sort(sort) = &plan {
                let input = sort.input.clone();
                let mut changed = false;
                let mut exprs = Vec::with_capacity(sort.expr.len());
                for s in &sort.expr {
                    if is_json_text(&s.expr, &[input.as_ref()]) {
                        changed = true;
                        exprs.push(SortExpr::new(sort_key(s.expr.clone()), s.asc, s.nulls_first));
                    }
                    exprs.push(s.clone());
                }
                if changed {
                    return Ok(Transformed::yes(LogicalPlan::Sort(Sort {
                        expr: exprs,
                        input,
                        fetch: sort.fetch,
                    })));
                }
                return Ok(Transformed::no(plan));
            }
            if matches!(plan, LogicalPlan::TableScan(_)) || plan.inputs().is_empty() {
                return Ok(Transformed::no(plan));
            }
            let inputs: Vec<Arc<LogicalPlan>> =
                plan.inputs().into_iter().map(|p| Arc::new(p.clone())).collect();
            let input_refs: Vec<&LogicalPlan> = inputs.iter().map(|p| p.as_ref()).collect();
            let schema = merge_schema(&input_refs);
            let names = NamePreserver::new(&plan);
            let t = plan.map_expressions(|e| {
                let saved = names.save(&e);
                let t = e.transform_up(|e| rewrite_expr(e, &schema, &input_refs))?;
                if t.transformed {
                    Ok(Transformed::yes(saved.restore(t.data)))
                } else {
                    Ok(t)
                }
            })?;
            if t.transformed {
                // Expression types may have changed; recompute the schema.
                let p = t.data.recompute_schema()?;
                Ok(Transformed::yes(p))
            } else {
                Ok(t)
            }
        })?;
        Ok(out.data)
    }

    fn name(&self) -> &str {
        "exspeed_json_numeric"
    }
}
