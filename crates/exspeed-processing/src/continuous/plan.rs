//! Compile a `CREATE STREAM/TABLE … AS SELECT` into a dataflow.
//!
//! The SELECT is planned and analyzed by DataFusion; the resulting logical
//! plan must have the shape
//!
//! ```text
//! [Projection | Filter]*                 -- "top": after the aggregate
//! [Aggregate                             -- optional, at most one
//!   [Projection | Filter]*]              -- "mid": before the aggregate
//! TableScan(stream)                      -- one source, or
//! Join(side, side)                       -- stream-stream (WITHIN) or stream-table
//!   side := [Projection | Filter]* TableScan
//! ```
//!
//! Everything else (sorts, limits, window functions, set operations,
//! subqueries, multi-way joins) is rejected with an `UNSUPPORTED` error.

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::catalog::default_table_source::source_as_provider;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{DFSchema, JoinType};
use datafusion::execution::session_state::SessionState;
use datafusion::logical_expr::{
    Aggregate, Expr, ExprSchemable, Join, LogicalPlan, SubqueryAlias, TableScan,
};
use datafusion::optimizer::extract_equijoin_predicate::ExtractEquijoinPredicate;
use datafusion::optimizer::{OptimizerContext, OptimizerRule};
use datafusion::physical_expr::PhysicalExpr;
#[allow(deprecated)]
use datafusion::physical_planner::create_aggregate_expr_and_maybe_filter;
use datafusion::sql::parser::Statement as DFStatement;
use datafusion::sql::sqlparser::ast::Statement as SQLStatement;
use exspeed_common::StreamName;

use super::ops::{AggDef, AggOp, SideDef, StreamJoin, TableJoin};
use super::rows::{Chain, Step};
use crate::ast;
use crate::error::ExqlError;
use crate::sql::{CreateKind, Emit, QuerySpec, WindowSpec};
use crate::stream_table::StreamTable;
use crate::tables::MaterializedTable;
use crate::udfs::{WEND, WSTART};

/// One stream read by the query.
pub struct SourceDef {
    pub stream: StreamName,
    /// Relation name in the query (alias or stream name).
    pub name: String,
    /// Scan projection, if the plan has one.
    pub scan_cols: Option<(Vec<usize>, SchemaRef)>,
    /// `TIMESTAMP BY` expression (over the scan's columns).
    pub ts: Option<Arc<dyn PhysicalExpr>>,
}

/// The input of the dataflow.
pub enum InputOp {
    Single { src: usize },
    StreamJoin(Box<StreamJoin>),
    TableJoin(Box<TableJoin>),
}

/// A compiled continuous query.
pub struct Dataflow {
    pub sources: Vec<SourceDef>,
    pub input: InputOp,
    /// Steps between the input and the aggregate (or the output, if none).
    pub mid: Chain,
    pub agg: Option<AggOp>,
    /// Steps after the aggregate.
    pub top: Chain,
    pub out_schema: SchemaRef,
    pub emit: Emit,
    pub window: Option<WindowSpec>,
    pub grace_ms: i64,
    /// How far behind the watermark rows can arrive at the aggregate (the
    /// WITHIN of a stream-stream join in front of it).
    pub delay_ms: i64,
    /// Materialized tables the query reads.
    pub tables_read: Vec<String>,
}

impl Dataflow {
    pub fn source_streams(&self) -> Vec<StreamName> {
        self.sources.iter().map(|s| s.stream.clone()).collect()
    }
}

fn arrow(schema: &DFSchema) -> SchemaRef {
    Arc::new(schema.as_arrow().clone())
}

fn unsupported_node(plan: &LogicalPlan) -> ExqlError {
    let (what, hint) = match plan {
        LogicalPlan::Sort(_) => (
            "ORDER BY in a continuous query",
            "order results when reading the output",
        ),
        LogicalPlan::Limit(_) => ("LIMIT / OFFSET in a continuous query", ""),
        LogicalPlan::Window(_) => (
            "window functions (OVER) in a continuous query",
            "use WINDOW TUMBLING / HOPPING with GROUP BY",
        ),
        LogicalPlan::Distinct(_) => ("SELECT DISTINCT in a continuous query", "use GROUP BY"),
        LogicalPlan::Union(_) => ("UNION in a continuous query", "create one query per input"),
        LogicalPlan::Aggregate(_) => (
            "more than one level of aggregation in a continuous query",
            "aggregate the output stream of another query",
        ),
        LogicalPlan::Join(_) => (
            "joining more than two relations in a continuous query",
            "join two streams per query and chain queries",
        ),
        LogicalPlan::Subquery(_) | LogicalPlan::SubqueryAlias(_) => {
            ("subqueries in a continuous query", "")
        }
        LogicalPlan::EmptyRelation(_) | LogicalPlan::Values(_) => {
            ("a continuous query without a FROM stream", "")
        }
        LogicalPlan::Unnest(_) => ("UNNEST in a continuous query", ""),
        _ => ("this query shape in a continuous query", ""),
    };
    ExqlError::unsupported(what, hint)
}

fn map_expr_err(e: datafusion::error::DataFusionError) -> ExqlError {
    let msg = e.to_string();
    if msg.contains("Subquery") || msg.contains("subquery") || msg.contains("Exists") {
        ExqlError::unsupported("subqueries in a continuous query", "")
    } else {
        ExqlError::from(e)
    }
}

/// `[Projection | Filter | SubqueryAlias]*` from the top, and the node below.
pub(crate) fn split_chain(plan: &LogicalPlan) -> (Vec<&LogicalPlan>, &LogicalPlan) {
    let mut nodes = vec![];
    let mut cur = plan;
    loop {
        match cur {
            LogicalPlan::Projection(p) => {
                nodes.push(cur);
                cur = p.input.as_ref();
            }
            LogicalPlan::Filter(f) => {
                nodes.push(cur);
                cur = f.input.as_ref();
            }
            LogicalPlan::SubqueryAlias(s) => {
                nodes.push(cur);
                cur = s.input.as_ref();
            }
            _ => break,
        }
    }
    (nodes, cur)
}

/// The SubqueryAlias directly above the scan, if the bottom of a chain is one.
fn bottom_alias<'a>(nodes: &[&'a LogicalPlan]) -> Option<&'a SubqueryAlias> {
    match nodes.last() {
        Some(LogicalPlan::SubqueryAlias(s)) => Some(s),
        _ => None,
    }
}

fn is_marker(e: &Expr, name: &str) -> bool {
    match e {
        Expr::ScalarFunction(f) => f.func.name() == name,
        Expr::Alias(a) => is_marker(&a.expr, name),
        _ => false,
    }
}

/// Simplify, then compile one expression. The logical optimizer never runs
/// on a continuous plan, and some functions (`coalesce`, `nvl`) only work
/// after simplification rewrites them; without it they fail at runtime.
/// No execution start time is set, so `now()` is not folded to a constant.
fn physical_expr(
    state: &SessionState,
    e: &Expr,
    schema: &DFSchema,
) -> Result<Arc<dyn PhysicalExpr>, ExqlError> {
    use datafusion::optimizer::simplify_expressions::{ExprSimplifier, SimplifyContext};
    let ctx = SimplifyContext::builder()
        .with_schema(Arc::new(schema.clone()))
        .with_config_options(Arc::new(state.config_options().as_ref().clone()))
        .build();
    let e = ExprSimplifier::new(ctx)
        .simplify(e.clone())
        .map_err(map_expr_err)?;
    state
        .create_physical_expr(e, schema)
        .map_err(map_expr_err)
}

/// Compile `[Projection | Filter | SubqueryAlias]*` nodes (top-down order)
/// into stateless steps (applied bottom-up).
pub(crate) fn compile_chain(
    state: &SessionState,
    nodes: &[&LogicalPlan],
) -> Result<Chain, ExqlError> {
    let phys = |e: &Expr, schema: &DFSchema| physical_expr(state, e, schema);
    let mut steps = vec![];
    for node in nodes.iter().rev() {
        match node {
            LogicalPlan::Projection(p) => {
                let exprs = p
                    .expr
                    .iter()
                    .map(|e| phys(e, p.input.schema()))
                    .collect::<Result<_, _>>()?;
                steps.push(Step::Project(exprs, arrow(&p.schema)));
            }
            LogicalPlan::Filter(f) => {
                steps.push(Step::Filter(phys(&f.predicate, f.input.schema())?));
            }
            LogicalPlan::SubqueryAlias(_) => {}
            other => return Err(unsupported_node(other)),
        }
    }
    Ok(Chain { steps })
}

struct Builder<'a> {
    state: &'a SessionState,
    spec: &'a QuerySpec,
    tables: &'a crate::tables::TableRegistry,
    sources: Vec<SourceDef>,
    ts_used: Vec<bool>,
    tables_read: Vec<String>,
}

enum ScanKind {
    Stream(usize),
    Table(Arc<MaterializedTable>, Option<(Vec<usize>, SchemaRef)>),
}

impl Builder<'_> {
    fn phys(&self, e: &Expr, schema: &DFSchema) -> Result<Arc<dyn PhysicalExpr>, ExqlError> {
        physical_expr(self.state, e, schema)
    }

    fn chain(&self, nodes: &[&LogicalPlan]) -> Result<Chain, ExqlError> {
        compile_chain(self.state, nodes)
    }

    fn scan(
        &mut self,
        scan: &TableScan,
        alias: Option<&SubqueryAlias>,
    ) -> Result<ScanKind, ExqlError> {
        if !scan.filters.is_empty() || scan.fetch.is_some() {
            return Err(ExqlError::Internal("unexpected pushed-down scan".into()));
        }
        let provider = source_as_provider(&scan.source)?;
        let name = alias
            .map(|a| a.alias.table().to_string())
            .unwrap_or_else(|| scan.table_name.table().to_string());
        let cols = scan
            .projection
            .clone()
            .map(|p| (p, arrow(&scan.projected_schema)));
        if let Some(t) = provider.downcast_ref::<MaterializedTable>() {
            let table = self
                .tables
                .get(&t.name)
                .ok_or_else(|| ExqlError::NotFound(format!("table '{}' not found", t.name)))?;
            if self
                .spec
                .timestamp_by
                .iter()
                .any(|t| t.relation.as_deref() == Some(name.as_str()))
            {
                return Err(ExqlError::Plan(format!(
                    "TIMESTAMP BY on table '{name}': only streams have event time"
                )));
            }
            self.tables_read.push(table.name.clone());
            return Ok(ScanKind::Table(table, cols));
        }
        let Some(st) = provider.downcast_ref::<StreamTable>() else {
            return Err(ExqlError::unsupported(
                format!("reading '{name}' in a continuous query"),
                "continuous queries read streams and materialized tables; external databases are bounded-only",
            ));
        };
        let ts_idx = self
            .spec
            .timestamp_by
            .iter()
            .position(|t| t.relation.as_deref() == Some(name.as_str()) || t.relation.is_none());
        let ts = match ts_idx {
            Some(i) => {
                self.ts_used[i] = true;
                let schema = alias
                    .map(|a| a.schema.clone())
                    .unwrap_or_else(|| scan.projected_schema.clone());
                let expr = self
                    .state
                    .create_logical_expr(&self.spec.timestamp_by[i].expr_sql, &schema)?;
                Some(self.phys(&expr, &schema)?)
            }
            None => None,
        };
        self.sources.push(SourceDef {
            stream: st.stream().clone(),
            name,
            scan_cols: cols,
            ts,
        });
        Ok(ScanKind::Stream(self.sources.len() - 1))
    }

    fn aggregate(&self, a: &Aggregate) -> Result<AggOp, ExqlError> {
        let mut groups: Vec<&Expr> = a.group_expr.iter().collect();
        if groups.iter().any(|g| matches!(g, Expr::GroupingSet(_))) {
            return Err(ExqlError::unsupported(
                "GROUPING SETS / ROLLUP / CUBE in a continuous query",
                "",
            ));
        }
        if self.spec.window.is_some() {
            if groups.len() < 2 || !is_marker(groups[0], WSTART) || !is_marker(groups[1], WEND) {
                return Err(ExqlError::Internal(
                    "window markers missing from GROUP BY".into(),
                ));
            }
            groups.drain(0..2);
        }
        let input_schema = a.input.schema();
        let group_exprs = groups
            .iter()
            .map(|e| self.phys(e, input_schema))
            .collect::<Result<_, _>>()?;
        let mut aggs = vec![];
        for e in &a.aggr_expr {
            #[allow(deprecated)]
            let (agg, filter, order) = create_aggregate_expr_and_maybe_filter(
                e,
                input_schema,
                input_schema.as_arrow(),
                self.state.execution_props(),
            )
            .map_err(map_expr_err)?;
            if !order.is_empty() {
                return Err(ExqlError::unsupported(
                    "ORDER BY inside an aggregate in a continuous query",
                    "",
                ));
            }
            let state_fields = agg.state_fields()?;
            aggs.push(AggDef {
                args: agg.expressions(),
                expr: agg,
                filter,
                state_fields,
            });
        }
        AggOp::new(group_exprs, self.spec.window, aggs, arrow(&a.schema))
    }

    fn input(
        &mut self,
        node: &LogicalPlan,
        chain_nodes: &[&LogicalPlan],
    ) -> Result<InputOp, ExqlError> {
        match node {
            LogicalPlan::TableScan(scan) => match self.scan(scan, bottom_alias(chain_nodes))? {
                ScanKind::Stream(src) => Ok(InputOp::Single { src }),
                ScanKind::Table(t, _) => Err(ExqlError::unsupported(
                    format!("a continuous query over table '{}' alone", t.name),
                    "tables can be joined with a stream; query the table with a bounded SELECT",
                )),
            },
            LogicalPlan::Join(j) => self.join(j),
            other => Err(unsupported_node(other)),
        }
    }

    fn join(&mut self, j: &Join) -> Result<InputOp, ExqlError> {
        let left_join = match j.join_type {
            JoinType::Inner => false,
            JoinType::Left => true,
            JoinType::Right => {
                return Err(ExqlError::unsupported(
                    "RIGHT JOIN in a continuous query",
                    "swap the sides and use LEFT JOIN",
                ))
            }
            other => {
                return Err(ExqlError::unsupported(
                    format!("{other} JOIN in a continuous query"),
                    "continuous queries support INNER and LEFT joins",
                ))
            }
        };
        let (lnodes, lrest) = split_chain(&j.left);
        let (rnodes, rrest) = split_chain(&j.right);
        let (LogicalPlan::TableScan(ls), LogicalPlan::TableScan(rs)) = (lrest, rrest) else {
            let bad = if matches!(lrest, LogicalPlan::TableScan(_)) {
                rrest
            } else {
                lrest
            };
            return Err(unsupported_node(bad));
        };
        let lk = self.scan(ls, bottom_alias(&lnodes))?;
        let rk = self.scan(rs, bottom_alias(&rnodes))?;
        let lchain = self.chain(&lnodes)?;
        let rchain = self.chain(&rnodes)?;
        let lschema = j.left.schema();
        let rschema = j.right.schema();
        let lkeys: Vec<Arc<dyn PhysicalExpr>> =
            j.on.iter()
                .map(|(l, _)| self.phys(l, lschema))
                .collect::<Result<_, _>>()?;
        let rkeys: Vec<Arc<dyn PhysicalExpr>> =
            j.on.iter()
                .map(|(_, r)| self.phys(r, rschema))
                .collect::<Result<_, _>>()?;
        let key_types: Vec<DataType> =
            j.on.iter()
                .map(|(l, _)| l.get_type(lschema))
                .collect::<Result<_, _>>()?;
        let filter = j
            .filter
            .as_ref()
            .map(|f| self.phys(f, &j.schema))
            .transpose()?;
        let out_schema = arrow(&j.schema);
        let within = self.spec.within.first().copied().flatten();
        if j.on.is_empty() {
            return Err(ExqlError::Plan(
                "a continuous JOIN needs at least one equality between the two sides in ON (e.g. ON a.key = b.key)".into(),
            ));
        }
        match (lk, rk) {
            (ScanKind::Stream(l), ScanKind::Stream(r)) => {
                let within = within.ok_or_else(|| {
                    ExqlError::Plan(
                        "a stream-stream JOIN needs WITHIN <interval> (e.g. JOIN b WITHIN INTERVAL '5 minutes' ON …)".into(),
                    )
                })?;
                let left = SideDef {
                    src: l,
                    chain: lchain,
                    keys: lkeys,
                    schema: arrow(lschema),
                };
                let right = SideDef {
                    src: r,
                    chain: rchain,
                    keys: rkeys,
                    schema: arrow(rschema),
                };
                Ok(InputOp::StreamJoin(Box::new(StreamJoin::new(
                    left, right, left_join, within, filter, out_schema, key_types,
                )?)))
            }
            (ScanKind::Stream(s), ScanKind::Table(t, cols)) => {
                if within.is_some() {
                    return Err(ExqlError::Plan(
                        "WITHIN only applies to stream-stream joins".into(),
                    ));
                }
                let mut tchain = rchain;
                if let Some((idx, sch)) = cols {
                    tchain.steps.insert(0, Step::Columns(idx, sch));
                }
                let stream = SideDef {
                    src: s,
                    chain: lchain,
                    keys: lkeys,
                    schema: arrow(lschema),
                };
                Ok(InputOp::TableJoin(Box::new(TableJoin::new(
                    stream,
                    t,
                    tchain,
                    rkeys,
                    arrow(rschema),
                    true,
                    left_join,
                    filter,
                    out_schema,
                    key_types,
                )?)))
            }
            (ScanKind::Table(t, cols), ScanKind::Stream(s)) => {
                if left_join {
                    return Err(ExqlError::unsupported(
                        "LEFT JOIN with the table on the left in a continuous query",
                        "put the stream on the left: stream LEFT JOIN table",
                    ));
                }
                if within.is_some() {
                    return Err(ExqlError::Plan(
                        "WITHIN only applies to stream-stream joins".into(),
                    ));
                }
                let mut tchain = lchain;
                if let Some((idx, sch)) = cols {
                    tchain.steps.insert(0, Step::Columns(idx, sch));
                }
                let stream = SideDef {
                    src: s,
                    chain: rchain,
                    keys: rkeys,
                    schema: arrow(rschema),
                };
                Ok(InputOp::TableJoin(Box::new(TableJoin::new(
                    stream,
                    t,
                    tchain,
                    lkeys,
                    arrow(lschema),
                    false,
                    false,
                    filter,
                    out_schema,
                    key_types,
                )?)))
            }
            (ScanKind::Table(..), ScanKind::Table(..)) => Err(ExqlError::unsupported(
                "a continuous join of two tables",
                "join a stream with a table, or query the tables with a bounded SELECT",
            )),
        }
    }
}

/// Plan the query of a `CREATE STREAM/TABLE`.
pub async fn compile(
    state: &SessionState,
    spec: &QuerySpec,
    kind: CreateKind,
    tables: &crate::tables::TableRegistry,
    default_grace_ms: i64,
) -> Result<Dataflow, ExqlError> {
    let mut stmt = ast::parse_one(&spec.select_sql)?;
    match &stmt {
        DFStatement::Statement(s) if matches!(s.as_ref(), SQLStatement::Query(_)) => {}
        _ => return Err(ExqlError::parse("expected a SELECT after AS")),
    }
    ast::rewrite_json_numeric_args(&mut stmt)?;
    ast::alias_qualified_json(&mut stmt);
    if spec.window.is_some() {
        ast::rewrite_window_markers(&mut stmt)?;
    }
    let plan = state.statement_to_plan(stmt).await?;
    let plan = ast::analyze(state, plan)?;
    let rule = ExtractEquijoinPredicate::new();
    let ctx = OptimizerContext::new();
    let plan = plan
        .transform_up(|p| {
            if matches!(p, LogicalPlan::Join(_)) {
                rule.rewrite(p, &ctx)
            } else {
                Ok(Transformed::no(p))
            }
        })
        .data()?;

    let mut b = Builder {
        state,
        spec,
        tables,
        sources: vec![],
        ts_used: vec![false; spec.timestamp_by.len()],
        tables_read: vec![],
    };
    let (top_nodes, rest) = split_chain(&plan);
    let (agg, mid, input, top) = match rest {
        LogicalPlan::Aggregate(a) => {
            let top = b.chain(&top_nodes)?;
            let agg = b.aggregate(a)?;
            let (mid_nodes, below) = split_chain(&a.input);
            let input = b.input(below, &mid_nodes)?;
            let mid = b.chain(&mid_nodes)?;
            (Some(agg), mid, input, top)
        }
        other => {
            let input = b.input(other, &top_nodes)?;
            let top = b.chain(&top_nodes)?;
            (None, Chain::default(), input, top)
        }
    };
    if let Some(i) = b.ts_used.iter().position(|u| !u) {
        return Err(ExqlError::Plan(format!(
            "TIMESTAMP BY refers to '{}', which is not a stream in this query",
            spec.timestamp_by[i].relation.clone().unwrap_or_default()
        )));
    }
    // window_start / window_end only exist after the aggregate.
    let mut below: Vec<String> = mid.expr_strings();
    match &input {
        InputOp::StreamJoin(j) => {
            below.extend(j.left.chain.expr_strings());
            below.extend(j.right.chain.expr_strings());
        }
        InputOp::TableJoin(j) => below.extend(j.stream.chain.expr_strings()),
        InputOp::Single { .. } => {}
    }
    if below.iter().any(|s| s.contains(WSTART) || s.contains(WEND)) {
        return Err(ExqlError::Plan(
            "window_start / window_end can only be used in the SELECT list and HAVING of a windowed query".into(),
        ));
    }
    if spec.window.is_some() && agg.is_none() {
        return Err(ExqlError::Plan(
            "WINDOW needs an aggregation (GROUP BY or aggregate functions)".into(),
        ));
    }
    if kind == CreateKind::Table && agg.is_none() {
        return Err(ExqlError::Plan(
            "CREATE TABLE needs an aggregation (GROUP BY or aggregate functions); use CREATE STREAM for row-by-row output".into(),
        ));
    }
    let emit = spec.emit.unwrap_or(Emit::Changes);
    if emit == Emit::Final && spec.window.is_none() {
        return Err(ExqlError::Plan(
            "EMIT FINAL needs a WINDOW (a non-windowed aggregate never closes)".into(),
        ));
    }
    let delay_ms = match &input {
        InputOp::StreamJoin(j) => j.within_ms,
        _ => 0,
    };
    let out_schema = arrow(plan.schema());
    let mut seen = std::collections::HashSet::new();
    for f in out_schema.fields() {
        if !seen.insert(f.name().clone()) {
            return Err(ExqlError::Plan(format!(
                "output column '{}' appears twice; give the columns distinct aliases",
                f.name()
            )));
        }
    }
    Ok(Dataflow {
        sources: b.sources,
        input,
        mid,
        agg,
        top,
        out_schema,
        emit,
        window: spec.window,
        grace_ms: spec.grace_ms.unwrap_or(default_grace_ms),
        delay_ms,
        tables_read: b.tables_read,
    })
}
