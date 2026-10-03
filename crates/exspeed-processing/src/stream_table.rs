//! Streams as DataFusion tables.
//!
//! [`StreamTable`] exposes a stream with the columns of
//! [`crate::convert::stream_schema`]. Scans push down offset ranges,
//! timestamp bounds (via `seek_by_time`), `LIMIT`, and — through
//! [`ReverseTailRule`] and `try_pushdown_sort` — `ORDER BY offset DESC LIMIT n`,
//! which reads the tail of the stream backwards.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{DFSchema, Result as DFResult, ScalarValue};
use datafusion::config::ConfigOptions;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{
    BinaryExpr, Expr, Operator, TableProviderFilterPushDown, TableType,
};
use datafusion::physical_expr::expressions::Column as PhysColumn;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr, PhysicalSortExpr};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::limit::GlobalLimitExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream, SortOrderPushdownResult,
};
use exspeed_common::{Offset, StreamName};
use exspeed_streams::{ReadLimits, StorageEngine, StoredRecord};

use crate::convert::{records_to_batch_projected, stream_schema, COL_OFFSET, COL_TIMESTAMP};
use crate::error::ExqlError;

/// Records per storage read / output batch.
const SCAN_BATCH: usize = 1024;
const SCAN_BYTES: usize = 8 * 1024 * 1024;

/// A stream exposed as a table.
#[derive(Clone)]
pub struct StreamTable {
    storage: Arc<dyn StorageEngine>,
    stream: StreamName,
}

impl fmt::Debug for StreamTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamTable")
            .field("stream", &self.stream.as_str())
            .finish()
    }
}

impl StreamTable {
    pub fn new(storage: Arc<dyn StorageEngine>, stream: StreamName) -> Self {
        Self { storage, stream }
    }

    pub fn stream(&self) -> &StreamName {
        &self.stream
    }
}

/// Which bound a pushed-down predicate gives.
#[derive(Debug, Default, Clone, Copy)]
struct Bounds {
    /// Inclusive lower offset.
    lo: Option<u64>,
    /// Exclusive upper offset.
    hi: Option<u64>,
    /// Inclusive lower timestamp, ns.
    ts_lo: Option<u64>,
    /// Exclusive upper timestamp, ns.
    ts_hi: Option<u64>,
}

impl Bounds {
    fn lo(&mut self, v: u64) {
        self.lo = Some(self.lo.map_or(v, |x| x.max(v)));
    }
    fn hi(&mut self, v: u64) {
        self.hi = Some(self.hi.map_or(v, |x| x.min(v)));
    }
    fn ts_lo(&mut self, v: u64) {
        self.ts_lo = Some(self.ts_lo.map_or(v, |x| x.max(v)));
    }
    fn ts_hi(&mut self, v: u64) {
        self.ts_hi = Some(self.ts_hi.map_or(v, |x| x.min(v)));
    }
}

/// Strip casts from the column side of a comparison.
fn column_name(e: &Expr) -> Option<&str> {
    match e {
        Expr::Column(c) => Some(c.name.as_str()),
        Expr::Cast(c) => column_name(&c.expr),
        Expr::TryCast(c) => column_name(&c.expr),
        _ => None,
    }
}

/// Evaluate an expression with no column references to a scalar.
pub(crate) fn const_eval(expr: &Expr, session: &dyn Session) -> Option<ScalarValue> {
    if !expr.column_refs().is_empty() {
        return None;
    }
    let phys = session
        .create_physical_expr(expr.clone(), &DFSchema::empty())
        .ok()?;
    let batch = RecordBatch::try_new_with_options(
        Arc::new(Schema::empty()),
        vec![],
        &datafusion::arrow::array::RecordBatchOptions::new().with_row_count(Some(1)),
    )
    .ok()?;
    let arr = phys.evaluate(&batch).ok()?.into_array(1).ok()?;
    ScalarValue::try_from_array(&arr, 0).ok()
}

fn scalar_i128(v: &ScalarValue) -> Option<i128> {
    if v.is_null() {
        return None;
    }
    match v {
        ScalarValue::Int8(Some(x)) => Some(*x as i128),
        ScalarValue::Int16(Some(x)) => Some(*x as i128),
        ScalarValue::Int32(Some(x)) => Some(*x as i128),
        ScalarValue::Int64(Some(x)) => Some(*x as i128),
        ScalarValue::UInt8(Some(x)) => Some(*x as i128),
        ScalarValue::UInt16(Some(x)) => Some(*x as i128),
        ScalarValue::UInt32(Some(x)) => Some(*x as i128),
        ScalarValue::UInt64(Some(x)) => Some(*x as i128),
        _ => None,
    }
}

/// Convert a time-like scalar to nanoseconds since the epoch.
fn scalar_ts_ns(v: &ScalarValue) -> Option<i128> {
    match v {
        ScalarValue::TimestampSecond(Some(x), _) => Some(*x as i128 * 1_000_000_000),
        ScalarValue::TimestampMillisecond(Some(x), _) => Some(*x as i128 * 1_000_000),
        ScalarValue::TimestampMicrosecond(Some(x), _) => Some(*x as i128 * 1_000),
        ScalarValue::TimestampNanosecond(Some(x), _) => Some(*x as i128),
        ScalarValue::Date32(Some(d)) => Some(*d as i128 * 86_400_000_000_000),
        _ => None,
    }
}

fn clamp_u64(v: i128) -> u64 {
    v.clamp(0, u64::MAX as i128) as u64
}

/// Apply one conjunct to `b`; returns whether it was understood.
fn apply_filter(e: &Expr, session: &dyn Session, b: &mut Bounds) -> bool {
    match e {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) if *op == Operator::And => {
            let l = apply_filter(left, session, b);
            let r = apply_filter(right, session, b);
            l || r
        }
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            let (col, other, op) = match (column_name(left), column_name(right)) {
                (Some(c), None) => (c, right.as_ref(), *op),
                (None, Some(c)) => match op.swap() {
                    Some(sw) => (c, left.as_ref(), sw),
                    None => return false,
                },
                _ => return false,
            };
            if col != COL_OFFSET && col != COL_TIMESTAMP {
                return false;
            }
            let Some(v) = const_eval(other, session) else {
                return false;
            };
            if col == COL_OFFSET {
                let Some(v) = scalar_i128(&v) else {
                    return false;
                };
                match op {
                    Operator::Eq => {
                        b.lo(clamp_u64(v));
                        b.hi(clamp_u64(v + 1));
                    }
                    Operator::Gt => b.lo(clamp_u64(v + 1)),
                    Operator::GtEq => b.lo(clamp_u64(v)),
                    Operator::Lt => b.hi(clamp_u64(v)),
                    Operator::LtEq => b.hi(clamp_u64(v + 1)),
                    _ => return false,
                }
            } else {
                let Some(ns) = scalar_ts_ns(&v) else {
                    return false;
                };
                // The column is ms-truncated: compare at ms granularity.
                let ms = ns.div_euclid(1_000_000);
                let ms_ceil = if ns.rem_euclid(1_000_000) == 0 {
                    ms
                } else {
                    ms + 1
                };
                match op {
                    Operator::GtEq => b.ts_lo(clamp_u64(ms_ceil * 1_000_000)),
                    Operator::Gt => b.ts_lo(clamp_u64((ms + 1) * 1_000_000)),
                    Operator::Lt => b.ts_hi(clamp_u64(ms_ceil * 1_000_000)),
                    Operator::LtEq => b.ts_hi(clamp_u64((ms + 1) * 1_000_000)),
                    Operator::Eq => {
                        b.ts_lo(clamp_u64(ms_ceil * 1_000_000));
                        b.ts_hi(clamp_u64((ms + 1) * 1_000_000));
                    }
                    _ => return false,
                }
            }
            true
        }
        Expr::Between(bt) if !bt.negated => {
            let Some(col) = column_name(&bt.expr) else {
                return false;
            };
            let col = datafusion::common::Column::from_name(col);
            let lo = Expr::BinaryExpr(BinaryExpr::new(
                Box::new(Expr::Column(col.clone())),
                Operator::GtEq,
                bt.low.clone(),
            ));
            let hi = Expr::BinaryExpr(BinaryExpr::new(
                Box::new(Expr::Column(col)),
                Operator::LtEq,
                bt.high.clone(),
            ));
            let a = apply_filter(&lo, session, b);
            let c = apply_filter(&hi, session, b);
            a || c
        }
        _ => false,
    }
}

fn filter_is_pushable(e: &Expr) -> bool {
    match e {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            if *op == Operator::And {
                return filter_is_pushable(left) || filter_is_pushable(right);
            }
            matches!(
                (column_name(left), column_name(right)),
                (Some(COL_OFFSET | COL_TIMESTAMP), None) | (None, Some(COL_OFFSET | COL_TIMESTAMP))
            )
        }
        Expr::Between(bt) => matches!(column_name(&bt.expr), Some(COL_OFFSET | COL_TIMESTAMP)),
        _ => false,
    }
}

#[async_trait]
impl TableProvider for StreamTable {
    fn schema(&self) -> SchemaRef {
        stream_schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        // Always Inexact: DataFusion re-applies the predicate, so a bound we
        // can't use costs nothing in correctness.
        Ok(filters
            .iter()
            .map(|f| {
                if filter_is_pushable(f) {
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        session: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let (earliest, next) = self
            .storage
            .stream_bounds(&self.stream)
            .await
            .map_err(|e| ExqlError::from(e).into_df())?;
        let mut b = Bounds::default();
        for f in filters {
            apply_filter(f, session, &mut b);
        }
        let mut lo = earliest.0.max(b.lo.unwrap_or(0));
        let mut hi = next.0.min(b.hi.unwrap_or(u64::MAX));
        if let Some(ts) = b.ts_lo {
            let o = self
                .storage
                .seek_by_time(&self.stream, ts)
                .await
                .map_err(|e| ExqlError::from(e).into_df())?;
            lo = lo.max(o.0);
        }
        if let Some(ts) = b.ts_hi {
            let o = self
                .storage
                .seek_by_time(&self.stream, ts)
                .await
                .map_err(|e| ExqlError::from(e).into_df())?;
            hi = hi.min(o.0);
        }
        if hi < lo {
            hi = lo;
        }
        Ok(Arc::new(StreamScanExec::new(
            self.storage.clone(),
            self.stream.clone(),
            lo,
            hi,
            projection.cloned(),
            false,
            limit,
        )))
    }
}

/// Physical scan over `[lo, hi)` of a stream.
#[derive(Clone)]
pub struct StreamScanExec {
    storage: Arc<dyn StorageEngine>,
    stream: StreamName,
    lo: u64,
    hi: u64,
    projection: Option<Vec<usize>>,
    reverse: bool,
    fetch: Option<usize>,
    schema: SchemaRef,
    props: Arc<PlanProperties>,
}

impl fmt::Debug for StreamScanExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamScanExec")
            .field("stream", &self.stream.as_str())
            .field("lo", &self.lo)
            .field("hi", &self.hi)
            .field("reverse", &self.reverse)
            .field("fetch", &self.fetch)
            .finish()
    }
}

impl StreamScanExec {
    fn new(
        storage: Arc<dyn StorageEngine>,
        stream: StreamName,
        lo: u64,
        hi: u64,
        projection: Option<Vec<usize>>,
        reverse: bool,
        fetch: Option<usize>,
    ) -> Self {
        let full = stream_schema();
        let schema = match &projection {
            Some(p) => Arc::new(full.project(p).expect("valid projection")),
            None => full,
        };
        let mut eq = EquivalenceProperties::new(schema.clone());
        if let Ok(idx) = schema.index_of(COL_OFFSET) {
            let sort = PhysicalSortExpr {
                expr: Arc::new(PhysColumn::new(COL_OFFSET, idx)),
                options: SortOptions {
                    descending: reverse,
                    nulls_first: reverse,
                },
            };
            eq = EquivalenceProperties::new_with_orderings(schema.clone(), [vec![sort]]);
        }
        let props = PlanProperties::new(
            eq,
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            storage,
            stream,
            lo,
            hi,
            projection,
            reverse,
            fetch,
            schema,
            props: Arc::new(props),
        }
    }

    fn with_reverse(&self, reverse: bool) -> Self {
        Self::new(
            self.storage.clone(),
            self.stream.clone(),
            self.lo,
            self.hi,
            self.projection.clone(),
            reverse,
            self.fetch,
        )
    }

    /// Whether `order` is satisfied by this scan in forward (`Some(false)`)
    /// or reverse (`Some(true)`) direction.
    fn direction_for(&self, order: &[PhysicalSortExpr]) -> Option<bool> {
        let first = order.first()?;
        let col = first.expr.downcast_ref::<PhysColumn>()?;
        if col.name() != COL_OFFSET || self.schema.index_of(COL_OFFSET).ok()? != col.index() {
            return None;
        }
        Some(first.options.descending)
    }
}

impl DisplayAs for StreamScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "StreamScanExec: stream={}, offsets=[{}, {}), reverse={}, fetch={:?}",
            self.stream, self.lo, self.hi, self.reverse, self.fetch
        )
    }
}

async fn read_range(
    storage: &Arc<dyn StorageEngine>,
    stream: &StreamName,
    from: u64,
    to: u64,
) -> Result<Vec<StoredRecord>, ExqlError> {
    let mut out = Vec::new();
    let mut pos = from;
    while pos < to {
        let max = ((to - pos) as usize).min(SCAN_BATCH);
        let batch = storage
            .read_batch(
                stream,
                Offset(pos),
                ReadLimits {
                    max_records: max,
                    max_bytes: SCAN_BYTES,
                },
            )
            .await?;
        if batch.records.is_empty() {
            break;
        }
        for r in batch.records {
            if r.offset.0 >= to {
                break;
            }
            out.push(r);
        }
        if batch.next_offset.0 <= pos {
            break;
        }
        pos = batch.next_offset.0;
        if out.len() >= SCAN_BATCH {
            break;
        }
    }
    Ok(out)
}

impl ExecutionPlan for StreamScanExec {
    fn name(&self) -> &str {
        "StreamScanExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DFResult<TreeNodeRecursion>,
    ) -> DFResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    // `supports_limit_pushdown` means "push a parent's limit through me to
    // my children". A leaf has none, so returning `true` made DataFusion's
    // LimitPushdown drop the limit (`ORDER BY offset LIMIT n` returned
    // every row). The limit is absorbed by `with_fetch` instead.
    fn supports_limit_pushdown(&self) -> bool {
        false
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        let mut s = self.clone();
        s.fetch = limit;
        Some(Arc::new(s))
    }

    fn fetch(&self) -> Option<usize> {
        self.fetch
    }

    fn try_pushdown_sort(
        &self,
        order: &[PhysicalSortExpr],
    ) -> DFResult<SortOrderPushdownResult<Arc<dyn ExecutionPlan>>> {
        Ok(match self.direction_for(order) {
            Some(rev) => SortOrderPushdownResult::Exact {
                inner: Arc::new(self.with_reverse(rev)),
            },
            None => SortOrderPushdownResult::Unsupported,
        })
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        struct St {
            storage: Arc<dyn StorageEngine>,
            stream: StreamName,
            lo: u64,
            hi: u64,
            projection: Option<Vec<usize>>,
            reverse: bool,
            remaining: Option<usize>,
        }
        let st = St {
            storage: self.storage.clone(),
            stream: self.stream.clone(),
            lo: self.lo,
            hi: self.hi,
            projection: self.projection.clone(),
            reverse: self.reverse,
            remaining: self.fetch,
        };
        let s = futures_util::stream::try_unfold(st, |mut st| async move {
            loop {
                if st.lo >= st.hi || st.remaining == Some(0) {
                    return Ok(None);
                }
                let mut want = SCAN_BATCH as u64;
                if let Some(r) = st.remaining {
                    want = want.min(r as u64);
                }
                let mut records = if st.reverse {
                    let from = st.hi.saturating_sub(want).max(st.lo);
                    let mut recs = read_range(&st.storage, &st.stream, from, st.hi)
                        .await
                        .map_err(|e| e.into_df())?;
                    st.hi = from;
                    recs.reverse();
                    recs
                } else {
                    let to = st.hi.min(st.lo.saturating_add(want));
                    let recs = read_range(&st.storage, &st.stream, st.lo, to)
                        .await
                        .map_err(|e| e.into_df())?;
                    st.lo = match recs.last() {
                        Some(r) => r.offset.0 + 1,
                        None => st.hi,
                    };
                    recs
                };
                if let Some(r) = st.remaining.as_mut() {
                    records.truncate(*r);
                    *r -= records.len();
                }
                if records.is_empty() {
                    continue;
                }
                let batch = records_to_batch_projected(&records, st.projection.as_deref())
                    .map_err(|e| e.into_df())?;
                // Let timeouts and cancellation in between batches.
                tokio::task::consume_budget().await;
                return Ok(Some((batch, st)));
            }
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            s,
        )))
    }
}

/// Rewrites `SortExec(offset DESC, fetch n)` over an order-preserving chain
/// ending in a forward [`StreamScanExec`] into a limit over a reverse scan,
/// so `… WHERE … ORDER BY offset DESC LIMIT n` reads only the tail.
#[derive(Debug, Default)]
pub struct ReverseTailRule;

fn reverse_chain(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    if let Some(scan) = plan.downcast_ref::<StreamScanExec>() {
        return Some(Arc::new(scan.with_reverse(!scan.reverse)));
    }
    let children = plan.children();
    if children.len() != 1
        || !plan
            .maintains_input_order()
            .first()
            .copied()
            .unwrap_or(false)
        || plan.properties().partitioning.partition_count() != 1
        || plan.fetch().is_some()
    {
        return None;
    }
    let new_child = reverse_chain(children[0])?;
    plan.clone()
        .replace_children(
            vec![new_child],
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
        .ok()
}

fn chain_scan(plan: &Arc<dyn ExecutionPlan>) -> Option<&StreamScanExec> {
    if let Some(scan) = plan.downcast_ref::<StreamScanExec>() {
        return Some(scan);
    }
    let children = plan.children();
    if children.len() == 1 {
        chain_scan(children[0])
    } else {
        None
    }
}

impl PhysicalOptimizerRule for ReverseTailRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        plan.transform_down(|node| {
            let Some(sort) = node.downcast_ref::<SortExec>() else {
                return Ok(Transformed::no(node));
            };
            let Some(fetch) = sort.fetch() else {
                return Ok(Transformed::no(node));
            };
            if sort.preserve_partitioning() {
                return Ok(Transformed::no(node));
            }
            let input = sort.input();
            let Some(scan) = chain_scan(input) else {
                return Ok(Transformed::no(node));
            };
            // The sort key must be the scan's offset column, as seen at the
            // sort's input (projections in between may rename/reorder).
            let order: Vec<PhysicalSortExpr> = sort.expr().iter().cloned().collect();
            let Some(first) = order.first() else {
                return Ok(Transformed::no(node));
            };
            let Some(col) = first.expr.downcast_ref::<PhysColumn>() else {
                return Ok(Transformed::no(node));
            };
            if col.name() != COL_OFFSET || !first.options.descending || scan.reverse {
                return Ok(Transformed::no(node));
            }
            let Some(new_input) = reverse_chain(input) else {
                return Ok(Transformed::no(node));
            };
            // Verify the rewritten input now reports offset DESC ordering.
            if !new_input
                .equivalence_properties()
                .ordering_satisfy(vec![first.clone()])
                .unwrap_or(false)
            {
                return Ok(Transformed::no(node));
            }
            let limit: Arc<dyn ExecutionPlan> =
                Arc::new(GlobalLimitExec::new(new_input, 0, Some(fetch)));
            Ok(Transformed::yes(limit))
        })
        .data()
    }

    fn name(&self) -> &str {
        "exspeed_reverse_tail"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

use datafusion::physical_plan::{
    ChildrenPropertiesMode, ExecutionPlanProperties, ReplaceChildrenOptions,
};
