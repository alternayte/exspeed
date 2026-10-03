//! Stateful operators: grouped/windowed aggregation, stream-stream join and
//! stream-table join.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::array::{
    new_empty_array, Array, ArrayRef, AsArray, BooleanArray, Int64Array, RecordBatch,
    RecordBatchOptions, TimestampMillisecondArray, UInt32Array, UInt64Array,
};
use datafusion::arrow::compute::{cast, filter, prep_null_mask_filter, take};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef};
use datafusion::arrow::row::{RowConverter, SortField};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::Accumulator;
use datafusion::physical_expr::aggregate::AggregateFunctionExpr;
use datafusion::physical_expr::PhysicalExpr;

use super::rows::{batch_from_rows, eval, eval_bool, Chain, RowId, Rows};
use crate::error::ExqlError;
use crate::sql::WindowSpec;
use crate::tables::MaterializedTable;

// ---------------------------------------------------------------------------
// Keys
// ---------------------------------------------------------------------------

/// Encodes composite keys to comparable bytes.
pub struct KeyEncoder {
    types: Vec<DataType>,
    conv: Option<RowConverter>,
}

impl std::fmt::Debug for KeyEncoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyEncoder").field("types", &self.types).finish()
    }
}

impl KeyEncoder {
    pub fn new(types: Vec<DataType>) -> Result<Self, ExqlError> {
        let conv = if types.is_empty() {
            None
        } else {
            Some(RowConverter::new(
                types.iter().map(|t| SortField::new(t.clone())).collect(),
            )?)
        };
        Ok(Self { types, conv })
    }

    pub fn types(&self) -> &[DataType] {
        &self.types
    }

    fn conform(&self, cols: &[ArrayRef]) -> Result<Vec<ArrayRef>, ExqlError> {
        cols.iter()
            .zip(&self.types)
            .map(|(c, t)| {
                if c.data_type() == t {
                    Ok(c.clone())
                } else {
                    cast(c, t).map_err(ExqlError::from)
                }
            })
            .collect()
    }

    /// One key per row (`n` rows).
    pub fn encode(&self, cols: &[ArrayRef], n: usize) -> Result<Vec<Vec<u8>>, ExqlError> {
        let Some(conv) = &self.conv else {
            return Ok(vec![Vec::new(); n]);
        };
        let cols = self.conform(cols)?;
        let rows = conv.convert_columns(&cols)?;
        Ok((0..n).map(|i| rows.row(i).as_ref().to_vec()).collect())
    }

    /// Like [`Self::encode`], but `None` where any key column is NULL (SQL
    /// equality never matches NULL).
    pub fn encode_join(&self, cols: &[ArrayRef], n: usize) -> Result<Vec<Option<Vec<u8>>>, ExqlError> {
        let keys = self.encode(cols, n)?;
        Ok(keys
            .into_iter()
            .enumerate()
            .map(|(i, k)| {
                if cols.iter().any(|c| c.is_null(i)) {
                    None
                } else {
                    Some(k)
                }
            })
            .collect())
    }
}

fn take_arr(a: &ArrayRef, idx: &UInt32Array) -> Result<ArrayRef, ExqlError> {
    Ok(take(a.as_ref(), idx, None)?)
}

fn ts_array(v: Vec<i64>) -> ArrayRef {
    Arc::new(TimestampMillisecondArray::from(v).with_timezone("UTC"))
}

fn scalar_ms(v: &ScalarValue) -> Option<i64> {
    match v {
        ScalarValue::TimestampMillisecond(Some(x), _) => Some(*x),
        ScalarValue::Int64(Some(x)) => Some(*x),
        _ => None,
    }
}

fn arrays_from_scalars(
    rows: impl Iterator<Item = ScalarValue>,
    ty: &DataType,
    n: usize,
) -> Result<ArrayRef, ExqlError> {
    if n == 0 {
        return Ok(new_empty_array(ty));
    }
    let arr = ScalarValue::iter_to_array(rows)?;
    if arr.data_type() != ty && !matches!(ty, DataType::List(_) | DataType::Struct(_)) {
        Ok(cast(&arr, ty)?)
    } else {
        Ok(arr)
    }
}

fn mk_batch(fields: Vec<Field>, cols: Vec<ArrayRef>, n: usize) -> Result<RecordBatch, ExqlError> {
    Ok(RecordBatch::try_new_with_options(
        Arc::new(Schema::new(fields)),
        cols,
        &RecordBatchOptions::new().with_row_count(Some(n)),
    )?)
}

// ---------------------------------------------------------------------------
// Aggregation
// ---------------------------------------------------------------------------

/// One aggregate of a GROUP BY.
pub struct AggDef {
    pub expr: Arc<AggregateFunctionExpr>,
    pub args: Vec<Arc<dyn PhysicalExpr>>,
    pub filter: Option<Arc<dyn PhysicalExpr>>,
    pub state_fields: Vec<FieldRef>,
}

struct Group {
    key: Vec<ScalarValue>,
    accs: Vec<Box<dyn Accumulator>>,
    window: Option<(i64, i64)>,
}

/// Grouped (optionally windowed) aggregation with one DataFusion
/// accumulator per aggregate per group.
pub struct AggOp {
    /// Grouping expressions, excluding the window bounds.
    pub group_exprs: Vec<Arc<dyn PhysicalExpr>>,
    pub window: Option<WindowSpec>,
    pub aggs: Vec<AggDef>,
    /// The Aggregate node's output: group columns, then aggregates.
    pub out_schema: SchemaRef,
    keys: KeyEncoder,
    groups: HashMap<Vec<u8>, Group>,
}

/// Result of feeding rows to an aggregation.
pub struct AggUpdate {
    /// Group keys whose value changed, in first-change order.
    pub changed: Vec<Vec<u8>>,
    /// Max event time of the rows that changed each group (same order).
    pub changed_et: Vec<i64>,
    pub late: u64,
}

impl AggOp {
    pub fn new(
        group_exprs: Vec<Arc<dyn PhysicalExpr>>,
        window: Option<WindowSpec>,
        aggs: Vec<AggDef>,
        out_schema: SchemaRef,
    ) -> Result<Self, ExqlError> {
        let nkeys = group_exprs.len() + if window.is_some() { 2 } else { 0 };
        let types = out_schema
            .fields()
            .iter()
            .take(nkeys)
            .map(|f| f.data_type().clone())
            .collect();
        Ok(Self {
            group_exprs,
            window,
            aggs,
            out_schema,
            keys: KeyEncoder::new(types)?,
            groups: HashMap::new(),
        })
    }

    pub fn num_keys(&self) -> usize {
        self.keys.types().len()
    }

    pub fn is_global(&self) -> bool {
        self.num_keys() == 0
    }

    pub fn len(&self) -> usize {
        self.groups.len()
    }

    pub fn is_empty(&self) -> bool {
        self.groups.is_empty()
    }

    fn new_group(&self, key: Vec<ScalarValue>) -> Result<Group, ExqlError> {
        let window = if self.window.is_some() {
            Some((
                scalar_ms(&key[0]).unwrap_or_default(),
                scalar_ms(&key[1]).unwrap_or_default(),
            ))
        } else {
            None
        };
        Ok(Group {
            key,
            accs: self
                .aggs
                .iter()
                .map(|a| a.expr.create_accumulator())
                .collect::<Result<_, _>>()?,
            window,
        })
    }

    /// A global aggregate (no GROUP BY, no window) always has its one row.
    pub fn init_global(&mut self) -> Result<Option<Vec<u8>>, ExqlError> {
        if self.is_global() && self.window.is_none() && self.groups.is_empty() {
            let g = self.new_group(vec![])?;
            self.groups.insert(vec![], g);
            return Ok(Some(vec![]));
        }
        Ok(None)
    }

    /// Feed rows. Window assignments that end at or before `close_wm` are
    /// late and dropped.
    pub fn update(&mut self, rows: &Rows, close_wm: Option<i64>) -> Result<AggUpdate, ExqlError> {
        let mut out = AggUpdate {
            changed: vec![],
            changed_et: vec![],
            late: 0,
        };
        let n = rows.len();
        if n == 0 {
            return Ok(out);
        }
        let mut key_cols: Vec<ArrayRef> = self
            .group_exprs
            .iter()
            .map(|e| eval(e, &rows.batch))
            .collect::<Result<_, _>>()?;
        let mut args: Vec<Vec<ArrayRef>> = self
            .aggs
            .iter()
            .map(|a| a.args.iter().map(|e| eval(e, &rows.batch)).collect())
            .collect::<Result<_, _>>()?;
        let mut filters: Vec<Option<ArrayRef>> = self
            .aggs
            .iter()
            .map(|a| a.filter.as_ref().map(|f| eval(f, &rows.batch)).transpose())
            .collect::<Result<_, _>>()?;
        let mut et: Vec<i64> = rows.et.clone();
        let mut m = n;

        if let Some(spec) = self.window {
            let mut idx: Vec<u32> = Vec::with_capacity(n);
            let mut starts = Vec::with_capacity(n);
            let mut ends = Vec::with_capacity(n);
            for (i, &t) in rows.et.iter().enumerate() {
                let mut any = false;
                for (s, e) in spec.assign(t) {
                    if close_wm.is_some_and(|w| e <= w) {
                        continue;
                    }
                    any = true;
                    idx.push(i as u32);
                    starts.push(s);
                    ends.push(e);
                }
                if !any {
                    out.late += 1;
                }
            }
            let ia = UInt32Array::from(idx.clone());
            key_cols = key_cols.iter().map(|c| take_arr(c, &ia)).collect::<Result<_, _>>()?;
            key_cols.insert(0, ts_array(ends));
            key_cols.insert(0, ts_array(starts));
            for a in args.iter_mut() {
                *a = a.iter().map(|c| take_arr(c, &ia)).collect::<Result<_, _>>()?;
            }
            for f in filters.iter_mut().flatten() {
                *f = take_arr(f, &ia)?;
            }
            et = idx.iter().map(|&i| rows.et[i as usize]).collect();
            m = idx.len();
        }
        if m == 0 {
            return Ok(out);
        }

        let keys = self.keys.encode(&key_cols, m)?;
        let mut order: Vec<(Vec<u8>, Vec<u32>)> = Vec::new();
        let mut pos: HashMap<Vec<u8>, usize> = HashMap::new();
        for (i, k) in keys.into_iter().enumerate() {
            match pos.get(&k) {
                Some(&p) => order[p].1.push(i as u32),
                None => {
                    pos.insert(k.clone(), order.len());
                    order.push((k, vec![i as u32]));
                }
            }
        }
        for (k, ids) in order {
            if !self.groups.contains_key(&k) {
                let key: Vec<ScalarValue> = key_cols
                    .iter()
                    .map(|c| ScalarValue::try_from_array(c, ids[0] as usize))
                    .collect::<Result<_, _>>()?;
                let g = self.new_group(key)?;
                self.groups.insert(k.clone(), g);
            }
            let group = self.groups.get_mut(&k).expect("inserted");
            let ia = UInt32Array::from(ids.clone());
            for (j, acc) in group.accs.iter_mut().enumerate() {
                let mut vals: Vec<ArrayRef> = args[j]
                    .iter()
                    .map(|c| take_arr(c, &ia))
                    .collect::<Result<_, _>>()?;
                if let Some(f) = &filters[j] {
                    let mask = take_arr(f, &ia)?;
                    let mask = mask.as_boolean_opt().cloned().ok_or_else(|| {
                        ExqlError::Plan("aggregate FILTER must be boolean".into())
                    })?;
                    let mask = prep_null_mask_filter(&mask);
                    vals = vals
                        .iter()
                        .map(|v| filter(v.as_ref(), &mask))
                        .collect::<Result<_, _>>()?;
                }
                acc.update_batch(&vals)?;
            }
            out.changed_et
                .push(ids.iter().map(|&i| et[i as usize]).max().unwrap_or(i64::MIN));
            out.changed.push(k);
        }
        Ok(out)
    }

    /// The Aggregate output row of a group.
    pub fn row(&mut self, key: &[u8]) -> Result<Option<Vec<ScalarValue>>, ExqlError> {
        let Some(g) = self.groups.get_mut(key) else {
            return Ok(None);
        };
        let mut row = g.key.clone();
        for acc in g.accs.iter_mut() {
            row.push(acc.evaluate()?);
        }
        Ok(Some(row))
    }

    /// The group-by values of a group.
    pub fn key_values(&self, key: &[u8]) -> Option<Vec<ScalarValue>> {
        self.groups.get(key).map(|g| g.key.clone())
    }

    /// Window end of a group, if windowed.
    pub fn window_of(&self, key: &[u8]) -> Option<(i64, i64)> {
        self.groups.get(key).and_then(|g| g.window)
    }

    /// Remove and return (in window order) the groups whose window ended at
    /// or before `wm`.
    pub fn close(&mut self, wm: Option<i64>) -> Vec<Vec<u8>> {
        let Some(wm) = wm else {
            return vec![];
        };
        if self.window.is_none() {
            return vec![];
        }
        let mut closed: Vec<(i64, i64, Vec<u8>)> = self
            .groups
            .iter()
            .filter_map(|(k, g)| {
                let (s, e) = g.window?;
                (e <= wm).then(|| (e, s, k.clone()))
            })
            .collect();
        closed.sort();
        closed.into_iter().map(|(_, _, k)| k).collect()
    }

    pub fn remove(&mut self, key: &[u8]) {
        self.groups.remove(key);
    }

    /// All group keys in a deterministic order.
    pub fn all_keys(&self) -> Vec<Vec<u8>> {
        let mut v: Vec<Vec<u8>> = self.groups.keys().cloned().collect();
        v.sort();
        v
    }

    /// Serialize all groups (keys + accumulator states) to one batch.
    pub fn snapshot(&mut self) -> Result<RecordBatch, ExqlError> {
        let keys = self.all_keys();
        let n = keys.len();
        let mut fields = vec![];
        let mut cols: Vec<ArrayRef> = vec![];
        for (i, t) in self.keys.types().to_vec().iter().enumerate() {
            let arr = arrays_from_scalars(
                keys.iter().map(|k| self.groups[k].key[i].clone()),
                t,
                n,
            )?;
            fields.push(Field::new(format!("k{i}"), arr.data_type().clone(), true));
            cols.push(arr);
        }
        let mut states: Vec<Vec<Vec<ScalarValue>>> = Vec::with_capacity(n);
        for k in &keys {
            let g = self.groups.get_mut(k).expect("key exists");
            let mut per = vec![];
            for acc in g.accs.iter_mut() {
                per.push(acc.state()?);
            }
            states.push(per);
        }
        for (j, a) in self.aggs.iter().enumerate() {
            for (s, f) in a.state_fields.iter().enumerate() {
                let arr = arrays_from_scalars(
                    states.iter().map(|st| st[j][s].clone()),
                    f.data_type(),
                    n,
                )?;
                fields.push(Field::new(format!("a{j}_{s}"), arr.data_type().clone(), true));
                cols.push(arr);
            }
        }
        mk_batch(fields, cols, n)
    }

    /// Restore groups from [`Self::snapshot`].
    pub fn restore(&mut self, batch: &RecordBatch) -> Result<(), ExqlError> {
        self.groups.clear();
        let n = batch.num_rows();
        let nk = self.num_keys();
        let key_cols: Vec<ArrayRef> = (0..nk).map(|i| batch.column(i).clone()).collect();
        let keys = self.keys.encode(&key_cols, n)?;
        for (r, k) in keys.into_iter().enumerate() {
            let key: Vec<ScalarValue> = key_cols
                .iter()
                .map(|c| ScalarValue::try_from_array(c, r))
                .collect::<Result<_, _>>()?;
            let mut g = self.new_group(key)?;
            let mut col = nk;
            for (j, a) in self.aggs.iter().enumerate() {
                let states: Vec<ArrayRef> = (0..a.state_fields.len())
                    .map(|s| batch.column(col + s).slice(r, 1))
                    .collect();
                col += a.state_fields.len();
                g.accs[j].merge_batch(&states)?;
            }
            self.groups.insert(k, g);
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Stream-stream join
// ---------------------------------------------------------------------------

/// One input of a join: a source, the stateless steps over it, and the
/// equi-join key expressions (evaluated on the steps' output).
#[derive(Debug, Clone)]
pub struct SideDef {
    pub src: usize,
    pub chain: Chain,
    pub keys: Vec<Arc<dyn PhysicalExpr>>,
    pub schema: SchemaRef,
}

#[derive(Debug, Clone)]
struct BufRow {
    values: Vec<ScalarValue>,
    et: i64,
    key: Option<Vec<u8>>,
    matched: bool,
}

#[derive(Debug, Default)]
struct SideBuf {
    rows: BTreeMap<u64, BufRow>,
    index: HashMap<Vec<u8>, BTreeSet<u64>>,
    by_et: BTreeSet<(i64, u64)>,
}

impl SideBuf {
    fn insert(&mut self, off: u64, row: BufRow) {
        if let Some(k) = &row.key {
            self.index.entry(k.clone()).or_default().insert(off);
        }
        self.by_et.insert((row.et, off));
        self.rows.insert(off, row);
    }

    /// Remove rows with `et < bound`, in offset order.
    fn evict_before(&mut self, bound: i64) -> Vec<(u64, BufRow)> {
        let mut gone = vec![];
        while let Some(&(et, off)) = self.by_et.first() {
            if et >= bound {
                break;
            }
            self.by_et.pop_first();
            if let Some(row) = self.rows.remove(&off) {
                if let Some(k) = &row.key {
                    if let Some(set) = self.index.get_mut(k) {
                        set.remove(&off);
                        if set.is_empty() {
                            self.index.remove(k);
                        }
                    }
                }
                gone.push((off, row));
            }
        }
        gone.sort_by_key(|(o, _)| *o);
        gone
    }

    fn len(&self) -> usize {
        self.rows.len()
    }
}

/// Symmetric hash join of two streams with `|t_l - t_r| <= WITHIN`.
pub struct StreamJoin {
    pub left: SideDef,
    pub right: SideDef,
    pub left_join: bool,
    pub within_ms: i64,
    pub filter: Option<Arc<dyn PhysicalExpr>>,
    pub out_schema: SchemaRef,
    keys: KeyEncoder,
    lbuf: SideBuf,
    rbuf: SideBuf,
}

/// Result of a join step.
pub struct JoinOutput {
    pub rows: Rows,
    pub late: u64,
}

impl StreamJoin {
    pub fn new(
        left: SideDef,
        right: SideDef,
        left_join: bool,
        within_ms: i64,
        filter: Option<Arc<dyn PhysicalExpr>>,
        out_schema: SchemaRef,
        key_types: Vec<DataType>,
    ) -> Result<Self, ExqlError> {
        Ok(Self {
            left,
            right,
            left_join,
            within_ms,
            filter,
            out_schema,
            keys: KeyEncoder::new(key_types)?,
            lbuf: SideBuf::default(),
            rbuf: SideBuf::default(),
        })
    }

    pub fn buffered(&self) -> (usize, usize) {
        (self.lbuf.len(), self.rbuf.len())
    }

    fn side_keys(&self, side: &SideDef, rows: &Rows) -> Result<Vec<Option<Vec<u8>>>, ExqlError> {
        let cols: Vec<ArrayRef> = side
            .keys
            .iter()
            .map(|e| eval(e, &rows.batch))
            .collect::<Result<_, _>>()?;
        self.keys.encode_join(&cols, rows.len())
    }

    fn drop_late(rows: Rows, late_wm: Option<i64>) -> Result<(Rows, u64), ExqlError> {
        let Some(w) = late_wm else {
            return Ok((rows, 0));
        };
        let mask: BooleanArray = rows.et.iter().map(|&t| Some(t >= w)).collect();
        let late = mask.values().count_set_bits();
        let late = (rows.len() - late) as u64;
        if late == 0 {
            return Ok((rows, 0));
        }
        Ok((rows.filter(&mask)?, late))
    }

    fn off_of(id: &RowId) -> u64 {
        match id {
            RowId::Src { off, .. } => *off,
            _ => 0,
        }
    }

    /// Join new rows of both sides (already through their side chains).
    /// Rows with event time before `late_wm` are late and dropped.
    pub fn process(&mut self, left: Rows, right: Rows, late_wm: Option<i64>) -> Result<JoinOutput, ExqlError> {
        let (left, l_late) = Self::drop_late(left, late_wm)?;
        let (right, r_late) = Self::drop_late(right, late_wm)?;
        let lkeys = self.side_keys(&self.left, &left)?;
        let rkeys = self.side_keys(&self.right, &right)?;
        let within = self.within_ms;
        // (left offset, right offset)
        let mut cands: Vec<(u64, u64)> = vec![];
        let mut new_right: Vec<(u64, BufRow)> = vec![];
        let close = |a: i64, b: i64| (a - b).abs() <= within;

        // New left rows probe the existing right buffer, then are buffered.
        for i in 0..left.len() {
            let off = Self::off_of(&left.ids[i]);
            let key = lkeys[i].clone();
            let et = left.et[i];
            if let Some(k) = &key {
                if let Some(set) = self.rbuf.index.get(k) {
                    for &roff in set {
                        if close(et, self.rbuf.rows[&roff].et) {
                            cands.push((off, roff));
                        }
                    }
                }
            }
            if key.is_some() || self.left_join {
                self.lbuf.insert(
                    off,
                    BufRow {
                        values: left.row_values(i)?,
                        et,
                        key,
                        matched: false,
                    },
                );
            }
        }
        // New right rows probe the (updated) left buffer, then are buffered.
        for j in 0..right.len() {
            let Some(k) = rkeys[j].clone() else {
                continue;
            };
            let off = Self::off_of(&right.ids[j]);
            let et = right.et[j];
            if let Some(set) = self.lbuf.index.get(&k) {
                for &loff in set {
                    if close(self.lbuf.rows[&loff].et, et) {
                        cands.push((loff, off));
                    }
                }
            }
            new_right.push((
                off,
                BufRow {
                    values: right.row_values(j)?,
                    et,
                    key: Some(k),
                    matched: false,
                },
            ));
        }
        for (off, row) in new_right {
            self.rbuf.insert(off, row);
        }
        cands.sort();
        cands.dedup();

        let rows: Vec<Vec<ScalarValue>> = cands
            .iter()
            .map(|(l, r)| {
                let mut v = self.lbuf.rows[l].values.clone();
                v.extend(self.rbuf.rows[r].values.iter().cloned());
                v
            })
            .collect();
        let batch = batch_from_rows(&self.out_schema, &rows)?;
        let keep: Vec<bool> = match &self.filter {
            Some(f) if !rows.is_empty() => {
                let m = eval_bool(f, &batch)?;
                (0..m.len()).map(|i| m.is_valid(i) && m.value(i)).collect()
            }
            _ => vec![true; rows.len()],
        };
        let mut idx = vec![];
        let mut et = vec![];
        let mut ids = vec![];
        for (n, (l, r)) in cands.iter().enumerate() {
            if !keep[n] {
                continue;
            }
            if let Some(lr) = self.lbuf.rows.get_mut(l) {
                lr.matched = true;
            }
            idx.push(n as u32);
            et.push(self.lbuf.rows[l].et.max(self.rbuf.rows[r].et));
            ids.push(RowId::Pair { l: *l, r: *r });
        }
        let all = Rows {
            batch,
            et: vec![0; rows.len()],
            ids: vec![RowId::Group(0); rows.len()],
        };
        let mut out = all.take(&idx)?;
        out.et = et;
        out.ids = ids;
        Ok(JoinOutput {
            rows: out,
            late: l_late + r_late,
        })
    }

    /// Evict rows that can no longer match once the watermark is `wm`;
    /// for LEFT joins, return the unmatched left rows (right side NULL).
    pub fn advance(&mut self, wm: Option<i64>) -> Result<Rows, ExqlError> {
        let Some(wm) = wm else {
            return Ok(Rows::empty(self.out_schema.clone()));
        };
        let bound = wm.saturating_sub(self.within_ms);
        let gone_left = self.lbuf.evict_before(bound);
        self.rbuf.evict_before(bound);
        if !self.left_join {
            return Ok(Rows::empty(self.out_schema.clone()));
        }
        let nulls: Vec<ScalarValue> = self
            .right
            .schema
            .fields()
            .iter()
            .map(|f| ScalarValue::try_from(f.data_type()))
            .collect::<Result<_, _>>()?;
        let mut rows = vec![];
        let mut et = vec![];
        let mut ids = vec![];
        for (off, r) in gone_left {
            if r.matched {
                continue;
            }
            let mut v = r.values;
            v.extend(nulls.iter().cloned());
            rows.push(v);
            et.push(r.et);
            ids.push(RowId::Unmatched { l: off });
        }
        Ok(Rows {
            batch: batch_from_rows(&self.out_schema, &rows)?,
            et,
            ids,
        })
    }

    fn side_snapshot(buf: &SideBuf, schema: &SchemaRef) -> Result<RecordBatch, ExqlError> {
        let rows: Vec<Vec<ScalarValue>> = buf.rows.values().map(|r| r.values.clone()).collect();
        let base = batch_from_rows(schema, &rows)?;
        let n = rows.len();
        let mut fields: Vec<Field> = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| Field::new(format!("c{i}"), f.data_type().clone(), true))
            .collect();
        let mut cols: Vec<ArrayRef> = base.columns().to_vec();
        fields.push(Field::new("__off", DataType::UInt64, false));
        cols.push(Arc::new(UInt64Array::from_iter_values(buf.rows.keys().copied())));
        fields.push(Field::new("__et", DataType::Int64, false));
        cols.push(Arc::new(Int64Array::from_iter_values(buf.rows.values().map(|r| r.et))));
        fields.push(Field::new("__matched", DataType::Boolean, false));
        cols.push(Arc::new(BooleanArray::from(
            buf.rows.values().map(|r| r.matched).collect::<Vec<_>>(),
        )));
        mk_batch(fields, cols, n)
    }

    /// Serialize both buffers.
    pub fn snapshot(&self) -> Result<(RecordBatch, RecordBatch), ExqlError> {
        Ok((
            Self::side_snapshot(&self.lbuf, &self.left.schema)?,
            Self::side_snapshot(&self.rbuf, &self.right.schema)?,
        ))
    }

    fn side_restore(&self, side: &SideDef, batch: &RecordBatch) -> Result<SideBuf, ExqlError> {
        let nf = side.schema.fields().len();
        let n = batch.num_rows();
        let cols: Vec<ArrayRef> = (0..nf).map(|i| batch.column(i).clone()).collect();
        let data = super::rows::make_batch(&side.schema, cols, n)?;
        let offs = batch.column(nf).as_primitive::<datafusion::arrow::datatypes::UInt64Type>();
        let ets = batch.column(nf + 1).as_primitive::<datafusion::arrow::datatypes::Int64Type>();
        let matched = batch.column(nf + 2).as_boolean();
        let rows = Rows {
            batch: data,
            et: ets.values().to_vec(),
            ids: offs.values().iter().map(|&off| RowId::Src { src: 0, off }).collect(),
        };
        let keys = self.side_keys(side, &rows)?;
        let mut buf = SideBuf::default();
        for i in 0..n {
            buf.insert(
                offs.value(i),
                BufRow {
                    values: rows.row_values(i)?,
                    et: ets.value(i),
                    key: keys[i].clone(),
                    matched: matched.value(i),
                },
            );
        }
        Ok(buf)
    }

    pub fn restore(&mut self, left: &RecordBatch, right: &RecordBatch) -> Result<(), ExqlError> {
        self.lbuf = self.side_restore(&self.left, left)?;
        self.rbuf = self.side_restore(&self.right, right)?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Stream-table join
// ---------------------------------------------------------------------------

/// Lookup join of a stream against a materialized table that updates live.
pub struct TableJoin {
    pub stream: SideDef,
    pub table: Arc<MaterializedTable>,
    /// Steps over the table's rows (from the table's schema to its side of
    /// the join).
    pub table_chain: Chain,
    pub table_keys: Vec<Arc<dyn PhysicalExpr>>,
    pub table_side_schema: SchemaRef,
    pub stream_is_left: bool,
    pub left_join: bool,
    pub filter: Option<Arc<dyn PhysicalExpr>>,
    pub out_schema: SchemaRef,
    keys: KeyEncoder,
    cache: Option<(u64, HashMap<Vec<u8>, Vec<Vec<ScalarValue>>>)>,
}

impl TableJoin {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        stream: SideDef,
        table: Arc<MaterializedTable>,
        table_chain: Chain,
        table_keys: Vec<Arc<dyn PhysicalExpr>>,
        table_side_schema: SchemaRef,
        stream_is_left: bool,
        left_join: bool,
        filter: Option<Arc<dyn PhysicalExpr>>,
        out_schema: SchemaRef,
        key_types: Vec<DataType>,
    ) -> Result<Self, ExqlError> {
        Ok(Self {
            stream,
            table,
            table_chain,
            table_keys,
            table_side_schema,
            stream_is_left,
            left_join,
            filter,
            out_schema,
            keys: KeyEncoder::new(key_types)?,
            cache: None,
        })
    }

    fn refresh(&mut self) -> Result<(), ExqlError> {
        let (batch, version) = self.table.snapshot()?;
        if self.cache.as_ref().is_some_and(|(v, _)| *v == version) {
            return Ok(());
        }
        let n = batch.num_rows();
        let rows = Rows {
            batch,
            et: vec![0; n],
            ids: vec![RowId::Group(0); n],
        };
        let rows = self.table_chain.apply(rows)?;
        let cols: Vec<ArrayRef> = self
            .table_keys
            .iter()
            .map(|e| eval(e, &rows.batch))
            .collect::<Result<_, _>>()?;
        let keys = self.keys.encode_join(&cols, rows.len())?;
        let mut index: HashMap<Vec<u8>, Vec<Vec<ScalarValue>>> = HashMap::new();
        for (i, k) in keys.into_iter().enumerate() {
            if let Some(k) = k {
                index.entry(k).or_default().push(rows.row_values(i)?);
            }
        }
        self.cache = Some((version, index));
        Ok(())
    }

    pub fn process(&mut self, rows: Rows) -> Result<Rows, ExqlError> {
        if rows.is_empty() {
            return Ok(Rows::empty(self.out_schema.clone()));
        }
        self.refresh()?;
        let cols: Vec<ArrayRef> = self
            .stream
            .keys
            .iter()
            .map(|e| eval(e, &rows.batch))
            .collect::<Result<_, _>>()?;
        let keys = self.keys.encode_join(&cols, rows.len())?;
        let index = &self.cache.as_ref().expect("refreshed").1;
        let nulls: Vec<ScalarValue> = self
            .table_side_schema
            .fields()
            .iter()
            .map(|f| ScalarValue::try_from(f.data_type()))
            .collect::<Result<_, _>>()?;
        let join = |s: &[ScalarValue], t: &[ScalarValue]| -> Vec<ScalarValue> {
            if self.stream_is_left {
                s.iter().chain(t.iter()).cloned().collect()
            } else {
                t.iter().chain(s.iter()).cloned().collect()
            }
        };
        let mut cand_rows: Vec<Vec<ScalarValue>> = vec![];
        let mut cand_of: Vec<usize> = vec![];
        let mut svals: Vec<Vec<ScalarValue>> = Vec::with_capacity(rows.len());
        for i in 0..rows.len() {
            let s = rows.row_values(i)?;
            if let Some(k) = &keys[i] {
                if let Some(ts) = index.get(k) {
                    for t in ts {
                        cand_rows.push(join(&s, t));
                        cand_of.push(i);
                    }
                }
            }
            svals.push(s);
        }
        let cand_batch = batch_from_rows(&self.out_schema, &cand_rows)?;
        let keep: Vec<bool> = match &self.filter {
            Some(f) if !cand_rows.is_empty() => {
                let m = eval_bool(f, &cand_batch)?;
                (0..m.len()).map(|i| m.is_valid(i) && m.value(i)).collect()
            }
            _ => vec![true; cand_rows.len()],
        };
        let mut per: Vec<Vec<Vec<ScalarValue>>> = vec![vec![]; rows.len()];
        for (n, row) in cand_rows.into_iter().enumerate() {
            if keep[n] {
                per[cand_of[n]].push(row);
            }
        }
        let mut out = vec![];
        let mut et = vec![];
        let mut ids = vec![];
        for (i, matches) in per.into_iter().enumerate() {
            if matches.is_empty() {
                if self.left_join {
                    out.push(join(&svals[i], &nulls));
                    et.push(rows.et[i]);
                    ids.push(rows.ids[i].clone());
                }
                continue;
            }
            for m in matches {
                out.push(m);
                et.push(rows.et[i]);
                ids.push(rows.ids[i].clone());
            }
        }
        Ok(Rows {
            batch: batch_from_rows(&self.out_schema, &out)?,
            et,
            ids,
        })
    }
}

/// Unique, in-order set of byte keys (used to track changed groups).
#[derive(Default)]
pub struct KeySet {
    order: Vec<Vec<u8>>,
    seen: HashSet<Vec<u8>>,
}

impl KeySet {
    pub fn push(&mut self, k: Vec<u8>) {
        if self.seen.insert(k.clone()) {
            self.order.push(k);
        }
    }
    pub fn into_vec(self) -> Vec<Vec<u8>> {
        self.order
    }
}
