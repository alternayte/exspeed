//! Row batches flowing through a continuous query, and stateless steps.

use std::sync::Arc;

use datafusion::arrow::array::{
    new_null_array, Array, ArrayRef, AsArray, BooleanArray, RecordBatch, RecordBatchOptions,
    UInt32Array,
};
use datafusion::arrow::compute::{cast, concat_batches, filter_record_batch, prep_null_mask_filter, take};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::ScalarValue;
use datafusion::physical_expr::PhysicalExpr;

use crate::error::ExqlError;

/// Where an output row came from. Used to build deterministic idempotency
/// keys for output records.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RowId {
    /// Record `off` of source `src`.
    Src { src: u8, off: u64 },
    /// A stream-stream join match of left offset `l` and right offset `r`.
    Pair { l: u64, r: u64 },
    /// A left row of a LEFT join that found no match before the watermark.
    Unmatched { l: u64 },
    /// Aggregate group number `n` of the current emit.
    Group(u32),
}

impl RowId {
    pub fn tag(&self) -> String {
        match self {
            RowId::Src { src, off } => format!("s{src}.{off}"),
            RowId::Pair { l, r } => format!("j{l}.{r}"),
            RowId::Unmatched { l } => format!("u{l}"),
            RowId::Group(n) => format!("g{n}"),
        }
    }
}

/// A batch of rows with their event times (ms) and provenance.
#[derive(Debug, Clone)]
pub struct Rows {
    pub batch: RecordBatch,
    pub et: Vec<i64>,
    pub ids: Vec<RowId>,
}

impl Rows {
    pub fn empty(schema: SchemaRef) -> Self {
        Rows {
            batch: RecordBatch::new_empty(schema),
            et: vec![],
            ids: vec![],
        }
    }

    pub fn len(&self) -> usize {
        self.et.len()
    }

    pub fn is_empty(&self) -> bool {
        self.et.is_empty()
    }

    pub fn schema(&self) -> SchemaRef {
        self.batch.schema()
    }

    /// Keep the rows where `mask` is true (NULL counts as false).
    pub fn filter(self, mask: &BooleanArray) -> Result<Rows, ExqlError> {
        let mask = if mask.null_count() > 0 {
            prep_null_mask_filter(mask)
        } else {
            mask.clone()
        };
        let batch = filter_record_batch(&self.batch, &mask)?;
        let mut et = Vec::with_capacity(batch.num_rows());
        let mut ids = Vec::with_capacity(batch.num_rows());
        for (i, (t, id)) in self.et.into_iter().zip(self.ids).enumerate() {
            if mask.value(i) {
                et.push(t);
                ids.push(id);
            }
        }
        Ok(Rows { batch, et, ids })
    }

    /// Rows at `idx`, in that order.
    pub fn take(&self, idx: &[u32]) -> Result<Rows, ExqlError> {
        let indices = UInt32Array::from(idx.to_vec());
        let cols = self
            .batch
            .columns()
            .iter()
            .map(|c| take(c.as_ref(), &indices, None))
            .collect::<Result<Vec<_>, _>>()?;
        let batch = RecordBatch::try_new_with_options(
            self.batch.schema(),
            cols,
            &RecordBatchOptions::new().with_row_count(Some(idx.len())),
        )?;
        Ok(Rows {
            batch,
            et: idx.iter().map(|&i| self.et[i as usize]).collect(),
            ids: idx.iter().map(|&i| self.ids[i as usize].clone()).collect(),
        })
    }

    /// Concatenate row sets with the same schema.
    pub fn concat(schema: SchemaRef, parts: Vec<Rows>) -> Result<Rows, ExqlError> {
        let parts: Vec<Rows> = parts.into_iter().filter(|p| !p.is_empty()).collect();
        match parts.len() {
            0 => return Ok(Rows::empty(schema)),
            1 => return Ok(parts.into_iter().next().unwrap()),
            _ => {}
        }
        let batches: Vec<&RecordBatch> = parts.iter().map(|p| &p.batch).collect();
        let batch = concat_batches(&schema, batches)?;
        let mut et = Vec::with_capacity(batch.num_rows());
        let mut ids = Vec::with_capacity(batch.num_rows());
        for p in parts {
            et.extend(p.et);
            ids.extend(p.ids);
        }
        Ok(Rows { batch, et, ids })
    }

    /// Row `i` as scalars.
    pub fn row_values(&self, i: usize) -> Result<Vec<ScalarValue>, ExqlError> {
        self.batch
            .columns()
            .iter()
            .map(|c| ScalarValue::try_from_array(c, i).map_err(ExqlError::from))
            .collect()
    }
}

/// Evaluate `expr` over `batch` to an array.
pub fn eval(expr: &Arc<dyn PhysicalExpr>, batch: &RecordBatch) -> Result<ArrayRef, ExqlError> {
    Ok(expr.evaluate(batch)?.into_array(batch.num_rows())?)
}

/// Evaluate a predicate to a boolean array.
pub fn eval_bool(expr: &Arc<dyn PhysicalExpr>, batch: &RecordBatch) -> Result<BooleanArray, ExqlError> {
    let arr = eval(expr, batch)?;
    match arr.as_boolean_opt() {
        Some(b) => Ok(b.clone()),
        None => Err(ExqlError::Plan(format!(
            "predicate {expr} is {} rather than boolean",
            arr.data_type()
        ))),
    }
}

/// Build a batch from rows of scalars, casting to the schema's types.
pub fn batch_from_rows(schema: &SchemaRef, rows: &[Vec<ScalarValue>]) -> Result<RecordBatch, ExqlError> {
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    for (i, f) in schema.fields().iter().enumerate() {
        let arr = if rows.is_empty() {
            new_null_array(f.data_type(), 0)
        } else {
            ScalarValue::iter_to_array(rows.iter().map(|r| r[i].clone()))?
        };
        let arr = if arr.data_type() != f.data_type() {
            cast(&arr, f.data_type())?
        } else {
            arr
        };
        cols.push(arr);
    }
    Ok(RecordBatch::try_new_with_options(
        schema.clone(),
        cols,
        &RecordBatchOptions::new().with_row_count(Some(rows.len())),
    )?)
}

/// Re-label a batch with `schema`, casting columns whose types differ.
pub fn conform(batch: RecordBatch, schema: &SchemaRef) -> Result<RecordBatch, ExqlError> {
    let n = batch.num_rows();
    make_batch(schema, batch.columns().to_vec(), n)
}

/// Build a batch with `schema` from `cols`, casting columns whose types
/// differ.
pub fn make_batch(schema: &SchemaRef, cols: Vec<ArrayRef>, n: usize) -> Result<RecordBatch, ExqlError> {
    if cols.len() != schema.fields().len() {
        return Err(ExqlError::Internal(format!(
            "batch has {} columns, expected {}",
            cols.len(),
            schema.fields().len()
        )));
    }
    let cols = cols
        .into_iter()
        .zip(schema.fields().iter())
        .map(|(c, f)| {
            if c.data_type() == f.data_type() {
                Ok(c)
            } else {
                cast(&c, f.data_type())
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(RecordBatch::try_new_with_options(
        schema.clone(),
        cols,
        &RecordBatchOptions::new().with_row_count(Some(n)),
    )?)
}

/// A stateless step.
#[derive(Debug, Clone)]
pub enum Step {
    Filter(Arc<dyn PhysicalExpr>),
    Project(Vec<Arc<dyn PhysicalExpr>>, SchemaRef),
    /// Keep these columns (a scan projection).
    Columns(Vec<usize>, SchemaRef),
}

/// A sequence of stateless steps applied in order.
#[derive(Debug, Clone, Default)]
pub struct Chain {
    pub steps: Vec<Step>,
}

impl Chain {
    pub fn apply(&self, mut rows: Rows) -> Result<Rows, ExqlError> {
        for step in &self.steps {
            match step {
                Step::Filter(pred) => {
                    if rows.is_empty() {
                        continue;
                    }
                    let mask = eval_bool(pred, &rows.batch)?;
                    rows = rows.filter(&mask)?;
                }
                Step::Project(exprs, schema) => {
                    let n = rows.len();
                    let cols = exprs
                        .iter()
                        .map(|e| eval(e, &rows.batch))
                        .collect::<Result<Vec<_>, _>>()?;
                    rows.batch = make_batch(schema, cols, n)?;
                }
                Step::Columns(idx, schema) => {
                    let cols = idx.iter().map(|&i| rows.batch.column(i).clone()).collect();
                    rows.batch = RecordBatch::try_new_with_options(
                        schema.clone(),
                        cols,
                        &RecordBatchOptions::new().with_row_count(Some(rows.len())),
                    )?;
                }
            }
        }
        Ok(rows)
    }

    /// Text of every expression, for validation messages.
    pub fn expr_strings(&self) -> Vec<String> {
        let mut out = vec![];
        for s in &self.steps {
            match s {
                Step::Filter(e) => out.push(e.to_string()),
                Step::Project(es, _) => out.extend(es.iter().map(|e| e.to_string())),
                Step::Columns(..) => {}
            }
        }
        out
    }
}
