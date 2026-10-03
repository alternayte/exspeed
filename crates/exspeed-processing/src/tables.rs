//! Materialized tables (`CREATE TABLE … AS SELECT … GROUP BY …`).
//!
//! A table keeps its current rows in memory, keyed by the query's group key.
//! The query that owns it writes every change to the table's changelog
//! stream (a stream with the table's name) and snapshots the rows in its
//! checkpoints, so the table survives restarts (see `continuous`).

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use datafusion::arrow::array::{new_empty_array, ArrayRef, RecordBatch};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{Result as DFResult, ScalarValue};
use datafusion::datasource::MemTable;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::ExecutionPlan;
use serde_json::Value as Json;

use crate::convert::scalar_to_json;
use crate::error::ExqlError;

/// Render a group key as the string used for record keys and
/// `GET /api/v1/views/{name}?key=`: empty for a global aggregate, the plain
/// value for a single column, a JSON array for composite keys.
pub fn key_string(key: &[ScalarValue]) -> String {
    match key {
        [] => String::new(),
        [one] => match scalar_to_json(one) {
            Json::Null => "null".to_string(),
            Json::String(s) => s,
            other => other.to_string(),
        },
        many => Json::Array(many.iter().map(scalar_to_json).collect()).to_string(),
    }
}

#[derive(Default)]
struct Inner {
    /// key string → (group key, output row)
    rows: BTreeMap<String, (Vec<ScalarValue>, Vec<ScalarValue>)>,
    version: u64,
}

/// One materialized table.
pub struct MaterializedTable {
    pub name: String,
    pub query_id: String,
    schema: SchemaRef,
    inner: RwLock<Inner>,
}

impl fmt::Debug for MaterializedTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MaterializedTable")
            .field("name", &self.name)
            .field("query_id", &self.query_id)
            .finish()
    }
}

impl MaterializedTable {
    pub fn new(name: &str, query_id: &str, schema: SchemaRef) -> Self {
        Self {
            name: name.to_string(),
            query_id: query_id.to_string(),
            schema,
            inner: RwLock::new(Inner::default()),
        }
    }

    pub fn schema_ref(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub fn columns(&self) -> Vec<String> {
        self.schema.fields().iter().map(|f| f.name().clone()).collect()
    }

    pub fn len(&self) -> usize {
        self.inner.read().unwrap().rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Monotonic counter bumped on every change (used to refresh lookup
    /// indexes of stream-table joins).
    pub fn version(&self) -> u64 {
        self.inner.read().unwrap().version
    }

    pub fn upsert(&self, key: Vec<ScalarValue>, row: Vec<ScalarValue>) {
        let mut g = self.inner.write().unwrap();
        g.rows.insert(key_string(&key), (key, row));
        g.version += 1;
    }

    pub fn delete(&self, key: &[ScalarValue]) {
        let mut g = self.inner.write().unwrap();
        if g.rows.remove(&key_string(key)).is_some() {
            g.version += 1;
        }
    }

    pub fn clear(&self) {
        let mut g = self.inner.write().unwrap();
        g.rows.clear();
        g.version += 1;
    }

    /// Replace all rows (restore from a checkpoint).
    pub fn replace(&self, rows: Vec<(Vec<ScalarValue>, Vec<ScalarValue>)>) {
        let mut g = self.inner.write().unwrap();
        g.rows = rows
            .into_iter()
            .map(|(k, r)| (key_string(&k), (k, r)))
            .collect();
        g.version += 1;
    }

    /// All `(group key, row)` pairs, ordered by key string.
    pub fn entries(&self) -> Vec<(Vec<ScalarValue>, Vec<ScalarValue>)> {
        self.inner.read().unwrap().rows.values().cloned().collect()
    }

    /// The row for a key string.
    pub fn get(&self, key: &str) -> Option<Vec<ScalarValue>> {
        self.inner
            .read()
            .unwrap()
            .rows
            .get(key)
            .map(|(_, r)| r.clone())
    }

    /// Snapshot as one batch (plus the version it reflects).
    pub fn snapshot(&self) -> Result<(RecordBatch, u64), ExqlError> {
        let g = self.inner.read().unwrap();
        let rows: Vec<&Vec<ScalarValue>> = g.rows.values().map(|(_, r)| r).collect();
        let batch = rows_to_batch(&self.schema, &rows)?;
        Ok((batch, g.version))
    }
}

/// Build a batch from rows of scalars.
pub fn rows_to_batch(schema: &SchemaRef, rows: &[&Vec<ScalarValue>]) -> Result<RecordBatch, ExqlError> {
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    for (i, f) in schema.fields().iter().enumerate() {
        if rows.is_empty() {
            cols.push(new_empty_array(f.data_type()));
            continue;
        }
        let arr = ScalarValue::iter_to_array(rows.iter().map(|r| r[i].clone()))?;
        let arr = if arr.data_type() != f.data_type() {
            datafusion::arrow::compute::cast(&arr, f.data_type())?
        } else {
            arr
        };
        cols.push(arr);
    }
    Ok(RecordBatch::try_new_with_options(
        schema.clone(),
        cols,
        &datafusion::arrow::array::RecordBatchOptions::new().with_row_count(Some(rows.len())),
    )?)
}

#[async_trait]
impl TableProvider for MaterializedTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        session: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let (batch, _) = self.snapshot().map_err(|e| e.into_df())?;
        let mem = MemTable::try_new(self.schema.clone(), vec![vec![batch]])?;
        mem.scan(session, projection, filters, limit).await
    }
}

/// All materialized tables, by name.
#[derive(Default)]
pub struct TableRegistry {
    tables: RwLock<HashMap<String, Arc<MaterializedTable>>>,
}

impl TableRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, name: &str) -> Option<Arc<MaterializedTable>> {
        self.tables.read().unwrap().get(name).cloned()
    }

    pub fn insert(&self, table: Arc<MaterializedTable>) {
        self.tables
            .write()
            .unwrap()
            .insert(table.name.clone(), table);
    }

    pub fn remove(&self, name: &str) -> Option<Arc<MaterializedTable>> {
        self.tables.write().unwrap().remove(name)
    }

    pub fn list(&self) -> Vec<Arc<MaterializedTable>> {
        let mut v: Vec<_> = self.tables.read().unwrap().values().cloned().collect();
        v.sort_by(|a, b| a.name.cmp(&b.name));
        v
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_strings() {
        assert_eq!(key_string(&[]), "");
        assert_eq!(key_string(&[ScalarValue::Utf8(Some("eu".into()))]), "eu");
        assert_eq!(key_string(&[ScalarValue::Int64(Some(3))]), "3");
        assert_eq!(
            key_string(&[ScalarValue::Utf8(Some("eu".into())), ScalarValue::Int64(Some(3))]),
            r#"["eu",3]"#
        );
    }
}
