use std::path::PathBuf;
use std::sync::Arc;

use exspeed_common::{Offset, StreamName};
use exspeed_storage::file::secondary_index::SecondaryIndex;
use exspeed_streams::StorageEngine;

use crate::parser::ast::Expr;
use crate::planner::column_set::ColumnSet;
use crate::runtime::eval::eval_expr;
use crate::runtime::operators::Operator;
use crate::runtime::row_builder::stored_record_to_row;
use crate::types::{Row, Value};

pub struct IndexScanOperator {
    rows: Vec<Row>,
    position: usize,
    computed: bool,
    storage: Arc<dyn StorageEngine>,
    stream: StreamName,
    alias: Option<String>,
    required: ColumnSet,
    partition_dir: PathBuf,
    index_name: String,
    lookup_value: String,
    predicate: Option<Expr>,
}

impl IndexScanOperator {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        storage: Arc<dyn StorageEngine>,
        stream: StreamName,
        alias: Option<String>,
        required: ColumnSet,
        partition_dir: PathBuf,
        index_name: String,
        lookup_value: String,
        predicate: Option<Expr>,
    ) -> Self {
        Self {
            rows: Vec::new(),
            position: 0,
            computed: false,
            storage,
            stream,
            alias,
            required,
            partition_dir,
            index_name,
            lookup_value,
            predicate,
        }
    }

    fn compute(&mut self) {
        // Find all .sidx.{index_name} files in the partition directory
        let mut all_offsets: Vec<u64> = Vec::new();
        let suffix = format!(".sidx.{}", self.index_name);

        if let Ok(entries) = std::fs::read_dir(&self.partition_dir) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let name_str = name.to_str().unwrap_or("");
                if name_str.ends_with(&suffix) {
                    if let Ok(idx) = SecondaryIndex::load(&entry.path()) {
                        let offsets = idx.lookup(&self.lookup_value);
                        all_offsets.extend(offsets);
                    }
                }
            }
        }

        // Sort offsets for sequential access
        all_offsets.sort();
        all_offsets.dedup();

        // Fetch each matching record by offset
        for offset in &all_offsets {
            let batch = tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current()
                    .block_on(self.storage.read(&self.stream, Offset(*offset), 1))
            });

            if let Ok(records) = batch {
                for record in &records {
                    if record.offset.0 == *offset {
                        let row =
                            stored_record_to_row(record, self.alias.as_deref(), &self.required);
                        if let Some(ref pred) = self.predicate {
                            if eval_expr(pred, &row) != Value::Bool(true) {
                                continue;
                            }
                        }
                        self.rows.push(row);
                    }
                }
            }
        }

        self.computed = true;
    }
}

impl Operator for IndexScanOperator {
    fn next(&mut self) -> Option<Row> {
        if !self.computed {
            self.compute();
        }
        if self.position < self.rows.len() {
            let row = self.rows[self.position].clone();
            self.position += 1;
            Some(row)
        } else {
            None
        }
    }

    fn columns(&self) -> Vec<String> {
        use exspeed_streams::StoredRecord;
        let dummy = StoredRecord {
            offset: Offset(0),
            timestamp: 0,
            key: None,
            subject: String::new(),
            value: bytes::Bytes::from_static(b"{}"),
            headers: vec![],
        };
        stored_record_to_row(&dummy, self.alias.as_deref(), &self.required).columns
    }
}
