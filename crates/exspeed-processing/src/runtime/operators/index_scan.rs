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

        // Fetch each matching record from indexed (sealed) segments
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

        // The active segment has no .sidx file — scan it with predicate
        // filtering. Determine the active segment's start offset by finding
        // the highest base_offset among sealed segments (from .seg filenames).
        let active_start = {
            let mut max_sealed_offset: Option<u64> = None;
            if let Ok(entries) = std::fs::read_dir(&self.partition_dir) {
                for entry in entries.flatten() {
                    let name = entry.file_name();
                    let name_str = name.to_str().unwrap_or("");
                    if name_str.ends_with(".seg") {
                        if let Ok(base) = name_str.trim_end_matches(".seg").parse::<u64>() {
                            max_sealed_offset = Some(max_sealed_offset.map_or(base, |prev: u64| prev.max(base)));
                        }
                    }
                }
            }
            // The active segment is the one with the highest base_offset.
            // Records in it start at that base_offset. We already got indexed
            // results from sealed segments, so scan from the active's base.
            max_sealed_offset.unwrap_or(0)
        };

        let batch_size = 1024usize;
        let mut cursor = Offset(active_start);
        loop {
            let batch = tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current()
                    .block_on(self.storage.read(&self.stream, cursor, batch_size))
            });

            let records = match batch {
                Ok(r) => r,
                Err(_) => break,
            };

            if records.is_empty() {
                break;
            }

            cursor = Offset(records.last().unwrap().offset.0 + 1);

            for record in &records {
                let row = stored_record_to_row(record, self.alias.as_deref(), &self.required);
                if let Some(ref pred) = self.predicate {
                    if eval_expr(pred, &row) != Value::Bool(true) {
                        continue;
                    }
                }
                self.rows.push(row);
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
