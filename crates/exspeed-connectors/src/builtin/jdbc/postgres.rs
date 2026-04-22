use super::dialect::{ColumnSpec, Dialect};

pub struct PostgresDialect;

impl Dialect for PostgresDialect {
    fn quote_ident(&self, name: &str) -> String {
        format!("\"{}\"", name)
    }
    fn placeholder(&self, n: usize) -> String {
        format!("${}", n)
    }
    fn json_blob_type(&self) -> &'static str { "JSONB" }
    fn timestamptz_type(&self) -> &'static str { "TIMESTAMPTZ" }
    fn double_type(&self) -> &'static str { "DOUBLE PRECISION" }
    fn create_table_blob_sql(&self, _table: &str) -> String { unimplemented!("task 8") }
    fn create_table_typed_sql(&self, _table: &str, _cols: &[ColumnSpec], _pk: &[&str]) -> String { unimplemented!("task 8") }
    fn insert_sql(&self, _table: &str, _cols: &[&str]) -> String { unimplemented!("task 8") }
    fn upsert_sql(&self, _table: &str, _cols: &[&str], _keys: &[&str]) -> String { unimplemented!("task 8") }
}
