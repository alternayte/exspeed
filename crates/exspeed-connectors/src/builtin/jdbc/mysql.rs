use super::dialect::{ColumnSpec, Dialect};

pub struct MysqlDialect;

impl Dialect for MysqlDialect {
    fn quote_ident(&self, name: &str) -> String {
        format!("`{}`", name)
    }
    fn placeholder(&self, _n: usize) -> String {
        "?".to_string()
    }
    fn json_blob_type(&self) -> &'static str { "JSON" }
    fn timestamptz_type(&self) -> &'static str { "DATETIME(6)" }
    fn double_type(&self) -> &'static str { "DOUBLE" }
    fn create_table_blob_sql(&self, _table: &str) -> String { unimplemented!("task 9") }
    fn create_table_typed_sql(&self, _table: &str, _cols: &[ColumnSpec], _pk: &[&str]) -> String { unimplemented!("task 9") }
    fn insert_sql(&self, _table: &str, _cols: &[&str]) -> String { unimplemented!("task 9") }
    fn upsert_sql(&self, _table: &str, _cols: &[&str], _keys: &[&str]) -> String { unimplemented!("task 9") }
}
