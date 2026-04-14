use crate::error::ExqlError;
use crate::types::Row;

/// Query all rows from a SQL Server table and return them as `Vec<Row>`.
///
/// **Note:** sqlx 0.8 dropped MSSQL support (it was previously experimental).
/// This stub returns an error at runtime. When MSSQL support is restored in
/// sqlx or an alternative driver is integrated, this implementation should be
/// replaced with a real query function following the same pattern as
/// `postgres::query_postgres`.
pub async fn query_mssql(url: &str, table: &str) -> Result<Vec<Row>, ExqlError> {
    Err(ExqlError::Execution(format!(
        "MSSQL driver is not yet supported (sqlx 0.8 dropped experimental MSSQL). \
         Connection url={url}, table={table}"
    )))
}
