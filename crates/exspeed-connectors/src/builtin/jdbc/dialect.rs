//! Dialect abstraction for the JDBC-style SQL sink.
//!
//! The trait encapsulates the dialect-specific bits (placeholder syntax,
//! identifier quoting, UPSERT grammar, JSON column type). A new dialect is a
//! new file that implements `Dialect`. `DialectKind::from_url` inspects the
//! connection URL's scheme so users don't have to configure it twice.

use crate::traits::ConnectorError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DialectKind {
    Postgres,
    MySql,
    Mssql,
    Sqlite,
}

impl DialectKind {
    /// Infer the dialect from a connection URL's scheme.
    /// `postgres://` and `postgresql://` → Postgres; `mysql://` → MySql;
    /// `mssql://` and `sqlserver://` → Mssql; `sqlite:` → Sqlite.
    pub fn from_url(url: &str) -> Result<Self, ConnectorError> {
        let lower = url.trim_start().to_ascii_lowercase();
        if lower.starts_with("postgres://") || lower.starts_with("postgresql://") {
            Ok(Self::Postgres)
        } else if lower.starts_with("mysql://") {
            Ok(Self::MySql)
        } else if lower.starts_with("mssql://") || lower.starts_with("sqlserver://") {
            Ok(Self::Mssql)
        } else if lower.starts_with("sqlite:") {
            Ok(Self::Sqlite)
        } else {
            Err(ConnectorError::config(format!(
                "jdbc sink: unsupported connection URL scheme; expected postgres://, mysql://, mssql://, or sqlite:, got: {}",
                url.split("://").next().unwrap_or(url)
            )))
        }
    }
}

pub fn dialect_for(kind: DialectKind) -> Box<dyn Dialect> {
    match kind {
        DialectKind::Postgres => Box::new(super::postgres::PostgresDialect),
        DialectKind::MySql => Box::new(super::mysql::MysqlDialect),
        DialectKind::Mssql => Box::new(super::mssql::MssqlDialect),
        DialectKind::Sqlite => Box::new(super::sqlite::SqliteDialect),
    }
}

#[derive(Debug, Clone)]
pub struct ColumnSpec {
    pub name: String,
    pub json_type: JsonType,
    pub nullable: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonType {
    Text,
    Bigint,
    Double,
    Boolean,
    Timestamptz,
    Jsonb,
}

impl JsonType {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "text" => Some(Self::Text),
            "bigint" => Some(Self::Bigint),
            "double" => Some(Self::Double),
            "boolean" => Some(Self::Boolean),
            "timestamptz" => Some(Self::Timestamptz),
            "jsonb" => Some(Self::Jsonb),
            _ => None,
        }
    }
}

/// Dialect surface. Implementors are stateless — one instance per connector.
pub trait Dialect: Send + Sync {
    fn quote_ident(&self, name: &str) -> String;
    fn placeholder(&self, n: usize) -> String;
    fn json_blob_type(&self) -> &'static str;
    fn timestamptz_type(&self) -> &'static str;
    fn double_type(&self) -> &'static str;
    fn create_table_blob_sql(&self, table: &str) -> String;
    fn create_table_typed_sql(&self, table: &str, cols: &[ColumnSpec], pk_cols: &[&str]) -> String;
    fn insert_sql(&self, table: &str, cols: &[&str]) -> String;
    fn upsert_sql(&self, table: &str, cols: &[&str], keys: &[&str]) -> String;
    /// Add explicit casts to placeholders where the driver binds parameters
    /// with a type the database won't implicitly convert (Postgres types every
    /// sqlx `Any` text parameter as TEXT, which it refuses to put into JSONB,
    /// TIMESTAMPTZ, …). `types[i]` is the column type for placeholder `i+1`.
    fn cast_placeholders(&self, sql: String, _types: &[JsonType]) -> String {
        sql
    }

    /// Most bind parameters one statement may carry.
    fn max_params(&self) -> usize;

    /// `(p1, ..., pn), (pn+1, ..., p2n), ...` for `rows` rows of `ncols`.
    fn row_placeholders(&self, ncols: usize, rows: usize) -> String {
        (0..rows)
            .map(|r| {
                let ps: Vec<String> = (0..ncols)
                    .map(|c| self.placeholder(r * ncols + c + 1))
                    .collect();
                format!("({})", ps.join(", "))
            })
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// Multi-row form of [`Dialect::insert_sql`].
    fn insert_rows_sql(&self, table: &str, cols: &[&str], rows: usize) -> String {
        splice_rows(self, self.insert_sql(table, cols), cols.len(), rows)
    }

    /// Multi-row form of [`Dialect::upsert_sql`]. Callers must not repeat a
    /// key within one statement.
    fn upsert_rows_sql(&self, table: &str, cols: &[&str], keys: &[&str], rows: usize) -> String {
        splice_rows(self, self.upsert_sql(table, cols, keys), cols.len(), rows)
    }
}

/// Replace the single-row `VALUES` placeholder group with `rows` groups.
fn splice_rows<D: Dialect + ?Sized>(d: &D, sql: String, ncols: usize, rows: usize) -> String {
    if rows <= 1 {
        return sql;
    }
    let single = d.row_placeholders(ncols, 1);
    match sql.find(&single) {
        Some(pos) => format!(
            "{}{}{}",
            &sql[..pos],
            d.row_placeholders(ncols, rows),
            &sql[pos + single.len()..]
        ),
        None => sql,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kind_from_postgres_url() {
        assert_eq!(
            DialectKind::from_url("postgres://u:p@h/d").unwrap(),
            DialectKind::Postgres
        );
        assert_eq!(
            DialectKind::from_url("postgresql://u:p@h/d").unwrap(),
            DialectKind::Postgres
        );
        assert_eq!(
            DialectKind::from_url("POSTGRES://u:p@h/d").unwrap(),
            DialectKind::Postgres
        );
    }

    #[test]
    fn kind_from_mysql_url() {
        assert_eq!(
            DialectKind::from_url("mysql://u:p@h/d").unwrap(),
            DialectKind::MySql
        );
    }

    #[test]
    fn kind_rejects_unsupported() {
        assert!(DialectKind::from_url("not-a-url").is_err());
        assert!(DialectKind::from_url("oracle://u:p@h/d").is_err());
    }

    #[test]
    fn kind_from_sqlite_url() {
        assert_eq!(
            DialectKind::from_url("sqlite:///tmp/x.db").unwrap(),
            DialectKind::Sqlite
        );
        assert_eq!(
            DialectKind::from_url("sqlite::memory:").unwrap(),
            DialectKind::Sqlite
        );
        assert_eq!(
            DialectKind::from_url("SQLITE:data.db").unwrap(),
            DialectKind::Sqlite
        );
    }

    #[test]
    fn kind_from_mssql_url() {
        assert_eq!(
            DialectKind::from_url("mssql://u:p@h:1433/d").unwrap(),
            DialectKind::Mssql
        );
        assert_eq!(
            DialectKind::from_url("sqlserver://u:p@h:1433/d").unwrap(),
            DialectKind::Mssql
        );
        assert_eq!(
            DialectKind::from_url("MSSQL://u:p@h/d").unwrap(),
            DialectKind::Mssql
        );
    }

    #[test]
    fn multi_row_sql_per_dialect() {
        let pg = dialect_for(DialectKind::Postgres);
        let sql = pg.upsert_rows_sql("t", &["id", "v"], &["id"], 3);
        assert!(
            sql.contains("VALUES ($1, $2), ($3, $4), ($5, $6) ON CONFLICT"),
            "{sql}"
        );
        let my = dialect_for(DialectKind::MySql);
        let sql = my.insert_rows_sql("t", &["a"], 2);
        assert_eq!(sql, "INSERT INTO `t` (`a`) VALUES (?), (?)");
        let ms = dialect_for(DialectKind::Mssql);
        let sql = ms.upsert_rows_sql("t", &["id", "v"], &["id"], 2);
        assert!(
            sql.contains("USING (VALUES (@P1, @P2), (@P3, @P4)) AS s"),
            "{sql}"
        );
        let lite = dialect_for(DialectKind::Sqlite);
        let sql = lite.upsert_rows_sql("t", &["id", "v"], &["id"], 2);
        assert!(sql.contains("VALUES (?, ?), (?, ?) ON CONFLICT"), "{sql}");
    }

    #[test]
    fn json_type_parse() {
        assert_eq!(JsonType::parse("text"), Some(JsonType::Text));
        assert_eq!(JsonType::parse("bigint"), Some(JsonType::Bigint));
        assert_eq!(JsonType::parse("jsonb"), Some(JsonType::Jsonb));
        assert!(JsonType::parse("smallint").is_none());
        assert!(JsonType::parse("").is_none());
    }
}
