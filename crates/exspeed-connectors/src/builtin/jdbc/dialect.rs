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
            Err(ConnectorError::Config(format!(
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
    fn create_table_typed_sql(
        &self,
        table: &str,
        cols: &[ColumnSpec],
        pk_cols: &[&str],
    ) -> String;
    fn insert_sql(&self, table: &str, cols: &[&str]) -> String;
    fn upsert_sql(&self, table: &str, cols: &[&str], keys: &[&str]) -> String;
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
        assert_eq!(DialectKind::from_url("sqlite:///tmp/x.db").unwrap(), DialectKind::Sqlite);
        assert_eq!(DialectKind::from_url("sqlite::memory:").unwrap(), DialectKind::Sqlite);
        assert_eq!(DialectKind::from_url("SQLITE:data.db").unwrap(), DialectKind::Sqlite);
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
    fn json_type_parse() {
        assert_eq!(JsonType::parse("text"), Some(JsonType::Text));
        assert_eq!(JsonType::parse("bigint"), Some(JsonType::Bigint));
        assert_eq!(JsonType::parse("jsonb"), Some(JsonType::Jsonb));
        assert!(JsonType::parse("smallint").is_none());
        assert!(JsonType::parse("").is_none());
    }
}
