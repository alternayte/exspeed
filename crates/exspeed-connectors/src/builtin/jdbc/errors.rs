//! Per-dialect error taxonomy for the JDBC sink and `jdbc_poll`.
//!
//! `code` is what [`super::backend::BackendError::Sql`] carries: SQLSTATE
//! for Postgres, the vendor error number for MySQL and SQL Server, the
//! extended result code for SQLite.

use super::backend::BackendError;
use super::dialect::DialectKind;
use crate::traits::{ConnectorError, PoisonReason};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlClass {
    /// Unique/primary-key violation.
    Duplicate,
    /// This row can never be written (constraint, bad data).
    Poison,
    /// Retry may succeed (deadlock, lock timeout, overload).
    Transient,
    /// The connection is gone; reconnect.
    Connection,
    /// Configuration/schema/permission problem.
    Fatal,
}

pub fn classify_code(kind: DialectKind, code: &str) -> SqlClass {
    use SqlClass::*;
    match kind {
        DialectKind::Postgres => match code {
            "23505" => Duplicate,
            c if c.starts_with("23") || c.starts_with("22") => Poison,
            c if c.starts_with("08") => Connection,
            "57P01" | "57P02" | "57P03" => Connection,
            c if c.starts_with("28") || c.starts_with("42") || c == "3D000" || c == "0A000" => {
                Fatal
            }
            "" => Transient,
            _ => Transient,
        },
        DialectKind::MySql => match code {
            "1062" | "1586" => Duplicate,
            // not null, out of range, bad value, truncation, FK, check,
            // data too long, invalid JSON, division by zero
            "1048" | "1264" | "1366" | "1292" | "1265" | "1406" | "1451" | "1452" | "3819"
            | "3140" | "1365" | "1411" | "1690" => Poison,
            // server gone / lost connection / too many connections / shutdown
            "2002" | "2003" | "2006" | "2013" | "1040" | "1053" | "1927" => Connection,
            // deadlock, lock wait timeout, query interrupted, out of memory
            "1205" | "1213" | "1317" | "1037" | "1038" | "1041" | "3024" => Transient,
            // access denied, unknown db/table/column, syntax, privileges,
            // read-only
            "1044" | "1045" | "1049" | "1146" | "1054" | "1064" | "1142" | "1143" | "1227"
            | "1290" | "1109" => Fatal,
            _ => Transient,
        },
        DialectKind::Sqlite => match code {
            // SQLITE_CONSTRAINT_PRIMARYKEY, _UNIQUE
            "1555" | "2067" => Duplicate,
            // SQLITE_CONSTRAINT (+ NOTNULL, CHECK, FOREIGNKEY, DATATYPE), MISMATCH, TOOBIG
            "19" | "1299" | "275" | "787" | "3091" | "20" | "18" => Poison,
            // BUSY, LOCKED and variants, FULL, IOERR
            "5" | "6" | "261" | "517" | "262" | "13" | "10" => Transient,
            // ERROR (no such table/column, syntax), READONLY, CANTOPEN, PERM, AUTH
            "1" | "8" | "14" | "3" | "23" => Fatal,
            _ => Transient,
        },
        DialectKind::Mssql => match code {
            "2627" | "2601" => Duplicate,
            // NULL into NOT NULL, constraint (FK/check), truncation,
            // conversion failures, arithmetic overflow, out-of-range dates
            "515" | "547" | "8152" | "2628" | "245" | "241" | "242" | "8114" | "8115" | "220"
            | "232" | "13609" => Poison,
            // deadlock victim, lock timeout, Azure throttling/failover
            "1205" | "1222" | "40501" | "40613" | "40197" | "49918" | "49919" | "49920"
            | "4221" => Transient,
            "10053" | "10054" | "10060" | "233" | "-2" => Connection,
            // invalid object/column, login failed, permission denied,
            // cannot open database, syntax
            "208" | "207" | "18456" | "229" | "230" | "262" | "4060" | "102" | "156" => Fatal,
            _ => Transient,
        },
    }
}

pub fn classify(kind: DialectKind, e: &BackendError) -> SqlClass {
    match e {
        BackendError::Connection(_) => SqlClass::Connection,
        BackendError::Encode(_) => SqlClass::Poison,
        BackendError::Config(_) => SqlClass::Fatal,
        BackendError::Other(_) => SqlClass::Transient,
        BackendError::Sql { code, .. } => classify_code(kind, code),
    }
}

/// Convert to the connector taxonomy (duplicates count as poison here;
/// the sink decides when a duplicate is harmless).
pub fn to_connector_error(kind: DialectKind, e: &BackendError, what: &str) -> ConnectorError {
    let msg = format!("{what}: {e}");
    match classify(kind, e) {
        SqlClass::Duplicate | SqlClass::Poison => {
            ConnectorError::Poison(PoisonReason::SinkRejected { detail: msg })
        }
        SqlClass::Transient => ConnectorError::transient(msg),
        SqlClass::Connection => ConnectorError::connection(msg),
        SqlClass::Fatal => ConnectorError::fatal(msg),
    }
}

/// Map a sqlx error. MySQL errors carry the vendor number as the code.
pub fn from_sqlx(e: sqlx::Error) -> BackendError {
    match &e {
        sqlx::Error::Database(db) => {
            let code = db
                .try_downcast_ref::<sqlx::mysql::MySqlDatabaseError>()
                .map(|m| m.number().to_string())
                .or_else(|| db.code().map(|c| c.to_string()))
                .unwrap_or_default();
            BackendError::Sql {
                code,
                message: db.message().to_string(),
            }
        }
        sqlx::Error::Io(_)
        | sqlx::Error::Tls(_)
        | sqlx::Error::Protocol(_)
        | sqlx::Error::PoolTimedOut
        | sqlx::Error::PoolClosed
        | sqlx::Error::WorkerCrashed => BackendError::Connection(e.to_string()),
        sqlx::Error::Configuration(_) => BackendError::Config(e.to_string()),
        sqlx::Error::Encode(_) => BackendError::Encode(e.to_string()),
        _ => BackendError::Other(e.to_string()),
    }
}

/// Map a tiberius error. Server errors carry the error number.
pub fn from_tiberius(e: tiberius::error::Error) -> BackendError {
    use tiberius::error::Error as E;
    match &e {
        E::Server(t) => BackendError::Sql {
            code: t.code().to_string(),
            message: t.message().to_string(),
        },
        E::Io { .. } | E::Tls(_) | E::Routing { .. } => BackendError::Connection(e.to_string()),
        E::Conversion(_) => BackendError::Encode(e.to_string()),
        _ => BackendError::Other(e.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use SqlClass::*;

    #[test]
    fn postgres() {
        let k = DialectKind::Postgres;
        assert_eq!(classify_code(k, "23505"), Duplicate);
        assert_eq!(classify_code(k, "23502"), Poison);
        assert_eq!(classify_code(k, "23503"), Poison);
        assert_eq!(classify_code(k, "22P02"), Poison);
        assert_eq!(classify_code(k, "22003"), Poison);
        assert_eq!(classify_code(k, "40P01"), Transient);
        assert_eq!(classify_code(k, "40001"), Transient);
        assert_eq!(classify_code(k, "08006"), Connection);
        assert_eq!(classify_code(k, "42P01"), Fatal);
        assert_eq!(classify_code(k, "28P01"), Fatal);
    }

    #[test]
    fn mysql_vendor_codes() {
        let k = DialectKind::MySql;
        assert_eq!(classify_code(k, "1062"), Duplicate);
        assert_eq!(classify_code(k, "1048"), Poison);
        assert_eq!(classify_code(k, "1452"), Poison);
        assert_eq!(classify_code(k, "1213"), Transient);
        assert_eq!(classify_code(k, "2013"), Connection);
        assert_eq!(classify_code(k, "1146"), Fatal);
        assert_eq!(classify_code(k, "1045"), Fatal);
    }

    #[test]
    fn sqlite_extended_codes() {
        let k = DialectKind::Sqlite;
        assert_eq!(classify_code(k, "1555"), Duplicate);
        assert_eq!(classify_code(k, "2067"), Duplicate);
        assert_eq!(classify_code(k, "1299"), Poison);
        assert_eq!(classify_code(k, "5"), Transient);
        assert_eq!(classify_code(k, "1"), Fatal);
    }

    #[test]
    fn mssql_numbers() {
        let k = DialectKind::Mssql;
        assert_eq!(classify_code(k, "2627"), Duplicate);
        assert_eq!(classify_code(k, "2601"), Duplicate);
        assert_eq!(classify_code(k, "515"), Poison);
        assert_eq!(classify_code(k, "547"), Poison);
        assert_eq!(classify_code(k, "1205"), Transient);
        assert_eq!(classify_code(k, "208"), Fatal);
        assert_eq!(classify_code(k, "18456"), Fatal);
    }

    #[test]
    fn backend_variants() {
        let k = DialectKind::Postgres;
        assert_eq!(
            classify(k, &BackendError::Connection("x".into())),
            Connection
        );
        assert_eq!(classify(k, &BackendError::Encode("x".into())), Poison);
        assert_eq!(classify(k, &BackendError::Config("x".into())), Fatal);
    }
}
