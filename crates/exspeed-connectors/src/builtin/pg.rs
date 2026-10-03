//! Postgres helpers shared by `postgres_cdc`, `postgres_poll` and
//! `postgres_outbox`: connections, error taxonomy, identifiers, slot and
//! publication management, and OID-based value typing.

use pgwire_replication::{Lsn, ReplicationConfig};
use tokio_postgres::{Client, NoTls};
use tracing::{debug, info, warn};

use crate::config::sanitized_name;
use crate::traits::ConnectorError;

/// Open a regular connection; the connection task ends (and later queries
/// fail with a connection error) when the socket drops.
pub async fn connect(conn_str: &str) -> Result<Client, ConnectorError> {
    let (client, connection) = tokio_postgres::connect(conn_str, NoTls)
        .await
        .map_err(|e| classify(&e, "connect"))?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            debug!(error = %e, "postgres connection closed");
        }
    });
    Ok(client)
}

/// Map a Postgres error into the connector taxonomy.
///
/// | SQLSTATE                          | Class |
/// |-----------------------------------|-------|
/// | no code (I/O, closed), 08xxx, 57P01–57P03 | connection |
/// | 28xxx (auth), 3D000 (no database), 42xxx (syntax/undefined/privilege), 0A000 | fatal |
/// | 22xxx, 23xxx (data/constraint)    | poison |
/// | everything else (40001, 40P01, 53xxx, 55xxx, 57014, …) | transient |
pub fn classify(e: &tokio_postgres::Error, what: &str) -> ConnectorError {
    let msg = format!("{what}: {e}");
    let Some(code) = e.code() else {
        return ConnectorError::connection(msg);
    };
    classify_sqlstate(code.code(), msg)
}

pub fn classify_sqlstate(code: &str, msg: String) -> ConnectorError {
    match code {
        c if c.starts_with("08") => ConnectorError::connection(msg),
        "57P01" | "57P02" | "57P03" => ConnectorError::connection(msg),
        c if c.starts_with("28") || c.starts_with("42") || c == "3D000" || c == "0A000" => {
            ConnectorError::fatal(msg)
        }
        c if c.starts_with("22") || c.starts_with("23") => {
            ConnectorError::Poison(crate::traits::PoisonReason::SinkRejected { detail: msg })
        }
        _ => ConnectorError::transient(msg),
    }
}

/// A `schema.table` reference.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TableRef {
    pub schema: String,
    pub table: String,
}

impl TableRef {
    pub fn parse(s: &str) -> Result<Self, ConnectorError> {
        let s = s.trim();
        let (schema, table) = match s.split_once('.') {
            Some((a, b)) => (a.trim(), b.trim()),
            None => ("public", s),
        };
        if schema.is_empty() || table.is_empty() || table.contains('.') {
            return Err(ConnectorError::config(format!("invalid table name '{s}'")));
        }
        Ok(Self {
            schema: schema.trim_matches('"').to_string(),
            table: table.trim_matches('"').to_string(),
        })
    }

    pub fn quoted(&self) -> String {
        format!("{}.{}", quote_ident(&self.schema), quote_ident(&self.table))
    }
}

impl std::fmt::Display for TableRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.schema, self.table)
    }
}

pub fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

pub fn quote_literal(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// Default slot/publication names derived from the connector name.
pub fn default_slot_name(connector: &str) -> String {
    truncate63(format!("exspeed_{}_slot", sanitized_name(connector)))
}

pub fn default_publication_name(connector: &str) -> String {
    truncate63(format!("exspeed_{}_pub", sanitized_name(connector)))
}

fn truncate63(mut s: String) -> String {
    s.truncate(63);
    s
}

/// Replication slot names: lowercase letters, digits and `_`, ≤ 63 bytes.
pub fn validate_slot_name(s: &str) -> Result<(), ConnectorError> {
    if s.is_empty()
        || s.len() > 63
        || !s
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
    {
        return Err(ConnectorError::config(format!(
            "invalid slot_name '{s}': use 1-63 lowercase letters, digits or '_'"
        )));
    }
    Ok(())
}

/// Parse a connection string for the replication client.
pub fn replication_config(
    conn_str: &str,
    slot: &str,
    publication: &str,
    start_lsn: Lsn,
) -> Result<ReplicationConfig, ConnectorError> {
    let cfg: tokio_postgres::Config = conn_str
        .parse()
        .map_err(|e| ConnectorError::config(format!("invalid connection string: {e}")))?;
    let host = cfg
        .get_hosts()
        .first()
        .map(|h| match h {
            tokio_postgres::config::Host::Tcp(s) => s.clone(),
            #[cfg(unix)]
            tokio_postgres::config::Host::Unix(p) => p.to_string_lossy().into_owned(),
        })
        .unwrap_or_else(|| "127.0.0.1".to_string());
    Ok(ReplicationConfig {
        host,
        port: cfg.get_ports().first().copied().unwrap_or(5432),
        user: cfg.get_user().unwrap_or("postgres").to_string(),
        password: cfg
            .get_password()
            .map(|p| String::from_utf8_lossy(p).into_owned())
            .unwrap_or_default(),
        database: cfg.get_dbname().unwrap_or("postgres").to_string(),
        slot: slot.to_string(),
        publication: publication.to_string(),
        start_lsn,
        status_interval: std::time::Duration::from_secs(1),
        idle_wakeup_interval: std::time::Duration::from_secs(5),
        ..Default::default()
    })
}

pub fn validate_connection_string(s: &str) -> Result<(), ConnectorError> {
    s.parse::<tokio_postgres::Config>()
        .map(drop)
        .map_err(|e| ConnectorError::config(format!("invalid connection string: {e}")))
}

/// `wal_level` must be `logical` for CDC.
pub async fn check_wal_level(client: &Client) -> Result<(), ConnectorError> {
    let row = client
        .query_one("SHOW wal_level", &[])
        .await
        .map_err(|e| classify(&e, "SHOW wal_level"))?;
    let level: String = row.get(0);
    if level != "logical" {
        return Err(ConnectorError::fatal(format!(
            "wal_level is '{level}'; CDC needs wal_level=logical"
        )));
    }
    Ok(())
}

/// Create the publication if missing; if it exists with a different table
/// set, `ALTER PUBLICATION … SET TABLE` to the configured tables.
pub async fn ensure_publication(
    client: &Client,
    name: &str,
    tables: &[TableRef],
) -> Result<(), ConnectorError> {
    let exists = client
        .query_opt("SELECT 1 FROM pg_publication WHERE pubname = $1", &[&name])
        .await
        .map_err(|e| classify(&e, "query pg_publication"))?
        .is_some();
    let list = tables
        .iter()
        .map(|t| t.quoted())
        .collect::<Vec<_>>()
        .join(", ");
    if !exists {
        let sql = format!("CREATE PUBLICATION {} FOR TABLE {list}", quote_ident(name));
        match client.batch_execute(&sql).await {
            Ok(()) => info!(publication = name, "created publication"),
            Err(e) if e.code().map(|c| c.code()) == Some("42710") => {} // raced: exists
            Err(e) => {
                return Err(classify(
                    &e,
                    &format!("create publication (run manually: {sql})"),
                ))
            }
        }
        return Ok(());
    }
    let rows = client
        .query(
            "SELECT schemaname::text, tablename::text FROM pg_publication_tables WHERE pubname = $1",
            &[&name],
        )
        .await
        .map_err(|e| classify(&e, "query pg_publication_tables"))?;
    let mut current: Vec<TableRef> = rows
        .iter()
        .map(|r| TableRef {
            schema: r.get(0),
            table: r.get(1),
        })
        .collect();
    current.sort();
    let mut wanted = tables.to_vec();
    wanted.sort();
    wanted.dedup();
    if current != wanted {
        let sql = format!("ALTER PUBLICATION {} SET TABLE {list}", quote_ident(name));
        client
            .batch_execute(&sql)
            .await
            .map_err(|e| classify(&e, &format!("alter publication (run manually: {sql})")))?;
        info!(publication = name, tables = %list, "publication table set updated");
    }
    Ok(())
}

/// Whether a logical slot exists (and uses pgoutput).
pub async fn slot_exists(client: &Client, slot: &str) -> Result<bool, ConnectorError> {
    let row = client
        .query_opt(
            "SELECT plugin::text FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .await
        .map_err(|e| classify(&e, "query pg_replication_slots"))?;
    match row {
        None => Ok(false),
        Some(r) => {
            let plugin: Option<String> = r.get(0);
            if plugin.as_deref() != Some("pgoutput") {
                return Err(ConnectorError::fatal(format!(
                    "replication slot '{slot}' exists but uses plugin {plugin:?}, not pgoutput"
                )));
            }
            Ok(true)
        }
    }
}

pub async fn ensure_slot(client: &Client, slot: &str) -> Result<(), ConnectorError> {
    if slot_exists(client, slot).await? {
        return Ok(());
    }
    match client
        .query(
            "SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&slot],
        )
        .await
    {
        Ok(_) => {
            info!(slot, "created replication slot");
            Ok(())
        }
        Err(e) if e.code().map(|c| c.code()) == Some("42710") => Ok(()),
        Err(e) => Err(classify(&e, "create replication slot")),
    }
}

/// Drop the slot (waits briefly if it is still marked active by a
/// just-closed connection).
pub async fn drop_slot(client: &Client, slot: &str) -> Result<(), ConnectorError> {
    for attempt in 0..10 {
        if !slot_exists(client, slot).await? {
            return Ok(());
        }
        match client
            .query("SELECT pg_drop_replication_slot($1)", &[&slot])
            .await
        {
            Ok(_) => {
                info!(slot, "dropped replication slot");
                return Ok(());
            }
            Err(e) if e.code().map(|c| c.code()) == Some("55006") && attempt < 9 => {
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
            Err(e) => return Err(classify(&e, "drop replication slot")),
        }
    }
    warn!(slot, "replication slot still active; not dropped");
    Ok(())
}

pub async fn drop_publication(client: &Client, name: &str) -> Result<(), ConnectorError> {
    client
        .batch_execute(&format!("DROP PUBLICATION IF EXISTS {}", quote_ident(name)))
        .await
        .map_err(|e| classify(&e, "drop publication"))
}

// ---------------------------------------------------------------------------
// Value typing by OID
// ---------------------------------------------------------------------------

pub mod oid {
    pub const BOOL: u32 = 16;
    pub const INT8: u32 = 20;
    pub const INT2: u32 = 21;
    pub const INT4: u32 = 23;
    pub const OID: u32 = 26;
    pub const JSON: u32 = 114;
    pub const FLOAT4: u32 = 700;
    pub const FLOAT8: u32 = 701;
    pub const NUMERIC: u32 = 1700;
    pub const JSONB: u32 = 3802;
}

/// Convert a column's text representation to a typed JSON value.
///
/// - `bool` → boolean; `int2/int4/int8/oid` → integer
/// - `float4/float8` → number (`NaN`/`Infinity` stay strings)
/// - `numeric` → number when it round-trips exactly, otherwise a string
///   (no silent precision loss)
/// - `json/jsonb` → parsed JSON
/// - everything else (text, timestamps, uuid, arrays, …) → string
pub fn typed_value(type_oid: u32, text: &str) -> serde_json::Value {
    use serde_json::Value;
    match type_oid {
        oid::BOOL => match text {
            "t" | "true" => Value::Bool(true),
            "f" | "false" => Value::Bool(false),
            _ => Value::String(text.to_string()),
        },
        oid::INT2 | oid::INT4 | oid::INT8 | oid::OID => text
            .parse::<i64>()
            .map(Value::from)
            .or_else(|_| text.parse::<u64>().map(Value::from))
            .unwrap_or_else(|_| Value::String(text.to_string())),
        oid::FLOAT4 | oid::FLOAT8 => text
            .parse::<f64>()
            .ok()
            .and_then(serde_json::Number::from_f64)
            .map(Value::Number)
            .unwrap_or_else(|| Value::String(text.to_string())),
        oid::NUMERIC => numeric_value(text),
        oid::JSON | oid::JSONB => {
            serde_json::from_str(text).unwrap_or_else(|_| Value::String(text.to_string()))
        }
        _ => Value::String(text.to_string()),
    }
}

fn numeric_value(text: &str) -> serde_json::Value {
    use serde_json::Value;
    if let Ok(i) = text.parse::<i64>() {
        return Value::from(i);
    }
    if let Ok(f) = text.parse::<f64>() {
        if f.is_finite() && canonical_decimal(&f.to_string()) == canonical_decimal(text) {
            if let Some(n) = serde_json::Number::from_f64(f) {
                return Value::Number(n);
            }
        }
    }
    Value::String(text.to_string())
}

/// Strip trailing fractional zeros and a trailing '.', so "1.50" == "1.5".
fn canonical_decimal(s: &str) -> String {
    let s = s.trim_start_matches('+');
    if let Some((int, frac)) = s.split_once('.') {
        let frac = frac.trim_end_matches('0');
        if frac.is_empty() {
            int.to_string()
        } else {
            format!("{int}.{frac}")
        }
    } else {
        s.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::ErrorKind;
    use serde_json::json;

    #[test]
    fn typed_values() {
        assert_eq!(typed_value(oid::BOOL, "t"), json!(true));
        assert_eq!(typed_value(oid::INT4, "42"), json!(42));
        assert_eq!(
            typed_value(oid::INT8, "-9000000000"),
            json!(-9_000_000_000i64)
        );
        assert_eq!(typed_value(oid::FLOAT8, "1.5"), json!(1.5));
        assert_eq!(typed_value(oid::FLOAT8, "NaN"), json!("NaN"));
        assert_eq!(typed_value(oid::NUMERIC, "12.50"), json!(12.5));
        assert_eq!(typed_value(oid::NUMERIC, "7"), json!(7));
        assert_eq!(
            typed_value(oid::NUMERIC, "12345678901234567890.123456789"),
            json!("12345678901234567890.123456789")
        );
        assert_eq!(typed_value(oid::JSONB, r#"{"a":[1]}"#), json!({"a": [1]}));
        assert_eq!(typed_value(25, "hello"), json!("hello"));
        assert_eq!(
            typed_value(1184, "2024-01-01 00:00:00+00"),
            json!("2024-01-01 00:00:00+00")
        );
    }

    #[test]
    fn sqlstate_taxonomy() {
        let k = |c: &str| classify_sqlstate(c, String::new()).kind();
        assert_eq!(k("08006"), ErrorKind::Connection);
        assert_eq!(k("57P01"), ErrorKind::Connection);
        assert_eq!(k("28P01"), ErrorKind::Fatal);
        assert_eq!(k("42P01"), ErrorKind::Fatal);
        assert_eq!(k("3D000"), ErrorKind::Fatal);
        assert_eq!(k("23502"), ErrorKind::Poison);
        assert_eq!(k("40001"), ErrorKind::Transient);
        assert_eq!(k("55006"), ErrorKind::Transient);
    }

    #[test]
    fn names() {
        assert_eq!(default_slot_name("Orders-CDC"), "exspeed_orders_cdc_slot");
        assert!(validate_slot_name("exspeed_x_slot").is_ok());
        assert!(validate_slot_name("Bad-Name").is_err());
        assert_eq!(
            TableRef::parse("users").unwrap().to_string(),
            "public.users"
        );
        assert_eq!(
            TableRef::parse("app.users").unwrap().quoted(),
            "\"app\".\"users\""
        );
        assert!(TableRef::parse("a.b.c").is_err());
    }
}
