//! Shared pgoutput logical replication protocol parser.
//!
//! This module handles:
//! - Creating publications and replication slots
//! - Starting replication and reading WAL messages
//! - Parsing pgoutput binary protocol (Begin, Commit, Relation, Insert, Update, Delete)
//! - Converting decoded rows to key-value pairs
//!
//! Used by both `postgres_outbox` (CDC mode) and `postgres` (CDC mode).

use std::collections::HashMap;

use bytes::Buf;
use tokio_postgres::Client;
use tracing::{debug, info, warn};

use crate::traits::ConnectorError;

// ---------------------------------------------------------------------------
// Decoded types
// ---------------------------------------------------------------------------

/// A column definition from a Relation message.
#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub type_oid: u32,
    pub type_modifier: i32,
}

/// A table relation received from the WAL.
#[derive(Debug, Clone)]
pub struct Relation {
    pub id: u32,
    pub schema: String,
    pub table: String,
    pub columns: Vec<ColumnDef>,
}

/// Decoded column value from a tuple in a WAL message.
#[derive(Debug, Clone)]
pub enum ColValue {
    Null,
    Text(String),
    Unchanged, // for TOAST columns in UPDATE
}

/// A decoded WAL change event.
#[derive(Debug, Clone)]
pub enum WalEvent {
    Begin {
        final_lsn: u64,
        timestamp: i64,
        xid: u32,
    },
    Commit {
        commit_lsn: u64,
        end_lsn: u64,
        timestamp: i64,
    },
    Insert {
        relation_id: u32,
        new_tuple: Vec<ColValue>,
    },
    Update {
        relation_id: u32,
        old_tuple: Option<Vec<ColValue>>,
        new_tuple: Vec<ColValue>,
    },
    Delete {
        relation_id: u32,
        old_tuple: Vec<ColValue>,
    },
    Relation(Relation),
    /// Unknown or unhandled message type — skip with warning.
    Unknown(u8),
}

// ---------------------------------------------------------------------------
// Protocol parsing
// ---------------------------------------------------------------------------

/// Parse a single pgoutput message from a CopyData payload.
pub fn parse_pgoutput_message(data: &[u8]) -> Result<WalEvent, ConnectorError> {
    if data.is_empty() {
        return Err(ConnectorError::Data("empty pgoutput message".into()));
    }

    let msg_type = data[0];
    let mut buf = &data[1..];

    match msg_type {
        b'B' => {
            // Begin: final_lsn(8) + timestamp(8) + xid(4)
            if buf.len() < 20 {
                return Err(ConnectorError::Data("truncated Begin message".into()));
            }
            let final_lsn = buf.get_u64();
            let timestamp = buf.get_i64();
            let xid = buf.get_u32();
            Ok(WalEvent::Begin {
                final_lsn,
                timestamp,
                xid,
            })
        }
        b'C' => {
            // Commit: flags(1) + commit_lsn(8) + end_lsn(8) + timestamp(8)
            if buf.len() < 25 {
                return Err(ConnectorError::Data("truncated Commit message".into()));
            }
            let _flags = buf.get_u8();
            let commit_lsn = buf.get_u64();
            let end_lsn = buf.get_u64();
            let timestamp = buf.get_i64();
            Ok(WalEvent::Commit {
                commit_lsn,
                end_lsn,
                timestamp,
            })
        }
        b'R' => {
            // Relation: id(4) + namespace(str) + name(str) + replica_identity(1) + ncols(2) + columns...
            let id = buf.get_u32();
            let schema = read_cstring(&mut buf)?;
            let table = read_cstring(&mut buf)?;
            let _replica_identity = buf.get_u8();
            let ncols = buf.get_u16() as usize;
            let mut columns = Vec::with_capacity(ncols);
            for _ in 0..ncols {
                let _flags = buf.get_u8();
                let name = read_cstring(&mut buf)?;
                let type_oid = buf.get_u32();
                let type_modifier = buf.get_i32();
                columns.push(ColumnDef {
                    name,
                    type_oid,
                    type_modifier,
                });
            }
            Ok(WalEvent::Relation(Relation {
                id,
                schema,
                table,
                columns,
            }))
        }
        b'I' => {
            // Insert: relation_id(4) + 'N' + tuple
            let relation_id = buf.get_u32();
            let _new_flag = buf.get_u8(); // always 'N'
            let new_tuple = parse_tuple(&mut buf)?;
            Ok(WalEvent::Insert {
                relation_id,
                new_tuple,
            })
        }
        b'U' => {
            // Update: relation_id(4) + ['K'|'O' + old_tuple] + 'N' + new_tuple
            let relation_id = buf.get_u32();
            let flag = buf.get_u8();
            let old_tuple = if flag == b'K' || flag == b'O' {
                let old = parse_tuple(&mut buf)?;
                let _new_flag = buf.get_u8(); // 'N'
                Some(old)
            } else {
                // flag was 'N' — no old tuple
                None
            };
            let new_tuple = parse_tuple(&mut buf)?;
            Ok(WalEvent::Update {
                relation_id,
                old_tuple,
                new_tuple,
            })
        }
        b'D' => {
            // Delete: relation_id(4) + 'K'|'O' + old_tuple
            let relation_id = buf.get_u32();
            let _flag = buf.get_u8();
            let old_tuple = parse_tuple(&mut buf)?;
            Ok(WalEvent::Delete {
                relation_id,
                old_tuple,
            })
        }
        other => Ok(WalEvent::Unknown(other)),
    }
}

/// Parse a tuple (row) from the pgoutput binary stream.
fn parse_tuple(buf: &mut &[u8]) -> Result<Vec<ColValue>, ConnectorError> {
    if buf.len() < 2 {
        return Err(ConnectorError::Data("truncated tuple header".into()));
    }
    let ncols = buf.get_u16() as usize;
    let mut values = Vec::with_capacity(ncols);
    for _ in 0..ncols {
        if buf.is_empty() {
            return Err(ConnectorError::Data("truncated tuple data".into()));
        }
        let col_type = buf.get_u8();
        match col_type {
            b'n' => values.push(ColValue::Null),
            b'u' => values.push(ColValue::Unchanged),
            b't' => {
                if buf.len() < 4 {
                    return Err(ConnectorError::Data("truncated text length".into()));
                }
                let len = buf.get_u32() as usize;
                if buf.len() < len {
                    return Err(ConnectorError::Data("truncated text value".into()));
                }
                let text = String::from_utf8_lossy(&buf[..len]).to_string();
                buf.advance(len);
                values.push(ColValue::Text(text));
            }
            other => {
                return Err(ConnectorError::Data(format!(
                    "unknown column type byte: {other}"
                )));
            }
        }
    }
    Ok(values)
}

/// Read a null-terminated C string from the buffer.
fn read_cstring(buf: &mut &[u8]) -> Result<String, ConnectorError> {
    let pos = buf
        .iter()
        .position(|&b| b == 0)
        .ok_or_else(|| ConnectorError::Data("unterminated C string".into()))?;
    let s = String::from_utf8_lossy(&buf[..pos]).to_string();
    *buf = &buf[pos + 1..]; // skip null byte
    Ok(s)
}

// ---------------------------------------------------------------------------
// Replication slot / publication management
// ---------------------------------------------------------------------------

/// Try to create a publication. Returns Ok if created or already exists.
pub async fn ensure_publication(
    client: &Client,
    publication_name: &str,
    tables: &[String],
) -> Result<(), ConnectorError> {
    let tables_clause = tables.join(", ");
    let create_sql = format!(
        "CREATE PUBLICATION {publication_name} FOR TABLE {tables_clause}"
    );

    match client.simple_query(&create_sql).await {
        Ok(_) => {
            info!(publication = publication_name, "created publication");
            Ok(())
        }
        Err(e) => {
            let msg = e.to_string();
            if msg.contains("already exists") {
                debug!(publication = publication_name, "publication already exists");
                Ok(())
            } else if msg.contains("permission denied") || msg.contains("must be superuser") {
                warn!(
                    publication = publication_name,
                    "cannot create publication — run this SQL manually:\n  {create_sql}"
                );
                Err(ConnectorError::Connection(format!(
                    "insufficient permissions to create publication '{publication_name}'. \
                     Run manually: {create_sql}"
                )))
            } else {
                Err(ConnectorError::Connection(format!(
                    "failed to create publication: {e}"
                )))
            }
        }
    }
}

/// Try to create a logical replication slot. Returns Ok if created or already exists.
pub async fn ensure_replication_slot(
    client: &Client,
    slot_name: &str,
) -> Result<(), ConnectorError> {
    let create_sql = format!(
        "SELECT pg_create_logical_replication_slot('{slot_name}', 'pgoutput')"
    );

    match client.simple_query(&create_sql).await {
        Ok(_) => {
            info!(slot = slot_name, "created replication slot");
            Ok(())
        }
        Err(e) => {
            let msg = e.to_string();
            if msg.contains("already exists") {
                debug!(slot = slot_name, "replication slot already exists");
                Ok(())
            } else if msg.contains("already active") || msg.contains("is active") {
                Err(ConnectorError::Connection(format!(
                    "replication slot '{slot_name}' is already active — \
                     another instance may be running"
                )))
            } else if msg.contains("permission denied") {
                warn!(
                    slot = slot_name,
                    "cannot create replication slot — run this SQL manually:\n  {create_sql}"
                );
                Err(ConnectorError::Connection(format!(
                    "insufficient permissions to create replication slot '{slot_name}'. \
                     Run manually: {create_sql}"
                )))
            } else {
                Err(ConnectorError::Connection(format!(
                    "failed to create replication slot: {e}"
                )))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Row-to-map conversion
// ---------------------------------------------------------------------------

/// Convert a decoded tuple + relation schema to a HashMap of column name -> string value.
pub fn tuple_to_map(
    relation: &Relation,
    tuple: &[ColValue],
) -> HashMap<String, Option<String>> {
    let mut map = HashMap::new();
    for (i, col) in relation.columns.iter().enumerate() {
        let value = tuple.get(i).and_then(|v| match v {
            ColValue::Null => None,
            ColValue::Unchanged => None, // TOAST — unchanged, not available
            ColValue::Text(s) => Some(s.clone()),
        });
        map.insert(col.name.clone(), value);
    }
    map
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_begin_message() {
        // 'B' + final_lsn(8) + timestamp(8) + xid(4)
        let mut data = vec![b'B'];
        data.extend_from_slice(&100u64.to_be_bytes()); // final_lsn
        data.extend_from_slice(&200i64.to_be_bytes()); // timestamp
        data.extend_from_slice(&42u32.to_be_bytes()); // xid

        let event = parse_pgoutput_message(&data).unwrap();
        match event {
            WalEvent::Begin {
                final_lsn,
                timestamp,
                xid,
            } => {
                assert_eq!(final_lsn, 100);
                assert_eq!(timestamp, 200);
                assert_eq!(xid, 42);
            }
            other => panic!("expected Begin, got {:?}", other),
        }
    }

    #[test]
    fn parse_commit_message() {
        let mut data = vec![b'C'];
        data.push(0); // flags
        data.extend_from_slice(&300u64.to_be_bytes()); // commit_lsn
        data.extend_from_slice(&400u64.to_be_bytes()); // end_lsn
        data.extend_from_slice(&500i64.to_be_bytes()); // timestamp

        let event = parse_pgoutput_message(&data).unwrap();
        match event {
            WalEvent::Commit {
                commit_lsn,
                end_lsn,
                timestamp,
            } => {
                assert_eq!(commit_lsn, 300);
                assert_eq!(end_lsn, 400);
                assert_eq!(timestamp, 500);
            }
            other => panic!("expected Commit, got {:?}", other),
        }
    }

    #[test]
    fn parse_relation_message() {
        let mut data = vec![b'R'];
        data.extend_from_slice(&1u32.to_be_bytes()); // relation id
        data.extend_from_slice(b"public\0"); // schema
        data.extend_from_slice(b"orders\0"); // table
        data.push(b'f'); // replica identity
        data.extend_from_slice(&2u16.to_be_bytes()); // 2 columns

        // Column 1: id
        data.push(0); // flags
        data.extend_from_slice(b"id\0");
        data.extend_from_slice(&23u32.to_be_bytes()); // int4 oid
        data.extend_from_slice(&(-1i32).to_be_bytes()); // type modifier

        // Column 2: name
        data.push(0);
        data.extend_from_slice(b"name\0");
        data.extend_from_slice(&25u32.to_be_bytes()); // text oid
        data.extend_from_slice(&(-1i32).to_be_bytes());

        let event = parse_pgoutput_message(&data).unwrap();
        match event {
            WalEvent::Relation(rel) => {
                assert_eq!(rel.id, 1);
                assert_eq!(rel.schema, "public");
                assert_eq!(rel.table, "orders");
                assert_eq!(rel.columns.len(), 2);
                assert_eq!(rel.columns[0].name, "id");
                assert_eq!(rel.columns[1].name, "name");
            }
            other => panic!("expected Relation, got {:?}", other),
        }
    }

    #[test]
    fn parse_insert_message() {
        let mut data = vec![b'I'];
        data.extend_from_slice(&1u32.to_be_bytes()); // relation_id
        data.push(b'N'); // new tuple flag
        data.extend_from_slice(&2u16.to_be_bytes()); // 2 columns

        // Column 1: text "42"
        data.push(b't');
        data.extend_from_slice(&2u32.to_be_bytes());
        data.extend_from_slice(b"42");

        // Column 2: null
        data.push(b'n');

        let event = parse_pgoutput_message(&data).unwrap();
        match event {
            WalEvent::Insert {
                relation_id,
                new_tuple,
            } => {
                assert_eq!(relation_id, 1);
                assert_eq!(new_tuple.len(), 2);
                assert!(matches!(&new_tuple[0], ColValue::Text(s) if s == "42"));
                assert!(matches!(&new_tuple[1], ColValue::Null));
            }
            other => panic!("expected Insert, got {:?}", other),
        }
    }

    #[test]
    fn unknown_message_type_returns_unknown() {
        let data = vec![b'X', 0, 0, 0];
        let event = parse_pgoutput_message(&data).unwrap();
        assert!(matches!(event, WalEvent::Unknown(b'X')));
    }

    #[test]
    fn tuple_to_map_conversion() {
        let relation = Relation {
            id: 1,
            schema: "public".into(),
            table: "orders".into(),
            columns: vec![
                ColumnDef { name: "id".into(), type_oid: 23, type_modifier: -1 },
                ColumnDef { name: "name".into(), type_oid: 25, type_modifier: -1 },
                ColumnDef { name: "email".into(), type_oid: 25, type_modifier: -1 },
            ],
        };
        let tuple = vec![
            ColValue::Text("42".into()),
            ColValue::Text("Alice".into()),
            ColValue::Null,
        ];

        let map = tuple_to_map(&relation, &tuple);
        assert_eq!(map.get("id"), Some(&Some("42".into())));
        assert_eq!(map.get("name"), Some(&Some("Alice".into())));
        assert_eq!(map.get("email"), Some(&None));
    }
}
