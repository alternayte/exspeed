//! pgoutput (protocol version 1) message parser, used by `postgres_cdc` and
//! `postgres_outbox` in CDC mode.
//!
//! The parser never panics: every read is bounds-checked and a short or
//! malformed buffer is an error.

use crate::traits::ConnectorError;

/// A column definition from a Relation message.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDef {
    pub name: String,
    pub type_oid: u32,
    pub type_modifier: i32,
    /// Part of the replica identity key (flag bit 1).
    pub is_key: bool,
}

/// A table relation received from the WAL.
#[derive(Debug, Clone, PartialEq)]
pub struct Relation {
    pub id: u32,
    pub schema: String,
    pub table: String,
    /// `d` default (primary key), `n` nothing, `f` full, `i` index.
    pub replica_identity: u8,
    pub columns: Vec<ColumnDef>,
}

impl Relation {
    /// Indexes of the replica-identity key columns, in column order.
    pub fn key_columns(&self) -> Vec<usize> {
        self.columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.is_key)
            .map(|(i, _)| i)
            .collect()
    }
}

/// Decoded column value from a tuple.
#[derive(Debug, Clone, PartialEq)]
pub enum ColValue {
    Null,
    Text(String),
    /// Unchanged TOASTed value (UPDATE): the value is not in the WAL.
    Unchanged,
}

/// Which old tuple an UPDATE/DELETE carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OldKind {
    /// `K`: only the replica-identity key columns are set.
    Key,
    /// `O`: the full old row (REPLICA IDENTITY FULL).
    Full,
}

#[derive(Debug, Clone, PartialEq)]
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
        old: Option<(OldKind, Vec<ColValue>)>,
        new_tuple: Vec<ColValue>,
    },
    Delete {
        relation_id: u32,
        old: (OldKind, Vec<ColValue>),
    },
    Relation(Relation),
    Truncate {
        relation_ids: Vec<u32>,
    },
    /// Origin, Type, Message, … — not needed.
    Unknown(u8),
}

/// Bounds-checked big-endian reader.
struct Reader<'a> {
    buf: &'a [u8],
    what: &'static str,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8], what: &'static str) -> Self {
        Self { buf, what }
    }

    fn short(&self) -> ConnectorError {
        ConnectorError::transient(format!("pgoutput: truncated {} message", self.what))
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], ConnectorError> {
        if self.buf.len() < n {
            return Err(self.short());
        }
        let (head, tail) = self.buf.split_at(n);
        self.buf = tail;
        Ok(head)
    }

    fn u8(&mut self) -> Result<u8, ConnectorError> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16, ConnectorError> {
        Ok(u16::from_be_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> Result<u32, ConnectorError> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn i32(&mut self) -> Result<i32, ConnectorError> {
        Ok(i32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64, ConnectorError> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn i64(&mut self) -> Result<i64, ConnectorError> {
        Ok(i64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn cstring(&mut self) -> Result<String, ConnectorError> {
        let pos = self
            .buf
            .iter()
            .position(|&b| b == 0)
            .ok_or_else(|| self.short())?;
        let s = String::from_utf8_lossy(&self.buf[..pos]).into_owned();
        self.buf = &self.buf[pos + 1..];
        Ok(s)
    }

    fn tuple(&mut self) -> Result<Vec<ColValue>, ConnectorError> {
        let n = self.u16()? as usize;
        let mut values = Vec::with_capacity(n.min(1024));
        for _ in 0..n {
            match self.u8()? {
                b'n' => values.push(ColValue::Null),
                b'u' => values.push(ColValue::Unchanged),
                b't' => {
                    let len = self.u32()? as usize;
                    let bytes = self.take(len)?;
                    values.push(ColValue::Text(String::from_utf8_lossy(bytes).into_owned()));
                }
                b'b' => {
                    // Binary format is never requested; skip defensively.
                    let len = self.u32()? as usize;
                    self.take(len)?;
                    values.push(ColValue::Null);
                }
                other => {
                    return Err(ConnectorError::transient(format!(
                        "pgoutput: unknown tuple column kind {other:#x}"
                    )))
                }
            }
        }
        Ok(values)
    }
}

/// Parse one pgoutput message (an XLogData payload).
pub fn parse_pgoutput_message(data: &[u8]) -> Result<WalEvent, ConnectorError> {
    let Some((&tag, rest)) = data.split_first() else {
        return Err(ConnectorError::transient("pgoutput: empty message"));
    };
    match tag {
        b'B' => {
            let mut r = Reader::new(rest, "Begin");
            Ok(WalEvent::Begin {
                final_lsn: r.u64()?,
                timestamp: r.i64()?,
                xid: r.u32()?,
            })
        }
        b'C' => {
            let mut r = Reader::new(rest, "Commit");
            let _flags = r.u8()?;
            Ok(WalEvent::Commit {
                commit_lsn: r.u64()?,
                end_lsn: r.u64()?,
                timestamp: r.i64()?,
            })
        }
        b'R' => {
            let mut r = Reader::new(rest, "Relation");
            let id = r.u32()?;
            let schema = r.cstring()?;
            let table = r.cstring()?;
            let replica_identity = r.u8()?;
            let n = r.u16()? as usize;
            let mut columns = Vec::with_capacity(n.min(1024));
            for _ in 0..n {
                let flags = r.u8()?;
                columns.push(ColumnDef {
                    name: r.cstring()?,
                    type_oid: r.u32()?,
                    type_modifier: r.i32()?,
                    is_key: flags & 1 == 1,
                });
            }
            Ok(WalEvent::Relation(Relation {
                id,
                schema,
                table,
                replica_identity,
                columns,
            }))
        }
        b'I' => {
            let mut r = Reader::new(rest, "Insert");
            let relation_id = r.u32()?;
            match r.u8()? {
                b'N' => {}
                other => {
                    return Err(ConnectorError::transient(format!(
                        "pgoutput: unexpected Insert tuple tag {other:#x}"
                    )))
                }
            }
            Ok(WalEvent::Insert {
                relation_id,
                new_tuple: r.tuple()?,
            })
        }
        b'U' => {
            let mut r = Reader::new(rest, "Update");
            let relation_id = r.u32()?;
            let mut tag = r.u8()?;
            let old = match tag {
                b'K' | b'O' => {
                    let kind = if tag == b'K' {
                        OldKind::Key
                    } else {
                        OldKind::Full
                    };
                    let t = r.tuple()?;
                    tag = r.u8()?;
                    Some((kind, t))
                }
                _ => None,
            };
            if tag != b'N' {
                return Err(ConnectorError::transient(format!(
                    "pgoutput: unexpected Update tuple tag {tag:#x}"
                )));
            }
            Ok(WalEvent::Update {
                relation_id,
                old,
                new_tuple: r.tuple()?,
            })
        }
        b'D' => {
            let mut r = Reader::new(rest, "Delete");
            let relation_id = r.u32()?;
            let kind = match r.u8()? {
                b'K' => OldKind::Key,
                b'O' => OldKind::Full,
                other => {
                    return Err(ConnectorError::transient(format!(
                        "pgoutput: unexpected Delete tuple tag {other:#x}"
                    )))
                }
            };
            Ok(WalEvent::Delete {
                relation_id,
                old: (kind, r.tuple()?),
            })
        }
        b'T' => {
            let mut r = Reader::new(rest, "Truncate");
            let n = r.u32()? as usize;
            let _options = r.u8()?;
            let mut relation_ids = Vec::with_capacity(n.min(1024));
            for _ in 0..n {
                relation_ids.push(r.u32()?);
            }
            Ok(WalEvent::Truncate { relation_ids })
        }
        other => Ok(WalEvent::Unknown(other)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn relation_bytes() -> Vec<u8> {
        let mut d = vec![b'R'];
        d.extend_from_slice(&1u32.to_be_bytes());
        d.extend_from_slice(b"public\0orders\0");
        d.push(b'd');
        d.extend_from_slice(&2u16.to_be_bytes());
        d.push(1); // key
        d.extend_from_slice(b"id\0");
        d.extend_from_slice(&23u32.to_be_bytes());
        d.extend_from_slice(&(-1i32).to_be_bytes());
        d.push(0);
        d.extend_from_slice(b"name\0");
        d.extend_from_slice(&25u32.to_be_bytes());
        d.extend_from_slice(&(-1i32).to_be_bytes());
        d
    }

    #[test]
    fn relation_with_key_flags() {
        let WalEvent::Relation(rel) = parse_pgoutput_message(&relation_bytes()).unwrap() else {
            panic!()
        };
        assert_eq!(rel.table, "orders");
        assert_eq!(rel.replica_identity, b'd');
        assert_eq!(rel.key_columns(), vec![0]);
        assert_eq!(rel.columns[1].type_oid, 25);
    }

    #[test]
    fn begin_commit() {
        let mut d = vec![b'B'];
        d.extend_from_slice(&100u64.to_be_bytes());
        d.extend_from_slice(&200i64.to_be_bytes());
        d.extend_from_slice(&42u32.to_be_bytes());
        assert_eq!(
            parse_pgoutput_message(&d).unwrap(),
            WalEvent::Begin {
                final_lsn: 100,
                timestamp: 200,
                xid: 42
            }
        );
        let mut c = vec![b'C', 0];
        c.extend_from_slice(&300u64.to_be_bytes());
        c.extend_from_slice(&400u64.to_be_bytes());
        c.extend_from_slice(&500i64.to_be_bytes());
        assert_eq!(
            parse_pgoutput_message(&c).unwrap(),
            WalEvent::Commit {
                commit_lsn: 300,
                end_lsn: 400,
                timestamp: 500
            }
        );
    }

    #[test]
    fn update_with_old_key_and_unchanged_toast() {
        let mut d = vec![b'U'];
        d.extend_from_slice(&1u32.to_be_bytes());
        d.push(b'K');
        d.extend_from_slice(&2u16.to_be_bytes());
        d.push(b't');
        d.extend_from_slice(&1u32.to_be_bytes());
        d.push(b'1');
        d.push(b'n');
        d.push(b'N');
        d.extend_from_slice(&2u16.to_be_bytes());
        d.push(b't');
        d.extend_from_slice(&1u32.to_be_bytes());
        d.push(b'2');
        d.push(b'u');
        let WalEvent::Update { old, new_tuple, .. } = parse_pgoutput_message(&d).unwrap() else {
            panic!()
        };
        let (kind, old) = old.unwrap();
        assert_eq!(kind, OldKind::Key);
        assert_eq!(old[0], ColValue::Text("1".into()));
        assert_eq!(
            new_tuple,
            vec![ColValue::Text("2".into()), ColValue::Unchanged]
        );
    }

    #[test]
    fn never_panics_on_short_or_garbage_input() {
        let samples: Vec<Vec<u8>> = vec![
            relation_bytes(),
            {
                let mut d = vec![b'I'];
                d.extend_from_slice(&1u32.to_be_bytes());
                d.push(b'N');
                d.extend_from_slice(&2u16.to_be_bytes());
                d.push(b't');
                d.extend_from_slice(&5u32.to_be_bytes());
                d.extend_from_slice(b"hello");
                d.push(b'n');
                d
            },
            vec![b'B', 1, 2, 3],
            vec![b'C', 0, 1],
            vec![b'U', 0, 0, 0, 1, b'O'],
            vec![b'D', 0, 0, 0, 1, b'K', 0, 5],
            vec![b'T', 0, 0, 0, 9],
        ];
        for s in &samples {
            for cut in 0..=s.len() {
                let _ = parse_pgoutput_message(&s[..cut]);
            }
        }
        // Every strict prefix of a valid message is an error, not a panic.
        let rel = relation_bytes();
        for cut in 1..rel.len() {
            assert!(parse_pgoutput_message(&rel[..cut]).is_err(), "cut {cut}");
        }
        // Huge declared lengths don't allocate or panic.
        let mut d = vec![b'I'];
        d.extend_from_slice(&1u32.to_be_bytes());
        d.push(b'N');
        d.extend_from_slice(&u16::MAX.to_be_bytes());
        d.push(b't');
        d.extend_from_slice(&u32::MAX.to_be_bytes());
        assert!(parse_pgoutput_message(&d).is_err());
    }
}
