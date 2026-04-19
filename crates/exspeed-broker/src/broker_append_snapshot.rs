use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotEntry {
    pub msg_id: String,
    pub offset: u64,
    pub inserted_at_unix_ms: u64,
    pub body_hash: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    pub covers_through_unix_ms: u64,
    pub entries: Vec<SnapshotEntry>,
}

const MAGIC: &[u8; 4] = b"EXSD";
const VERSION: u8 = 1;

pub fn snapshot_path(stream_dir: &Path) -> PathBuf {
    stream_dir.join("dedup_snapshot.bin")
}

pub fn write_snapshot(path: &Path, snapshot: &Snapshot) -> io::Result<()> {
    let tmp = path.with_extension("bin.tmp");
    let mut buf: Vec<u8> = Vec::new();
    buf.extend_from_slice(MAGIC);
    buf.push(VERSION);
    buf.extend_from_slice(&snapshot.covers_through_unix_ms.to_le_bytes());
    buf.extend_from_slice(&(snapshot.entries.len() as u32).to_le_bytes());
    for e in &snapshot.entries {
        let id = e.msg_id.as_bytes();
        buf.extend_from_slice(&(id.len() as u16).to_le_bytes());
        buf.extend_from_slice(id);
        buf.extend_from_slice(&e.offset.to_le_bytes());
        buf.extend_from_slice(&e.inserted_at_unix_ms.to_le_bytes());
        buf.extend_from_slice(&e.body_hash.to_le_bytes());
    }
    let crc = crc32c::crc32c(&buf);
    buf.extend_from_slice(&crc.to_le_bytes());

    let mut f = std::fs::File::create(&tmp)?;
    f.write_all(&buf)?;
    f.sync_all()?;
    drop(f);
    std::fs::rename(&tmp, path)?;

    if let Some(parent) = path.parent() {
        if let Ok(d) = std::fs::File::open(parent) {
            let _ = d.sync_all();
        }
    }
    Ok(())
}

pub fn read_snapshot(path: &Path) -> io::Result<Snapshot> {
    let mut buf = Vec::new();
    std::fs::File::open(path)?.read_to_end(&mut buf)?;
    let header_size = 4 + 1 + 8 + 4;
    if buf.len() < header_size + 4 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "snapshot truncated: header too short",
        ));
    }

    let body_len = buf.len() - 4;
    let expected_crc = u32::from_le_bytes(buf[body_len..].try_into().unwrap());
    let actual_crc = crc32c::crc32c(&buf[..body_len]);
    if expected_crc != actual_crc {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("snapshot crc mismatch: expected {expected_crc:08x}, got {actual_crc:08x}"),
        ));
    }

    let mut off = 0usize;
    if &buf[off..off + 4] != MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "snapshot magic mismatch",
        ));
    }
    off += 4;
    let version = buf[off];
    off += 1;
    if version != VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("snapshot version {version} unsupported"),
        ));
    }
    let covers_through_unix_ms =
        u64::from_le_bytes(buf[off..off + 8].try_into().unwrap());
    off += 8;
    let entry_count =
        u32::from_le_bytes(buf[off..off + 4].try_into().unwrap()) as usize;
    off += 4;

    let mut entries = Vec::with_capacity(entry_count);
    for _ in 0..entry_count {
        if off + 2 > body_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "snapshot truncated at msg_id len",
            ));
        }
        let id_len =
            u16::from_le_bytes(buf[off..off + 2].try_into().unwrap()) as usize;
        off += 2;
        if off + id_len + 8 + 8 + 8 > body_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "snapshot truncated at entry",
            ));
        }
        let msg_id = String::from_utf8(buf[off..off + id_len].to_vec()).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid utf8 msg_id: {e}"),
            )
        })?;
        off += id_len;
        let offset = u64::from_le_bytes(buf[off..off + 8].try_into().unwrap());
        off += 8;
        let inserted_at_unix_ms =
            u64::from_le_bytes(buf[off..off + 8].try_into().unwrap());
        off += 8;
        let body_hash = u64::from_le_bytes(buf[off..off + 8].try_into().unwrap());
        off += 8;
        entries.push(SnapshotEntry {
            msg_id,
            offset,
            inserted_at_unix_ms,
            body_hash,
        });
    }

    Ok(Snapshot {
        covers_through_unix_ms,
        entries,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn roundtrip() {
        let dir = TempDir::new().unwrap();
        let path = snapshot_path(dir.path());
        let snap = Snapshot {
            covers_through_unix_ms: 1_700_000_000_000,
            entries: vec![
                SnapshotEntry {
                    msg_id: "a".into(),
                    offset: 1,
                    inserted_at_unix_ms: 1_699_999_000_000,
                    body_hash: 0xdeadbeef,
                },
                SnapshotEntry {
                    msg_id: "bb".into(),
                    offset: 2,
                    inserted_at_unix_ms: 1_699_999_500_000,
                    body_hash: 0xcafef00d,
                },
            ],
        };
        write_snapshot(&path, &snap).unwrap();
        assert_eq!(read_snapshot(&path).unwrap(), snap);
    }

    #[test]
    fn corrupted_crc_fails() {
        let dir = TempDir::new().unwrap();
        let path = snapshot_path(dir.path());
        let snap = Snapshot {
            covers_through_unix_ms: 100,
            entries: vec![SnapshotEntry {
                msg_id: "a".into(),
                offset: 1,
                inserted_at_unix_ms: 50,
                body_hash: 42,
            }],
        };
        write_snapshot(&path, &snap).unwrap();
        let mut bytes = std::fs::read(&path).unwrap();
        let mid = bytes.len() / 2;
        bytes[mid] ^= 0xff;
        std::fs::write(&path, bytes).unwrap();
        assert!(read_snapshot(&path)
            .unwrap_err()
            .to_string()
            .contains("crc mismatch"));
    }

    #[test]
    fn truncated_fails() {
        let dir = TempDir::new().unwrap();
        let path = snapshot_path(dir.path());
        let snap = Snapshot {
            covers_through_unix_ms: 100,
            entries: vec![SnapshotEntry {
                msg_id: "a".into(),
                offset: 1,
                inserted_at_unix_ms: 50,
                body_hash: 42,
            }],
        };
        write_snapshot(&path, &snap).unwrap();
        let bytes = std::fs::read(&path).unwrap();
        std::fs::write(&path, &bytes[..bytes.len() / 2]).unwrap();
        assert!(read_snapshot(&path).is_err());
    }
}
