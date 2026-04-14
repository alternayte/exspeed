use std::fs;
use std::io;
use std::path::Path;

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct ConnectorOffset {
    pub position: String,
}

/// Save a connector's position to disk.
pub fn save_offset(path: &Path, position: &str) -> io::Result<()> {
    let offset = ConnectorOffset { position: position.to_string() };
    let json = serde_json::to_string_pretty(&offset)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    fs::write(path, json)?;
    fs::File::open(path)?.sync_all()?;
    Ok(())
}

/// Load a connector's position from disk. Returns None if file doesn't exist.
pub fn load_offset(path: &Path) -> io::Result<Option<String>> {
    if !path.exists() {
        return Ok(None);
    }
    let json = fs::read_to_string(path)?;
    let offset: ConnectorOffset = serde_json::from_str(&json)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    Ok(Some(offset.position))
}

/// Delete a connector's offset file.
pub fn delete_offset(path: &Path) -> io::Result<()> {
    if path.exists() {
        fs::remove_file(path)?;
    }
    Ok(())
}

/// Sink offset: tracks stream offset as u64.
#[derive(Debug, Serialize, Deserialize)]
pub struct SinkOffset {
    pub offset: u64,
}

pub fn save_sink_offset(path: &Path, offset: u64) -> io::Result<()> {
    let data = SinkOffset { offset };
    let json = serde_json::to_string_pretty(&data)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    fs::write(path, json)?;
    fs::File::open(path)?.sync_all()?;
    Ok(())
}

pub fn load_sink_offset(path: &Path) -> io::Result<u64> {
    if !path.exists() {
        return Ok(0);
    }
    let json = fs::read_to_string(path)?;
    let data: SinkOffset = serde_json::from_str(&json)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    Ok(data.offset)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn source_offset_roundtrip() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.offset.json");

        save_offset(&path, "0/1A2B3C4").unwrap();
        let loaded = load_offset(&path).unwrap();
        assert_eq!(loaded, Some("0/1A2B3C4".to_string()));
    }

    #[test]
    fn source_offset_missing_returns_none() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("nonexistent.json");
        assert_eq!(load_offset(&path).unwrap(), None);
    }

    #[test]
    fn sink_offset_roundtrip() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("sink.offset.json");

        save_sink_offset(&path, 12345).unwrap();
        let loaded = load_sink_offset(&path).unwrap();
        assert_eq!(loaded, 12345);
    }

    #[test]
    fn sink_offset_missing_returns_zero() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("nonexistent.json");
        assert_eq!(load_sink_offset(&path).unwrap(), 0);
    }

    #[test]
    fn delete_offset_removes_file() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.offset.json");
        save_offset(&path, "pos").unwrap();
        assert!(path.exists());
        delete_offset(&path).unwrap();
        assert!(!path.exists());
    }
}
