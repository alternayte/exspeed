use std::fs::File;
use std::io::{self, BufWriter, Read, Write};
use std::path::Path;

const SIDX_MAGIC: &[u8; 4] = b"EXSI";
const SIDX_VERSION: u8 = 0x01;

#[derive(Debug, Clone)]
pub struct SecondaryIndex {
    entries: Vec<(u64, u64)>, // (value_hash, offset)
}

impl SecondaryIndex {
    pub fn build(path: &Path, entries: &mut Vec<(u64, u64)>) -> io::Result<Self> {
        entries.sort_by_key(|&(hash, _)| hash);
        let idx = Self { entries: entries.clone() };
        idx.save(path)?;
        Ok(idx)
    }

    pub fn lookup(&self, value: &str) -> Vec<u64> {
        let hash = Self::hash_value(value);
        let mut offsets = Vec::new();
        let start = self.entries.partition_point(|&(h, _)| h < hash);
        for &(h, offset) in &self.entries[start..] {
            if h != hash { break; }
            offsets.push(offset);
        }
        offsets
    }

    pub fn hash_value(value: &str) -> u64 {
        let mut h: u64 = 0xcbf29ce484222325; // FNV-1a offset basis
        for &b in value.as_bytes() {
            h ^= b as u64;
            h = h.wrapping_mul(0x100000001b3); // FNV-1a prime
        }
        h
    }

    fn save(&self, path: &Path) -> io::Result<()> {
        let mut file = BufWriter::new(File::create(path)?);
        file.write_all(SIDX_MAGIC)?;
        file.write_all(&[SIDX_VERSION])?;
        file.write_all(&(self.entries.len() as u32).to_le_bytes())?;
        for &(hash, offset) in &self.entries {
            file.write_all(&hash.to_le_bytes())?;
            file.write_all(&offset.to_le_bytes())?;
        }
        file.flush()?;
        Ok(())
    }

    pub fn load(path: &Path) -> io::Result<Self> {
        let mut file = File::open(path)?;
        let mut magic = [0u8; 4];
        file.read_exact(&mut magic)?;
        if &magic != SIDX_MAGIC {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "bad sidx magic"));
        }
        let mut ver = [0u8; 1];
        file.read_exact(&mut ver)?;
        if ver[0] != SIDX_VERSION {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "unsupported sidx version"));
        }
        let mut buf4 = [0u8; 4];
        file.read_exact(&mut buf4)?;
        let count = u32::from_le_bytes(buf4) as usize;
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            let mut buf16 = [0u8; 16];
            file.read_exact(&mut buf16)?;
            let hash = u64::from_le_bytes(buf16[..8].try_into().unwrap());
            let offset = u64::from_le_bytes(buf16[8..].try_into().unwrap());
            entries.push((hash, offset));
        }
        Ok(Self { entries })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn build_and_lookup() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.sidx");
        let mut entries = vec![
            (SecondaryIndex::hash_value("alice"), 0),
            (SecondaryIndex::hash_value("bob"), 1),
            (SecondaryIndex::hash_value("alice"), 5),
            (SecondaryIndex::hash_value("charlie"), 10),
        ];
        let idx = SecondaryIndex::build(&path, &mut entries).unwrap();

        let alice = idx.lookup("alice");
        assert_eq!(alice.len(), 2);
        assert!(alice.contains(&0));
        assert!(alice.contains(&5));

        let bob = idx.lookup("bob");
        assert_eq!(bob, vec![1]);

        assert!(idx.lookup("missing").is_empty());
    }

    #[test]
    fn save_load_roundtrip() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.sidx");
        let mut entries = vec![
            (SecondaryIndex::hash_value("x"), 10),
            (SecondaryIndex::hash_value("y"), 20),
            (SecondaryIndex::hash_value("z"), 30),
        ];
        SecondaryIndex::build(&path, &mut entries).unwrap();
        let loaded = SecondaryIndex::load(&path).unwrap();
        assert_eq!(loaded.lookup("x"), vec![10]);
        assert_eq!(loaded.lookup("y"), vec![20]);
        assert_eq!(loaded.lookup("z"), vec![30]);
    }

    #[test]
    fn empty_index() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("empty.sidx");
        let mut entries = Vec::new();
        let idx = SecondaryIndex::build(&path, &mut entries).unwrap();
        assert!(idx.lookup("anything").is_empty());
    }

    #[test]
    fn hash_consistency() {
        let h1 = SecondaryIndex::hash_value("test");
        let h2 = SecondaryIndex::hash_value("test");
        assert_eq!(h1, h2);
        let h3 = SecondaryIndex::hash_value("different");
        assert_ne!(h1, h3);
    }
}
