use std::fs::File;
use std::io::{self, Read, Write};
use std::path::Path;

const BLOOM_MAGIC: &[u8; 4] = b"EXBF";
const BLOOM_VERSION: u8 = 0x01;
const NUM_HASHES: u32 = 3;

#[derive(Debug, Clone)]
pub struct BloomFilter {
    bits: Vec<u8>,
    num_bits: u64,
    num_hashes: u32,
}

impl BloomFilter {
    pub fn new(expected_items: usize) -> Self {
        let num_bits = (expected_items as u64 * 10).max(64);
        let num_bytes = num_bits.div_ceil(8) as usize;
        Self {
            bits: vec![0u8; num_bytes],
            num_bits,
            num_hashes: NUM_HASHES,
        }
    }

    pub fn insert(&mut self, key: &[u8]) {
        for i in 0..self.num_hashes {
            let h = self.hash(key, i) % self.num_bits;
            self.bits[(h / 8) as usize] |= 1 << (h % 8);
        }
    }

    pub fn might_contain(&self, key: &[u8]) -> bool {
        for i in 0..self.num_hashes {
            let h = self.hash(key, i) % self.num_bits;
            if self.bits[(h / 8) as usize] & (1 << (h % 8)) == 0 {
                return false;
            }
        }
        true
    }

    fn hash(&self, key: &[u8], seed: u32) -> u64 {
        let mut h: u64 = seed as u64 ^ 0x517cc1b727220a95;
        for &b in key {
            h = h.wrapping_mul(0x5bd1e9955bd1e995).wrapping_add(b as u64);
            h ^= h >> 17;
        }
        h
    }

    pub fn build(path: &Path, keys: &[&[u8]]) -> io::Result<Self> {
        let mut filter = Self::new(keys.len().max(1));
        for key in keys {
            filter.insert(key);
        }
        filter.save(path)?;
        Ok(filter)
    }

    pub fn save(&self, path: &Path) -> io::Result<()> {
        let mut file = File::create(path)?;
        file.write_all(BLOOM_MAGIC)?;
        file.write_all(&[BLOOM_VERSION])?;
        file.write_all(&self.num_bits.to_le_bytes())?;
        file.write_all(&self.num_hashes.to_le_bytes())?;
        file.write_all(&self.bits)?;
        Ok(())
    }

    pub fn load(path: &Path) -> io::Result<Self> {
        let mut file = File::open(path)?;
        let mut magic = [0u8; 4];
        file.read_exact(&mut magic)?;
        if &magic != BLOOM_MAGIC {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "bad bloom magic"));
        }
        let mut ver = [0u8; 1];
        file.read_exact(&mut ver)?;
        if ver[0] != BLOOM_VERSION {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "bad bloom version"));
        }
        let mut buf8 = [0u8; 8];
        file.read_exact(&mut buf8)?;
        let num_bits = u64::from_le_bytes(buf8);
        let mut buf4 = [0u8; 4];
        file.read_exact(&mut buf4)?;
        let num_hashes = u32::from_le_bytes(buf4);
        let num_bytes = num_bits.div_ceil(8) as usize;
        let mut bits = vec![0u8; num_bytes];
        file.read_exact(&mut bits)?;
        Ok(Self { bits, num_bits, num_hashes })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn insert_and_check() {
        let mut bf = BloomFilter::new(100);
        bf.insert(b"hello");
        bf.insert(b"world");
        assert!(bf.might_contain(b"hello"));
        assert!(bf.might_contain(b"world"));
        assert!(!bf.might_contain(b"missing"));
    }

    #[test]
    fn save_and_load_roundtrip() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.bloom");
        let keys: Vec<&[u8]> = vec![b"a", b"b", b"c"];
        BloomFilter::build(&path, &keys).unwrap();
        let loaded = BloomFilter::load(&path).unwrap();
        assert!(loaded.might_contain(b"a"));
        assert!(loaded.might_contain(b"b"));
        assert!(loaded.might_contain(b"c"));
        assert!(!loaded.might_contain(b"z"));
    }

    #[test]
    fn empty_filter_rejects_everything() {
        let bf = BloomFilter::new(10);
        assert!(!bf.might_contain(b"anything"));
    }

    #[test]
    fn many_keys_low_false_positive() {
        let mut bf = BloomFilter::new(1000);
        for i in 0..1000u32 {
            bf.insert(&i.to_le_bytes());
        }
        for i in 0..1000u32 {
            assert!(bf.might_contain(&i.to_le_bytes()));
        }
        let mut false_positives = 0;
        for i in 1000..2000u32 {
            if bf.might_contain(&i.to_le_bytes()) {
                false_positives += 1;
            }
        }
        assert!(false_positives < 50, "false positive rate too high: {false_positives}/1000");
    }
}
