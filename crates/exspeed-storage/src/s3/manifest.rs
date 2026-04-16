use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub stream: String,
    pub segments: Vec<SegmentEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentEntry {
    pub base_offset: u64,
    pub end_offset: u64,
    pub size_bytes: u64,
    pub record_count: u64,
    pub first_timestamp: u64,
    pub last_timestamp: u64,
    pub uploaded_at: String,
}

impl Manifest {
    pub fn new(stream: &str) -> Self {
        Self {
            stream: stream.to_string(),
            segments: Vec::new(),
        }
    }

    pub fn add_segment(&mut self, entry: SegmentEntry) {
        self.segments.push(entry);
    }

    pub fn find_segment(&self, offset: u64) -> Option<&SegmentEntry> {
        self.segments
            .iter()
            .find(|e| offset >= e.base_offset && offset <= e.end_offset)
    }

    pub fn to_json(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec_pretty(self)
    }

    pub fn from_json(data: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entry(base: u64, end: u64) -> SegmentEntry {
        SegmentEntry {
            base_offset: base,
            end_offset: end,
            size_bytes: 1024,
            record_count: end - base + 1,
            first_timestamp: 1_000_000,
            last_timestamp: 2_000_000,
            uploaded_at: "2026-04-16T00:00:00Z".to_string(),
        }
    }

    #[test]
    fn manifest_roundtrip() {
        let mut manifest = Manifest::new("orders");
        manifest.add_segment(make_entry(0, 99));
        manifest.add_segment(make_entry(100, 199));

        let json = manifest.to_json().expect("serialization failed");
        let restored = Manifest::from_json(&json).expect("deserialization failed");

        assert_eq!(restored.stream, "orders");
        assert_eq!(restored.segments.len(), 2);
        assert_eq!(restored.segments[0].base_offset, 0);
        assert_eq!(restored.segments[0].end_offset, 99);
        assert_eq!(restored.segments[1].base_offset, 100);
        assert_eq!(restored.segments[1].end_offset, 199);
    }

    #[test]
    fn find_segment_by_offset() {
        let mut manifest = Manifest::new("events");
        manifest.add_segment(make_entry(0, 99));
        manifest.add_segment(make_entry(100, 199));
        manifest.add_segment(make_entry(200, 299));

        let found = manifest.find_segment(150).expect("should find segment");
        assert_eq!(found.base_offset, 100);
        assert_eq!(found.end_offset, 199);

        let found_first = manifest.find_segment(0).expect("should find first segment");
        assert_eq!(found_first.base_offset, 0);

        let found_last = manifest.find_segment(299).expect("should find last segment");
        assert_eq!(found_last.base_offset, 200);

        let not_found = manifest.find_segment(300);
        assert!(not_found.is_none());
    }

    #[test]
    fn empty_manifest_find_returns_none() {
        let manifest = Manifest::new("empty-stream");
        assert!(manifest.find_segment(0).is_none());
        assert!(manifest.find_segment(42).is_none());
    }
}
