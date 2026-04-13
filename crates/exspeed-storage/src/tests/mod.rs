// Built in Task 4

mod file_tests;
mod trait_tests;

mod memory_tests {
    use super::trait_tests;
    use crate::memory::MemoryStorage;

    #[test]
    fn create_and_append() {
        trait_tests::test_create_and_append(&MemoryStorage::new());
    }

    #[test]
    fn append_and_read_back() {
        trait_tests::test_append_and_read_back(&MemoryStorage::new());
    }

    #[test]
    fn sequential_offsets() {
        trait_tests::test_sequential_offsets(&MemoryStorage::new());
    }

    #[test]
    fn read_range() {
        trait_tests::test_read_range(&MemoryStorage::new());
    }

    #[test]
    fn read_empty_stream() {
        trait_tests::test_read_empty_stream(&MemoryStorage::new());
    }

    #[test]
    fn read_past_end() {
        trait_tests::test_read_past_end(&MemoryStorage::new());
    }

    #[test]
    fn stream_not_found() {
        trait_tests::test_stream_not_found(&MemoryStorage::new());
    }

    #[test]
    fn stream_already_exists() {
        trait_tests::test_stream_already_exists(&MemoryStorage::new());
    }

    #[test]
    fn timestamps_increasing() {
        trait_tests::test_timestamps_increasing(&MemoryStorage::new());
    }

    #[test]
    fn seek_by_time() {
        trait_tests::test_seek_by_time(&MemoryStorage::new());
    }
}

mod file_trait_tests {
    use super::trait_tests;
    use crate::file::FileStorage;
    use tempfile::TempDir;

    fn make_storage() -> (FileStorage, TempDir) {
        let dir = TempDir::new().unwrap();
        let storage = FileStorage::new(dir.path()).unwrap();
        (storage, dir) // keep TempDir alive so directory isn't deleted
    }

    #[test]
    fn create_and_append() {
        let (s, _d) = make_storage();
        trait_tests::test_create_and_append(&s);
    }

    #[test]
    fn append_and_read_back() {
        let (s, _d) = make_storage();
        trait_tests::test_append_and_read_back(&s);
    }

    #[test]
    fn sequential_offsets() {
        let (s, _d) = make_storage();
        trait_tests::test_sequential_offsets(&s);
    }

    #[test]
    fn read_range() {
        let (s, _d) = make_storage();
        trait_tests::test_read_range(&s);
    }

    #[test]
    fn read_empty_stream() {
        let (s, _d) = make_storage();
        trait_tests::test_read_empty_stream(&s);
    }

    #[test]
    fn read_past_end() {
        let (s, _d) = make_storage();
        trait_tests::test_read_past_end(&s);
    }

    #[test]
    fn stream_not_found() {
        let (s, _d) = make_storage();
        trait_tests::test_stream_not_found(&s);
    }

    #[test]
    fn stream_already_exists() {
        let (s, _d) = make_storage();
        trait_tests::test_stream_already_exists(&s);
    }

    #[test]
    fn timestamps_increasing() {
        let (s, _d) = make_storage();
        trait_tests::test_timestamps_increasing(&s);
    }

    #[test]
    fn seek_by_time() {
        let (s, _d) = make_storage();
        trait_tests::test_seek_by_time(&s);
    }
}
