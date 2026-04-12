// Built in Task 4

mod trait_tests;

mod memory_tests {
    use crate::memory::MemoryStorage;
    use super::trait_tests;

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
    fn read_empty_partition() {
        trait_tests::test_read_empty_partition(&MemoryStorage::new());
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
    fn partition_not_found() {
        trait_tests::test_partition_not_found(&MemoryStorage::new());
    }

    #[test]
    fn stream_already_exists() {
        trait_tests::test_stream_already_exists(&MemoryStorage::new());
    }

    #[test]
    fn multiple_partitions() {
        trait_tests::test_multiple_partitions(&MemoryStorage::new());
    }

    #[test]
    fn timestamps_increasing() {
        trait_tests::test_timestamps_increasing(&MemoryStorage::new());
    }
}
