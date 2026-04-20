// Built in Task 4

mod file_tests;
mod trait_tests;

mod memory_tests {
    use super::trait_tests;
    use crate::memory::MemoryStorage;

    #[tokio::test]
    async fn create_and_append() {
        trait_tests::test_create_and_append(&MemoryStorage::new()).await;
    }

    #[tokio::test]
    async fn append_and_read_back() {
        trait_tests::test_append_and_read_back(&MemoryStorage::new()).await;
    }

    #[tokio::test]
    async fn sequential_offsets() {
        trait_tests::test_sequential_offsets(&MemoryStorage::new()).await;
    }

    #[tokio::test]
    async fn read_range() {
        trait_tests::test_read_range(&MemoryStorage::new()).await;
    }

    #[tokio::test]
    async fn read_empty_stream() {
        trait_tests::test_read_empty_stream(&MemoryStorage::new()).await;
    }

    #[tokio::test]
    async fn read_past_end() {
        trait_tests::test_read_past_end(&MemoryStorage::new()).await;
    }

    #[tokio::test]
    async fn stream_not_found() {
        trait_tests::test_stream_not_found(&MemoryStorage::new()).await;
    }

    #[tokio::test]
    async fn stream_already_exists() {
        trait_tests::test_stream_already_exists(&MemoryStorage::new()).await;
    }

    #[tokio::test]
    async fn timestamps_increasing() {
        trait_tests::test_timestamps_increasing(&MemoryStorage::new()).await;
    }

    #[tokio::test]
    async fn seek_by_time() {
        trait_tests::test_seek_by_time(&MemoryStorage::new()).await;
    }

    #[tokio::test]
    async fn stream_bounds() {
        trait_tests::test_stream_bounds(&MemoryStorage::new()).await;
    }

    #[tokio::test]
    async fn trim_up_to() {
        trait_tests::test_trim_up_to(&MemoryStorage::new()).await;
    }

    #[tokio::test]
    async fn delete_stream() {
        trait_tests::test_delete_stream(&MemoryStorage::new()).await;
    }

    #[tokio::test]
    async fn truncate_from() {
        trait_tests::test_truncate_from(&MemoryStorage::new()).await;
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

    #[tokio::test]
    async fn create_and_append() {
        let (s, _d) = make_storage();
        trait_tests::test_create_and_append(&s).await;
    }

    #[tokio::test]
    async fn append_and_read_back() {
        let (s, _d) = make_storage();
        trait_tests::test_append_and_read_back(&s).await;
    }

    #[tokio::test]
    async fn sequential_offsets() {
        let (s, _d) = make_storage();
        trait_tests::test_sequential_offsets(&s).await;
    }

    #[tokio::test]
    async fn read_range() {
        let (s, _d) = make_storage();
        trait_tests::test_read_range(&s).await;
    }

    #[tokio::test]
    async fn read_empty_stream() {
        let (s, _d) = make_storage();
        trait_tests::test_read_empty_stream(&s).await;
    }

    #[tokio::test]
    async fn read_past_end() {
        let (s, _d) = make_storage();
        trait_tests::test_read_past_end(&s).await;
    }

    #[tokio::test]
    async fn stream_not_found() {
        let (s, _d) = make_storage();
        trait_tests::test_stream_not_found(&s).await;
    }

    #[tokio::test]
    async fn stream_already_exists() {
        let (s, _d) = make_storage();
        trait_tests::test_stream_already_exists(&s).await;
    }

    #[tokio::test]
    async fn timestamps_increasing() {
        let (s, _d) = make_storage();
        trait_tests::test_timestamps_increasing(&s).await;
    }

    #[tokio::test]
    async fn seek_by_time() {
        let (s, _d) = make_storage();
        trait_tests::test_seek_by_time(&s).await;
    }

    #[tokio::test]
    async fn stream_bounds() {
        let (s, _d) = make_storage();
        trait_tests::test_stream_bounds(&s).await;
    }

    #[tokio::test]
    async fn trim_up_to() {
        let (s, _d) = make_storage();
        trait_tests::test_trim_up_to(&s).await;
    }

    #[tokio::test]
    async fn delete_stream() {
        let (s, _d) = make_storage();
        trait_tests::test_delete_stream(&s).await;
    }

    #[tokio::test]
    async fn truncate_from() {
        let (s, _d) = make_storage();
        trait_tests::test_truncate_from(&s).await;
    }
}
