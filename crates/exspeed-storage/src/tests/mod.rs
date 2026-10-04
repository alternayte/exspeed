// Built in Task 4

mod backup_tests;
mod bench;
mod compaction_tests;
mod durability_tests;
mod enospc_tests;
mod file_tests;
mod limits_tests;
mod property_tests;
mod read_tests;
mod trait_tests;
mod util;

mod memory_tests {
    use super::trait_tests;
    use crate::memory::MemoryStorage;

    // `exact_retention = true` — MemoryStorage trims record-exact.
    const EXACT_RETENTION: bool = true;

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
    async fn append_honors_timestamp_override() {
        trait_tests::test_append_honors_timestamp_override(&MemoryStorage::new()).await;
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
    async fn trim_up_to_earliest_is_noop() {
        trait_tests::test_trim_up_to_earliest_is_noop(&MemoryStorage::new()).await;
    }

    #[tokio::test]
    async fn trim_up_to_drops_earlier_records() {
        trait_tests::test_trim_up_to_drops_earlier_records(&MemoryStorage::new(), EXACT_RETENTION)
            .await;
    }

    #[tokio::test]
    async fn trim_up_to_past_latest_still_advances() {
        trait_tests::test_trim_up_to_past_latest_still_advances(
            &MemoryStorage::new(),
            EXACT_RETENTION,
        )
        .await;
    }

    #[tokio::test]
    async fn read_below_earliest_returns_out_of_range() {
        trait_tests::test_read_below_earliest_returns_out_of_range(&MemoryStorage::new()).await;
    }

    #[tokio::test]
    async fn truncate_from_drops_later_records() {
        trait_tests::test_truncate_from_drops_later_records(&MemoryStorage::new()).await;
    }

    #[tokio::test]
    async fn truncate_from_zero_empties() {
        trait_tests::test_truncate_from_zero_empties(&MemoryStorage::new()).await;
    }

    #[tokio::test]
    async fn truncate_from_at_next_is_noop() {
        trait_tests::test_truncate_from_at_next_is_noop(&MemoryStorage::new()).await;
    }

    #[tokio::test]
    async fn stream_bounds_after_trim_and_truncate() {
        trait_tests::test_stream_bounds_after_trim_and_truncate(
            &MemoryStorage::new(),
            EXACT_RETENTION,
        )
        .await;
    }

    #[tokio::test]
    async fn delete_then_recreate_resets_bounds() {
        trait_tests::test_delete_then_recreate_resets_bounds(&MemoryStorage::new()).await;
    }

    #[tokio::test]
    async fn delete_stream() {
        trait_tests::test_delete_stream(&MemoryStorage::new()).await;
    }

    #[tokio::test]
    async fn append_at() {
        trait_tests::test_append_at(&MemoryStorage::new()).await;
    }

    #[tokio::test]
    async fn watch_appends() {
        trait_tests::test_watch_appends(&MemoryStorage::new()).await;
    }
}

mod file_trait_tests {
    use super::trait_tests;
    use crate::file::FileStorage;
    use tempfile::TempDir;

    // FileStorage's `trim_up_to` is record-exact: it moves the log start
    // offset (and deletes the segments entirely below it).
    const EXACT_RETENTION: bool = true;

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
    async fn append_honors_timestamp_override() {
        let (s, _d) = make_storage();
        trait_tests::test_append_honors_timestamp_override(&s).await;
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
    async fn trim_up_to_earliest_is_noop() {
        let (s, _d) = make_storage();
        trait_tests::test_trim_up_to_earliest_is_noop(&s).await;
    }

    #[tokio::test]
    async fn trim_up_to_drops_earlier_records() {
        let (s, _d) = make_storage();
        trait_tests::test_trim_up_to_drops_earlier_records(&s, EXACT_RETENTION).await;
    }

    #[tokio::test]
    async fn trim_up_to_past_latest_still_advances() {
        let (s, _d) = make_storage();
        trait_tests::test_trim_up_to_past_latest_still_advances(&s, EXACT_RETENTION).await;
    }

    #[tokio::test]
    async fn read_below_earliest_returns_out_of_range() {
        let (s, _d) = make_storage();
        trait_tests::test_read_below_earliest_returns_out_of_range(&s).await;
    }

    #[tokio::test]
    async fn truncate_from_drops_later_records() {
        let (s, _d) = make_storage();
        trait_tests::test_truncate_from_drops_later_records(&s).await;
    }

    #[tokio::test]
    async fn truncate_from_zero_empties() {
        let (s, _d) = make_storage();
        trait_tests::test_truncate_from_zero_empties(&s).await;
    }

    #[tokio::test]
    async fn truncate_from_at_next_is_noop() {
        let (s, _d) = make_storage();
        trait_tests::test_truncate_from_at_next_is_noop(&s).await;
    }

    #[tokio::test]
    async fn stream_bounds_after_trim_and_truncate() {
        let (s, _d) = make_storage();
        trait_tests::test_stream_bounds_after_trim_and_truncate(&s, EXACT_RETENTION).await;
    }

    #[tokio::test]
    async fn delete_then_recreate_resets_bounds() {
        let (s, _d) = make_storage();
        trait_tests::test_delete_then_recreate_resets_bounds(&s).await;
    }

    #[tokio::test]
    async fn delete_stream() {
        let (s, _d) = make_storage();
        trait_tests::test_delete_stream(&s).await;
    }

    #[tokio::test]
    async fn append_at() {
        let (s, _d) = make_storage();
        trait_tests::test_append_at(&s).await;
    }

    #[tokio::test]
    async fn append_at_survives_restart() {
        let (s, d) = make_storage();
        trait_tests::test_append_at(&s).await;
        drop(s);
        let s = FileStorage::open(d.path()).unwrap();
        let name = exspeed_common::StreamName::try_from("test-append-at").unwrap();
        let offs: Vec<u64> =
            exspeed_streams::StorageEngine::read(&s, &name, exspeed_common::Offset(0), 100)
                .await
                .unwrap()
                .iter()
                .map(|r| r.offset.0)
                .collect();
        assert_eq!(offs, vec![0, 1, 5, 6, 7]);
        let (o, _) = exspeed_streams::StorageEngine::append(
            &s,
            &name,
            &exspeed_streams::Record {
                value: bytes::Bytes::from_static(b"z"),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(o.0, 8);
    }

    #[tokio::test]
    async fn watch_appends() {
        let (s, _d) = make_storage();
        trait_tests::test_watch_appends(&s).await;
    }
}
