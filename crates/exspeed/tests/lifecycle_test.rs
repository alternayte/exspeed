use std::path::PathBuf;
use tempfile::tempdir;

#[test]
fn second_server_open_on_same_data_dir_fails() {
    let tmp = tempdir().unwrap();
    let data_dir: PathBuf = tmp.path().to_path_buf();

    let _lock1 = exspeed::cli::server_lock::acquire_data_dir_lock(&data_dir)
        .expect("first lock should succeed");

    let err = exspeed::cli::server_lock::acquire_data_dir_lock(&data_dir)
        .expect_err("second lock should be rejected");

    let msg = format!("{err:#}");
    assert!(
        msg.contains("already in use") || msg.contains("locked"),
        "unexpected error: {msg}"
    );
}
