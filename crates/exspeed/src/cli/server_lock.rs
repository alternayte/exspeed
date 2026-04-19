use std::fs::{File, OpenOptions};
use std::path::Path;

use anyhow::{anyhow, Context, Result};
use fs4::fs_std::FileExt;

/// Acquire an exclusive advisory lock on `{data_dir}/.exspeed.lock`.
///
/// The returned `File` must be kept alive for the duration of the process —
/// dropping it releases the lock. The lock prevents two `exspeed server`
/// processes from opening the same data directory concurrently, which would
/// corrupt segment files and the WAL.
pub fn acquire_data_dir_lock(data_dir: &Path) -> Result<File> {
    std::fs::create_dir_all(data_dir)
        .with_context(|| format!("creating data dir {}", data_dir.display()))?;

    let lock_path = data_dir.join(".exspeed.lock");
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(&lock_path)
        .with_context(|| format!("opening lock file {}", lock_path.display()))?;

    let acquired = file.try_lock_exclusive().with_context(|| {
        format!(
            "querying advisory lock on {} for data directory {}",
            lock_path.display(),
            data_dir.display(),
        )
    })?;

    if !acquired {
        return Err(anyhow!(
            "data directory {} is already in use by another exspeed process \
             (could not acquire {})",
            data_dir.display(),
            lock_path.display(),
        ));
    }

    Ok(file)
}
