use std::fs::File;
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Args;
use flate2::write::GzEncoder;
use flate2::Compression;
use tracing::info;

#[derive(Args)]
pub struct SnapshotArgs {
    /// Data directory to snapshot. Must NOT be in use by a running server.
    #[arg(long, default_value = "./exspeed-data")]
    pub data_dir: PathBuf,

    /// Output file path (will be a gzipped tarball).
    #[arg(long)]
    pub output: PathBuf,
}

/// Snapshot an offline data directory into a `.tar.gz`.
///
/// Acquires the same exclusive lock used by `exspeed server`, so attempting
/// to snapshot a running server's data directory fails fast. To snapshot a
/// running server, stop it first.
pub async fn run(args: SnapshotArgs) -> Result<()> {
    let data_dir = args.data_dir.clone();
    let output = args.output.clone();

    // Acquire the lock — held for the duration of the tar operation.
    let lock = crate::cli::server_lock::acquire_data_dir_lock(&data_dir)?;

    info!(
        data_dir = %data_dir.display(),
        output = %output.display(),
        "starting snapshot"
    );

    let result = tokio::task::spawn_blocking(move || -> Result<u64> {
        let out_file = File::create(&output)
            .with_context(|| format!("creating output file {}", output.display()))?;
        let gz = GzEncoder::new(out_file, Compression::default());
        let mut tar = tar::Builder::new(gz);
        tar.follow_symlinks(false);

        // Skip the lock file + runtime probes — they are not stream data.
        for entry in std::fs::read_dir(&data_dir)
            .with_context(|| format!("reading {}", data_dir.display()))?
        {
            let entry = entry?;
            let name = entry.file_name();
            if name == ".exspeed.lock" || name == ".readyz-probe" {
                continue;
            }
            let path = entry.path();
            if path.is_dir() {
                tar.append_dir_all(&name, &path)
                    .with_context(|| format!("adding {} to archive", path.display()))?;
            } else {
                tar.append_path_with_name(&path, &name)
                    .with_context(|| format!("adding {} to archive", path.display()))?;
            }
        }

        let gz = tar.into_inner()?;
        let out = gz.finish()?;
        Ok(out.metadata()?.len())
    })
    .await
    .context("snapshot task panicked")??;

    drop(lock);

    info!(bytes = result, "snapshot complete");
    Ok(())
}
