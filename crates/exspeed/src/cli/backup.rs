//! `exspeed backup` (download an online backup over HTTP) and
//! `exspeed restore` (unpack one into an offline data directory).

use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use clap::Args;
use exspeed_storage::file::backup::{
    read_manifest, restore_backup, BackupManifest, RestoreOptions, MANIFEST_NAME,
};

#[derive(Args)]
pub struct BackupArgs {
    /// HTTP API base URL of the server to back up (defaults to the global
    /// `--server`).
    #[arg(long)]
    pub url: Option<String>,

    /// Bearer token of a global admin.
    #[arg(long, env = "EXSPEED_AUTH_TOKEN", hide_env_values = true)]
    pub token: Option<String>,

    /// Where to write the tar archive. Written to `<output>.partial` first
    /// and renamed once complete and verified.
    #[arg(long, short)]
    pub output: PathBuf,
}

#[derive(Args)]
pub struct RestoreArgs {
    /// Backup archive written by `exspeed backup`.
    #[arg(long, short)]
    pub input: PathBuf,

    /// Data directory to restore into. No server may be running on it.
    #[arg(long)]
    pub data_dir: PathBuf,

    /// Restore into a non-empty data directory: its streams, connector,
    /// connection and ExQL directories are replaced (other files such as
    /// credentials.toml are kept).
    #[arg(long)]
    pub force: bool,
}

fn summarize(m: &BackupManifest) {
    println!(
        "backup created {} by exspeed {}: {} stream(s)",
        m.created_at,
        m.server_version,
        m.streams.len()
    );
    for s in &m.streams {
        println!(
            "  {:<32} offsets [{}, {})  {} records  {} bytes",
            s.name, s.earliest_offset, s.next_offset, s.records, s.bytes
        );
    }
    if !m.config_dirs.is_empty() {
        println!("  config: {}", m.config_dirs.join(", "));
    }
}

/// Read the whole archive: the manifest must come first and every entry
/// must be complete (a connection cut mid-download fails here).
fn verify_archive(path: &Path) -> Result<BackupManifest> {
    let manifest = read_manifest(BufReader::new(File::open(path)?))
        .with_context(|| format!("{}: invalid backup", path.display()))?;
    let mut archive = tar::Archive::new(BufReader::new(File::open(path)?));
    let mut entries = 0usize;
    for e in archive.entries()? {
        let mut e = e.with_context(|| format!("{}: truncated archive", path.display()))?;
        std::io::copy(&mut e, &mut std::io::sink())
            .with_context(|| format!("{}: truncated archive", path.display()))?;
        entries += 1;
    }
    if entries == 0 {
        bail!("{}: empty archive (no {MANIFEST_NAME})", path.display());
    }
    Ok(manifest)
}

/// `exspeed backup`: stream `GET /api/v1/backup` into a file.
pub async fn backup(args: BackupArgs, default_url: &str) -> Result<()> {
    let base = args
        .url
        .as_deref()
        .unwrap_or(default_url)
        .trim_end_matches('/');
    let url = format!("{base}/api/v1/backup");
    let mut builder = reqwest::Client::builder();
    if std::env::var("EXSPEED_INSECURE_SKIP_VERIFY")
        .ok()
        .as_deref()
        == Some("1")
    {
        builder = builder.danger_accept_invalid_certs(true);
    }
    let client = builder.build()?;
    let mut req = client.get(&url);
    if let Some(t) = args.token.as_deref().filter(|t| !t.is_empty()) {
        req = req.bearer_auth(t);
    }
    let mut resp = req
        .send()
        .await
        .with_context(|| format!("failed to connect to {url}"))?;
    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        let msg = serde_json::from_str::<serde_json::Value>(&body)
            .ok()
            .and_then(|v| v["error"].as_str().map(str::to_string))
            .unwrap_or(body);
        bail!("GET {url} returned {status}: {msg}");
    }

    let partial = {
        let mut p = args.output.clone().into_os_string();
        p.push(".partial");
        PathBuf::from(p)
    };
    let result = async {
        let mut out = BufWriter::new(
            File::create(&partial).with_context(|| format!("creating {}", partial.display()))?,
        );
        let mut bytes = 0u64;
        while let Some(chunk) = resp
            .chunk()
            .await
            .context("download interrupted; the backup is incomplete")?
        {
            out.write_all(&chunk)?;
            bytes += chunk.len() as u64;
        }
        let file = out.into_inner().map_err(|e| e.into_error())?;
        file.sync_all()?;
        let manifest = verify_archive(&partial)?;
        std::fs::rename(&partial, &args.output)?;
        Ok::<_, anyhow::Error>((manifest, bytes))
    }
    .await;
    let (manifest, bytes) = match result {
        Ok(v) => v,
        Err(e) => {
            let _ = std::fs::remove_file(&partial);
            return Err(e);
        }
    };
    summarize(&manifest);
    println!("wrote {} ({bytes} bytes)", args.output.display());
    Ok(())
}

/// `exspeed restore`: offline restore into a data directory.
pub async fn restore(args: RestoreArgs) -> Result<()> {
    // Same lock as `exspeed server`: refuses to run against a live server
    // and keeps one from starting mid-restore.
    let lock = crate::cli::server_lock::acquire_data_dir_lock(&args.data_dir)?;
    let input = args.input.clone();
    let data_dir = args.data_dir.clone();
    let force = args.force;
    let manifest = tokio::task::spawn_blocking(move || -> Result<BackupManifest> {
        let f = BufReader::new(
            File::open(&input).with_context(|| format!("opening {}", input.display()))?,
        );
        restore_backup(f, &data_dir, RestoreOptions { force })
            .with_context(|| format!("restoring {} into {}", input.display(), data_dir.display()))
    })
    .await
    .context("restore task panicked")??;
    drop(lock);
    summarize(&manifest);
    println!(
        "restored into {}; start the server with --data-dir {}",
        args.data_dir.display(),
        args.data_dir.display()
    );
    Ok(())
}
