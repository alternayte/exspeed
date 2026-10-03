//! `exspeed connector validate|dry-run <file.toml>`.
//!
//! Both commands build the plugin through the same [`Registry`] the server
//! uses, so a config that validates here is accepted by the server (and a
//! misspelled settings key fails here too).

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{anyhow, Result};

use exspeed_connectors::config::{ConnectorConfig, ConnectorType};
use exspeed_connectors::Registry;

#[derive(clap::Args)]
pub struct ConnectorCommand {
    #[command(subcommand)]
    pub action: ConnectorAction,
}

#[derive(clap::Subcommand)]
pub enum ConnectorAction {
    /// Validate a connector config file (syntax, names, plugin settings).
    Validate { path: PathBuf },
    /// Validate, connect and fetch sample records without side effects
    /// (no replication slot or publication is created, nothing is acked).
    DryRun {
        path: PathBuf,
        /// Maximum number of sample records to print (sources).
        #[arg(long, default_value_t = 3)]
        max: usize,
    },
}

pub async fn run(cmd: ConnectorCommand) -> Result<()> {
    match cmd.action {
        ConnectorAction::Validate { path } => validate(&path).map(drop),
        ConnectorAction::DryRun { path, max } => dry_run(&path, max).await,
    }
}

fn metrics() -> Arc<exspeed_common::Metrics> {
    Arc::new(exspeed_common::Metrics::new().0)
}

fn validate(path: &Path) -> Result<ConnectorConfig> {
    let config =
        ConnectorConfig::load_toml(path).map_err(|e| anyhow!("failed to load config: {e}"))?;
    println!("✓ Config syntax valid");
    let registry = Registry::builtin();
    registry
        .validate(&config, true, metrics())
        .map_err(|e| anyhow!("{e}"))?;
    println!(
        "✓ {} plugin '{}' accepts the settings; stream '{}'",
        config.connector_type, config.plugin, config.stream
    );
    println!("\n✓ Config is valid");
    Ok(config)
}

async fn dry_run(path: &Path, max: usize) -> Result<()> {
    let config = validate(path)?;
    let registry = Registry::builtin();
    let init = registry
        .init(&config, true, metrics())
        .map_err(|e| anyhow!("{e}"))?;

    match config.connector_type {
        ConnectorType::Source if registry.is_passive(&config.plugin) => {
            println!(
                "ℹ '{}' is passive (driven by inbound HTTP); nothing to connect to",
                config.plugin
            );
        }
        ConnectorType::Source => {
            let mut source = registry.create_source(&init).map_err(|e| anyhow!("{e}"))?;
            let records = source
                .dry_run(max.max(1))
                .await
                .map_err(|e| anyhow!("dry run failed: {e}"))?;
            if records.is_empty() {
                println!("✓ Connected (no records available right now)");
            }
            for (i, r) in records.iter().enumerate() {
                println!("✓ Sample record {}:", i + 1);
                println!("  subject: {}", r.subject);
                if let Some(k) = &r.key {
                    println!("  key:     {}", String::from_utf8_lossy(k));
                }
                println!("  value:   {}", String::from_utf8_lossy(&r.value));
            }
        }
        ConnectorType::Sink => {
            let mut sink = registry.create_sink(&init).map_err(|e| anyhow!("{e}"))?;
            sink.start()
                .await
                .map_err(|e| anyhow!("failed to connect: {e}"))?;
            println!("✓ Connected");
            let _ = sink.stop().await;
        }
    }
    Ok(())
}
