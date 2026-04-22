use std::path::{Path, PathBuf};

use anyhow::{anyhow, Result};

use exspeed_common::types::StreamName;
use exspeed_connectors::builtin::{create_sink, create_source};
use exspeed_connectors::config::ConnectorConfig;
use exspeed_connectors::transform::Transform;

// Known source plugins (excluding http_webhook which is passive).
const KNOWN_SOURCE_PLUGINS: &[&str] = &[
    "http_webhook",
    "postgres_outbox",
    "postgres_cdc",
    "rabbitmq",
    "http_poll",
];

// Known sink plugins.
const KNOWN_SINK_PLUGINS: &[&str] = &["http_sink", "jdbc", "rabbitmq", "s3"];

#[derive(clap::Args)]
pub struct ConnectorCommand {
    #[command(subcommand)]
    pub action: ConnectorAction,
}

#[derive(clap::Subcommand)]
pub enum ConnectorAction {
    /// Validate a connector config file
    Validate { path: PathBuf },
    /// Validate + connect + fetch sample record
    DryRun { path: PathBuf },
}

pub async fn run(cmd: ConnectorCommand) -> Result<()> {
    match cmd.action {
        ConnectorAction::Validate { path } => validate(&path).await,
        ConnectorAction::DryRun { path } => dry_run(&path).await,
    }
}

async fn validate(path: &Path) -> Result<()> {
    // Step 1: Load TOML
    let mut config =
        ConnectorConfig::load_toml(path).map_err(|e| anyhow!("failed to load config: {e}"))?;
    println!("✓ Config syntax valid");

    // Step 2: Resolve env vars
    config.resolve_env_vars();
    println!("✓ Environment variables resolved");

    // Step 3: Check plugin is known
    let plugin = config.plugin.clone();
    let all_plugins: Vec<&str> = KNOWN_SOURCE_PLUGINS
        .iter()
        .chain(KNOWN_SINK_PLUGINS.iter())
        .copied()
        .collect();
    if !all_plugins.contains(&plugin.as_str()) {
        return Err(anyhow!("unknown plugin: '{plugin}'"));
    }
    println!("✓ Plugin '{plugin}' recognized");

    // Step 4: Validate stream name
    let stream = config.stream.clone();
    StreamName::try_from(stream.as_str()).map_err(|e| anyhow!("invalid stream name: {e}"))?;
    println!("✓ Stream name valid");

    // Step 5: Validate transform SQL if non-empty
    if !config.transform_sql.is_empty() {
        Transform::compile(&config.transform_sql)
            .map_err(|e| anyhow!("transform SQL error: {e}"))?;
        println!("✓ Transform SQL valid");
    }

    println!("\n✓ Config is valid");
    Ok(())
}

async fn dry_run(path: &Path) -> Result<()> {
    // Step 1: Run validate first
    validate(path).await?;

    // Reload config (validate already checked it's loadable)
    let mut config =
        ConnectorConfig::load_toml(path).map_err(|e| anyhow!("failed to load config: {e}"))?;
    config.resolve_env_vars();

    let plugin = config.plugin.clone();
    let connector_type = config.connector_type.clone();

    match connector_type.as_str() {
        "source" => {
            if plugin == "http_webhook" {
                println!("ℹ Webhook is passive — no connection to test");
            } else {
                let mut source = create_source(&plugin, &config)
                    .map_err(|e| anyhow!("failed to create source connector: {e}"))?;
                source
                    .start(None)
                    .await
                    .map_err(|e| anyhow!("failed to start source connector: {e}"))?;

                let batch = source
                    .poll(1)
                    .await
                    .map_err(|e| anyhow!("failed to poll source connector: {e}"))?;

                if batch.records.is_empty() {
                    println!("✓ Connected (no records available yet)");
                } else {
                    let record = &batch.records[0];
                    let payload = String::from_utf8_lossy(&record.value);
                    println!("✓ Sample record:");
                    println!("  subject: {}", record.subject);
                    println!("  value:   {payload}");
                }

                source
                    .stop()
                    .await
                    .map_err(|e| anyhow!("failed to stop source connector: {e}"))?;
            }
        }
        "sink" => {
            let (metrics, _registry) = exspeed_common::Metrics::new();
            let mut sink = create_sink(&plugin, &config, std::sync::Arc::new(metrics))
                .map_err(|e| anyhow!("failed to create sink connector: {e}"))?;
            sink.start()
                .await
                .map_err(|e| anyhow!("failed to start sink connector: {e}"))?;
            println!("✓ Connected");
            sink.stop()
                .await
                .map_err(|e| anyhow!("failed to stop sink connector: {e}"))?;
        }
        other => {
            return Err(anyhow!(
                "unknown connector type '{other}' (expected 'source' or 'sink')"
            ));
        }
    }

    Ok(())
}
