pub mod auth;
pub mod backup;
pub mod client;
pub mod connector;
pub mod consumer_cmd;
pub mod format;
pub mod publish;
pub mod query;
pub mod server;
pub mod server_lock;
pub mod server_tls;
pub mod snapshot;
pub mod stream;
pub mod tail;
pub mod view;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "exspeed",
    version,
    about = "A streaming platform in one binary: durable log, subjects, SQL over streams and connectors"
)]
pub struct Cli {
    /// URL of the exspeed server
    #[arg(
        long,
        global = true,
        env = "EXSPEED_URL",
        default_value = "http://localhost:8080"
    )]
    pub server: String,

    /// Output as JSON
    #[arg(long, global = true)]
    pub json: bool,

    #[command(subcommand)]
    pub command: Command,
}

/// Message limits, lifetimes and the retention policy of a stream. On
/// `update-stream`, only the flags given change.
#[derive(clap::Args, Debug, Default)]
pub struct StreamLimitArgs {
    /// Keep at most this many records (e.g. 10000, 1M); 0 = no limit
    #[arg(long)]
    pub max_msgs: Option<String>,
    /// At the limit: `old` drops the oldest records, `new` rejects new ones
    #[arg(long, value_parser = ["old", "new"])]
    pub discard: Option<String>,
    /// Keep only the newest N records per subject; 0 = no limit
    #[arg(long)]
    pub max_msgs_per_subject: Option<u64>,
    /// Accept a per-record TTL (`exspeed-ttl` header): true or false
    #[arg(long, num_args = 0..=1, default_missing_value = "true")]
    pub allow_msg_ttl: Option<bool>,
    /// TTL of records without their own (e.g. 30s, 5m, 1h); 0 = none
    #[arg(long)]
    pub msg_ttl: Option<String>,
    /// Accept delayed delivery (`exspeed-delay` / `exspeed-deliver-at`): true or false
    #[arg(long, num_args = 0..=1, default_missing_value = "true")]
    pub allow_delayed: Option<bool>,
    /// `limits`, `work_queue` (removed once acked by the one consumer) or
    /// `interest` (removed once every consumer acked)
    #[arg(long, value_parser = ["limits", "work_queue", "interest"])]
    pub retention_policy: Option<String>,
    /// Append core messages (Exspeed or NATS) published to subjects matching
    /// this filter; repeatable, comma-separated, or "" to clear
    #[arg(long, value_delimiter = ',')]
    pub capture: Option<Vec<String>>,
}

#[derive(Subcommand)]
pub enum Command {
    /// Start the exspeed server
    Server(crate::config::ServeArgs),
    /// Inspect and validate server configuration (exspeed.toml)
    #[command(subcommand)]
    Config(crate::config::ConfigCommand),
    /// Manage and validate connector configs
    Connector(connector::ConnectorCommand),
    /// Create a new stream
    Create {
        /// Stream name
        name: String,
        /// Retention period (e.g. 7d, 24h, 30m)
        #[arg(long, default_value = "7d")]
        retention: String,
        /// Max storage size (e.g. 10gb, 256mb)
        #[arg(long, default_value = "10gb")]
        max_size: String,
        /// Dedup window duration (e.g. 10m, 1h, 300s)
        #[arg(long)]
        dedup_window: Option<String>,
        /// Dedup max entries (e.g. 2M, 500k, 100000)
        #[arg(long)]
        dedup_max_entries: Option<String>,
        #[command(flatten)]
        limits: StreamLimitArgs,
    },
    /// Update an existing stream's configuration
    UpdateStream {
        /// Stream name
        name: String,
        /// Retention period (e.g. 7d, 24h, 30m)
        #[arg(long)]
        retention: Option<String>,
        /// Max storage size (e.g. 10gb, 256mb)
        #[arg(long)]
        max_size: Option<String>,
        /// Dedup window duration (e.g. 10m, 1h, 300s)
        #[arg(long)]
        dedup_window: Option<String>,
        /// Dedup max entries (e.g. 2M, 500k, 100000)
        #[arg(long)]
        dedup_max_entries: Option<String>,
        #[command(flatten)]
        limits: StreamLimitArgs,
    },
    /// Delete a stream
    Delete {
        /// Stream name
        name: String,
        /// Cascade through connectors, queries, and consumers that reference this stream.
        #[arg(long)]
        force: bool,
    },
    /// List all streams
    Streams,
    /// Show stream details
    Info {
        /// Stream name
        name: String,
    },
    /// Publish a record to a stream
    Pub {
        /// Target stream
        stream: String,
        /// Record data
        data: String,
        /// Subject/topic
        #[arg(long)]
        subject: Option<String>,
        /// Partition key
        #[arg(long)]
        key: Option<String>,
        /// Idempotency message ID for deduplication
        #[arg(long = "msg-id")]
        msg_id: Option<String>,
    },
    /// Tail records from a stream
    Tail {
        /// Stream to tail
        stream: String,
        /// Show last N records
        #[arg(long)]
        last: Option<usize>,
        /// Don't follow new records
        #[arg(long)]
        no_follow: bool,
        /// Filter by subject
        #[arg(long)]
        subject: Option<String>,
        /// Start from the beginning
        #[arg(long)]
        from_beginning: bool,
    },
    /// List all consumers
    Consumers,
    /// Show consumer details
    ConsumerInfo {
        /// Consumer name
        name: String,
    },
    /// Run an ExQL statement: SELECT, CREATE STREAM/TABLE … AS SELECT,
    /// DROP STREAM/TABLE/QUERY, PAUSE/RESUME QUERY
    Query {
        /// SQL statement
        sql: String,
        /// Only accept CREATE STREAM/TABLE (posts to /api/v1/queries/continuous)
        #[arg(long)]
        continuous: bool,
    },
    /// List materialized tables (CREATE TABLE … AS SELECT)
    Views,
    /// Show the rows of a materialized table
    View {
        /// Table name
        name: String,
    },
    /// List all connectors
    Connectors,
    /// Snapshot an offline data directory to a .tar.gz file
    Snapshot(snapshot::SnapshotArgs),
    /// Download an online backup of a running server (GET /api/v1/backup)
    Backup(backup::BackupArgs),
    /// Restore a backup into a data directory (offline: no server may be
    /// running on it)
    Restore(backup::RestoreArgs),
    /// Exit 0 if the server's readiness probe answers 200 (for Docker
    /// HEALTHCHECK and other probes that can only run a command)
    Healthcheck {
        /// Probe URL [default: /readyz on the server's api_bind port, https
        /// when TLS is configured, resolved from $EXSPEED_CONFIG and the
        /// environment like `exspeed server` does]
        #[arg(long, env = "EXSPEED_HEALTHCHECK_URL")]
        url: Option<String>,
        /// Timeout in seconds
        #[arg(long, default_value_t = 3)]
        timeout: u64,
    },
    /// Credential management helpers (gen-token, hash, lint, whoami)
    Auth {
        #[command(subcommand)]
        cmd: auth::AuthCmd,
    },
}
