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
#[command(name = "exspeed", about = "Lightweight stream processing platform")]
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

#[derive(Subcommand)]
pub enum Command {
    /// Start the exspeed server
    Server(server::ServerArgs),
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
    },
    /// Delete a stream
    Delete {
        /// Stream name
        name: String,
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
    /// Run a SQL query
    Query {
        /// SQL query string
        sql: String,
        /// Run as continuous query
        #[arg(long)]
        continuous: bool,
    },
    /// List all views
    Views,
    /// Show view details
    View {
        /// View name
        name: String,
    },
    /// List all connectors
    Connectors,
    /// Snapshot an offline data directory to a .tar.gz file
    Snapshot(snapshot::SnapshotArgs),
}
