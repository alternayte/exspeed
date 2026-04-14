pub mod client;
pub mod connector;
pub mod format;
pub mod server;

use clap::Parser;

#[derive(Parser)]
#[command(name = "exspeed", about = "Lightweight stream processing platform")]
pub enum Cli {
    /// Start the exspeed server
    Server(server::ServerArgs),
    /// Manage and validate connector configs
    Connector(connector::ConnectorCommand),
}
