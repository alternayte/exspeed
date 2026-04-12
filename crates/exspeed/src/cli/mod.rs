pub mod server;

use clap::Parser;

#[derive(Parser)]
#[command(name = "exspeed", about = "Lightweight stream processing platform")]
pub enum Cli {
    /// Start the exspeed server
    Server(server::ServerArgs),
}
