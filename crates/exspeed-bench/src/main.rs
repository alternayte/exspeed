use anyhow::Result;
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "exspeed-bench", about = "Exspeed benchmark harness")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Producer ceiling benchmark
    Publish,
    /// End-to-end latency benchmark
    Latency,
    /// Fan-out scaling benchmark
    Fanout,
    /// ExQL continuous query throughput benchmark
    Exql,
    /// Run every scenario and write a single JSON result
    All,
    /// Render a JSON result into Markdown (README snippet + BENCHMARKS.md)
    Render,
}

fn main() -> Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Publish | Cmd::Latency | Cmd::Fanout | Cmd::Exql | Cmd::All | Cmd::Render => {
            anyhow::bail!("not implemented yet")
        }
    }
}
