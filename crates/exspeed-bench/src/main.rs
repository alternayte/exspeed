use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};

use exspeed_bench::host::Host;
use exspeed_bench::profile::Profile;
use exspeed_bench::report::*;
use exspeed_bench::{renderer, scenarios};

#[derive(Parser)]
#[command(name = "exspeed-bench", about = "Exspeed benchmark harness")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(clap::ValueEnum, Clone, Copy, Debug)]
enum ProfileArg {
    Reference,
    Local,
}

impl ProfileArg {
    fn to_profile(self) -> Profile {
        match self {
            ProfileArg::Reference => Profile::reference(),
            ProfileArg::Local => Profile::local(),
        }
    }
}

#[derive(Subcommand)]
enum Cmd {
    /// Producer ceiling benchmark
    Publish(ScenarioArgs),
    /// End-to-end latency benchmark
    Latency(ScenarioArgs),
    /// Fan-out scaling benchmark
    Fanout(ScenarioArgs),
    /// ExQL continuous query throughput benchmark
    Exql(ExqlArgs),
    /// Run every scenario and write a single JSON result
    All(AllArgs),
    /// Render a JSON result into Markdown (README snippet + BENCHMARKS.md)
    Render {
        input: PathBuf,
        #[arg(long)]
        out: Option<PathBuf>,
    },
}

#[derive(clap::Args)]
struct ScenarioArgs {
    #[arg(long, default_value = "localhost:5933")]
    server: String,
    #[arg(long, value_enum, default_value_t = ProfileArg::Local)]
    profile: ProfileArg,
    #[arg(long)]
    sku: Option<String>,
    #[arg(long)]
    storage: Option<String>,
    #[arg(long)]
    output: Option<PathBuf>,
}

#[derive(clap::Args)]
struct ExqlArgs {
    #[arg(long, default_value = "localhost:5933")]
    server: String,
    #[arg(long, default_value = "http://localhost:8080")]
    api: String,
    #[arg(long, value_enum, default_value_t = ProfileArg::Local)]
    profile: ProfileArg,
    #[arg(long)]
    sku: Option<String>,
    #[arg(long)]
    storage: Option<String>,
    #[arg(long, default_value_t = 5_000)]
    low: u64,
    #[arg(long, default_value_t = 500_000)]
    high: u64,
    #[arg(long, default_value_t = 6)]
    iterations: u32,
    #[arg(long)]
    output: Option<PathBuf>,
}

#[derive(clap::Args)]
struct AllArgs {
    #[arg(long, default_value = "localhost:5933")]
    server: String,
    #[arg(long, default_value = "http://localhost:8080")]
    api: String,
    #[arg(long, value_enum, default_value_t = ProfileArg::Local)]
    profile: ProfileArg,
    #[arg(long)]
    sku: Option<String>,
    #[arg(long)]
    storage: Option<String>,
    #[arg(long)]
    output: PathBuf,
}

fn git_sha() -> String {
    std::process::Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_owned())
        .unwrap_or_else(|| "unknown".into())
}

fn new_result(profile: &Profile, sku: Option<String>, storage: Option<String>) -> BenchResult {
    BenchResult {
        schema_version: SCHEMA_VERSION,
        profile: profile.kind,
        exspeed_version: env!("CARGO_PKG_VERSION").into(),
        git_sha: git_sha(),
        host: Host::detect(sku, storage),
        run_timestamp: chrono::Utc::now(),
        scenarios: Scenarios::default(),
    }
}

fn write_output(result: &BenchResult, path: Option<PathBuf>) -> Result<()> {
    match path {
        Some(p) => {
            if let Some(parent) = p.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(&p, serde_json::to_vec_pretty(result)?)?;
            println!("wrote {}", p.display());
        }
        None => println!("{}", serde_json::to_string_pretty(result)?),
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Publish(a) => {
            let profile = a.profile.to_profile();
            let mut result = new_result(&profile, a.sku, a.storage);
            result.scenarios.publish = scenarios::publish::run(&a.server, &profile).await?;
            write_output(&result, a.output)
        }
        Cmd::Latency(a) => {
            let profile = a.profile.to_profile();
            let mut result = new_result(&profile, a.sku, a.storage);
            result.scenarios.latency = Some(scenarios::latency::run(&a.server, &profile).await?);
            write_output(&result, a.output)
        }
        Cmd::Fanout(a) => {
            let profile = a.profile.to_profile();
            let mut result = new_result(&profile, a.sku, a.storage);
            result.scenarios.fanout = scenarios::fanout::run(&a.server, &profile).await?;
            write_output(&result, a.output)
        }
        Cmd::Exql(a) => {
            let profile = a.profile.to_profile();
            let mut result = new_result(&profile, a.sku, a.storage);
            result.scenarios.exql = Some(
                scenarios::exql::run(&a.server, &a.api, &profile, a.low, a.high, a.iterations)
                    .await?,
            );
            write_output(&result, a.output)
        }
        Cmd::All(a) => {
            let profile = a.profile.to_profile();
            let mut result = new_result(&profile, a.sku, a.storage);
            result.scenarios.publish = scenarios::publish::run(&a.server, &profile).await?;
            result.scenarios.latency =
                Some(scenarios::latency::run(&a.server, &profile).await?);
            result.scenarios.fanout = scenarios::fanout::run(&a.server, &profile).await?;
            result.scenarios.exql = Some(
                scenarios::exql::run(&a.server, &a.api, &profile, 5_000, 500_000, 6).await?,
            );
            write_output(&result, Some(a.output))
        }
        Cmd::Render { input, out } => {
            let bytes = std::fs::read(&input)?;
            let r: BenchResult = serde_json::from_slice(&bytes)?;
            match out {
                Some(path) => {
                    let body = renderer::benchmarks_md(&r);
                    std::fs::write(&path, body)?;
                    println!("wrote {}", path.display());
                    match renderer::readme_snippet_strict(&r) {
                        Ok(snippet) => {
                            println!("\n---\n");
                            println!("{}", snippet);
                        }
                        Err(e) => eprintln!("note: {e}"),
                    }
                    Ok(())
                }
                None => {
                    println!("{}", renderer::benchmarks_md(&r));
                    // Local profile: skip silently when streaming to stdout
                    if let Ok(snippet) = renderer::readme_snippet_strict(&r) {
                        println!("\n---\n");
                        println!("{}", snippet);
                    }
                    Ok(())
                }
            }
        }
    }
}
