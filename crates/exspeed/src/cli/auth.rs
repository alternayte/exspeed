//! `exspeed auth` — credential management helpers.
//!
//! - `gen-token`   Generate a random 32-byte hex token; token to stdout,
//!                 sha256 digest to stderr.
//! - `hash`        Read a raw token from stdin (trimmed), print sha256 to stdout.
//! - `lint FILE`   Validate a credentials.toml file without starting the server.
//! - `whoami`      GET /api/v1/whoami with the CLI's configured token.

use std::io::Read;
use std::path::{Path, PathBuf};

use clap::Subcommand;
use rand::RngCore;

use crate::cli::client::CliClient;

#[derive(Debug, Subcommand)]
pub enum AuthCmd {
    /// Generate a fresh 32-byte token (hex) on stdout; its sha256 on stderr.
    GenToken,
    /// Read a raw token from stdin (trimmed), print its sha256 hex.
    Hash,
    /// Validate a credentials.toml without starting the server.
    Lint {
        /// Path to the credentials.toml file.
        file: PathBuf,
    },
    /// Call GET /api/v1/whoami with the CLI's configured token.
    Whoami,
}

/// Dispatch entry point. `client` is only consulted for `whoami`.
pub async fn run(cmd: AuthCmd, client: &CliClient) -> anyhow::Result<()> {
    match cmd {
        AuthCmd::GenToken => gen_token(),
        AuthCmd::Hash => hash_stdin(),
        AuthCmd::Lint { file } => lint(&file),
        AuthCmd::Whoami => whoami(client).await,
    }
}

fn gen_token() -> anyhow::Result<()> {
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    let token_hex: String = bytes.iter().map(|b| format!("{:02x}", b)).collect();
    // Token → stdout (capturable); hash → stderr (human confirmation).
    println!("{}", token_hex);
    let hash = exspeed_common::auth::sha256_hex(token_hex.as_bytes());
    eprintln!("{}", hash);
    Ok(())
}

fn hash_stdin() -> anyhow::Result<()> {
    let mut buf = String::new();
    std::io::stdin().read_to_string(&mut buf)?;
    // Trim so a trailing newline from `echo -n`-less pipelines or an
    // interactive paste doesn't alter the digest.
    let token = buf.trim();
    let hash = exspeed_common::auth::sha256_hex(token.as_bytes());
    println!("{}", hash);
    Ok(())
}

fn lint(path: &Path) -> anyhow::Result<()> {
    match exspeed_common::auth::CredentialStore::build(Some(path), None) {
        Ok(store) => {
            let (files, legacy) = store.source_breakdown();
            eprintln!(
                "ok: {files} credentials{} loaded",
                if legacy { " (+ legacy-admin)" } else { "" }
            );
            Ok(())
        }
        Err(e) => {
            eprintln!("error: {e}");
            std::process::exit(1);
        }
    }
}

async fn whoami(client: &CliClient) -> anyhow::Result<()> {
    let resp = client.get("/api/v1/whoami").await?;
    println!("{}", serde_json::to_string_pretty(&resp)?);
    Ok(())
}
