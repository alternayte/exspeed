use clap::Parser;

use exspeed::cli;
use exspeed::cli::client::CliClient;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = cli::Cli::parse();
    // The server's log settings come from its resolved config; other
    // commands use the environment.
    let server_args = match &args.command {
        cli::Command::Server(flags) => {
            let resolved = exspeed::config::resolve(flags)?;
            exspeed::log_format::init_logging_with(
                resolved.log_format.as_deref(),
                resolved.log_level.as_deref().or(Some("info")),
            );
            exspeed::config::validate(&resolved)?;
            Some(resolved)
        }
        _ => {
            exspeed::log_format::init_logging();
            None
        }
    };
    let client = CliClient::new(&args.server);
    let server_url = args.server.clone();
    let json = args.json;

    match args.command {
        cli::Command::Server(_) => cli::server::run(server_args.expect("resolved above")).await,
        cli::Command::Config(c) => exspeed::config::run_config_command(c),
        cli::Command::Connector(c) => cli::connector::run(c).await,
        cli::Command::Create {
            name,
            retention,
            max_size,
            dedup_window,
            dedup_max_entries,
            limits,
        } => {
            cli::stream::create(
                &client,
                &name,
                &retention,
                &max_size,
                dedup_window.as_deref(),
                dedup_max_entries.as_deref(),
                &limits,
            )
            .await
        }
        cli::Command::UpdateStream {
            name,
            retention,
            max_size,
            dedup_window,
            dedup_max_entries,
            limits,
        } => {
            cli::stream::update(
                &client,
                &name,
                retention.as_deref(),
                max_size.as_deref(),
                dedup_window.as_deref(),
                dedup_max_entries.as_deref(),
                &limits,
            )
            .await
        }
        cli::Command::Delete { name, force } => cli::stream::delete(&client, &name, force).await,
        cli::Command::Streams => cli::stream::list(&client, json).await,
        cli::Command::Info { name } => cli::stream::info(&client, &name, json).await,
        cli::Command::Pub {
            stream,
            data,
            subject,
            key,
            msg_id,
        } => {
            cli::publish::run(
                &client,
                &stream,
                &data,
                subject.as_deref(),
                key.as_deref(),
                msg_id.as_deref(),
            )
            .await
        }
        cli::Command::Tail {
            stream,
            last,
            no_follow,
            subject,
            from_beginning,
        } => {
            cli::tail::run(
                &client,
                &stream,
                last,
                no_follow,
                subject.as_deref(),
                from_beginning,
                json,
            )
            .await
        }
        cli::Command::Consumers => cli::consumer_cmd::list(&client, json).await,
        cli::Command::ConsumerInfo { name } => cli::consumer_cmd::info(&client, &name, json).await,
        cli::Command::Query { sql, continuous } => {
            cli::query::run(&client, &sql, continuous, json).await
        }
        cli::Command::Views => cli::view::list(&client, json).await,
        cli::Command::View { name } => cli::view::get(&client, &name, json).await,
        cli::Command::Connectors => cli::stream::list_connectors(&client, json).await,
        cli::Command::Snapshot(a) => cli::snapshot::run(a).await,
        cli::Command::Backup(a) => cli::backup::backup(a, &server_url).await,
        cli::Command::Restore(a) => cli::backup::restore(a).await,
        cli::Command::Healthcheck { url, timeout } => {
            let url = match url {
                Some(u) => u,
                None => exspeed::config::probe_url(&exspeed::config::resolve(
                    &exspeed::config::ServeArgs::default(),
                )?),
            };
            healthcheck(&url, timeout).await
        }
        cli::Command::Auth { cmd } => cli::auth::run(cmd, &client).await,
    }
}

/// Probe `url` and exit non-zero unless it answers 200. Accepts self-signed
/// certificates: a health probe checks liveness, not identity.
async fn healthcheck(url: &str, timeout_secs: u64) -> anyhow::Result<()> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(timeout_secs))
        .danger_accept_invalid_certs(true)
        .build()?;
    let status = client.get(url).send().await?.status();
    if status.is_success() {
        Ok(())
    } else {
        anyhow::bail!("{url} returned {status}")
    }
}
