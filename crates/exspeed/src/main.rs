use clap::Parser;

use exspeed::cli;
use exspeed::cli::client::CliClient;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    exspeed::log_format::init_logging();

    let args = cli::Cli::parse();
    let client = CliClient::new(&args.server);
    let json = args.json;

    match args.command {
        cli::Command::Server(a) => cli::server::run(a).await,
        cli::Command::Connector(c) => cli::connector::run(c).await,
        cli::Command::Create {
            name,
            retention,
            max_size,
        } => cli::stream::create(&client, &name, &retention, &max_size).await,
        cli::Command::Delete { name } => cli::stream::delete(&client, &name).await,
        cli::Command::Streams => cli::stream::list(&client, json).await,
        cli::Command::Info { name } => cli::stream::info(&client, &name, json).await,
        cli::Command::Pub {
            stream,
            data,
            subject,
            key,
        } => cli::publish::run(&client, &stream, &data, subject.as_deref(), key.as_deref()).await,
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
    }
}
