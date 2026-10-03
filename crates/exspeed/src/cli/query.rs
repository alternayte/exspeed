use anyhow::{anyhow, Result};
use serde_json::{json, Value};

use crate::cli::client::CliClient;
use crate::cli::format;

fn error_message(resp: &Value) -> String {
    let msg = resp["error"]
        .as_str()
        .unwrap_or("unknown error")
        .to_string();
    match resp["hint"].as_str() {
        Some(h) if !h.is_empty() => format!("{msg} (hint: {h})"),
        _ => msg,
    }
}

/// Run an ExQL statement through the HTTP API.
///
/// Every statement goes to `POST /api/v1/queries`, which runs bounded
/// queries and handles `CREATE STREAM/TABLE`, `DROP STREAM/TABLE/QUERY`
/// and `PAUSE/RESUME QUERY`. `--continuous` posts to
/// `/api/v1/queries/continuous`, which only accepts `CREATE …`.
pub async fn run(client: &CliClient, sql: &str, continuous: bool, json_output: bool) -> Result<()> {
    let body = json!({ "sql": sql });
    let path = if continuous {
        "/api/v1/queries/continuous"
    } else {
        "/api/v1/queries"
    };
    let (status, resp) = client.post(path, &body).await?;
    if !(200..300).contains(&status) {
        return Err(anyhow!("query failed: {}", error_message(&resp)));
    }

    if json_output {
        println!("{}", serde_json::to_string_pretty(&resp)?);
        return Ok(());
    }

    if resp.get("rows").is_some() {
        let (columns, rows) = format::extract_table_data(&resp);
        let execution_time_ms = resp["execution_time_ms"].as_u64().unwrap_or(0);
        println!(
            "{}",
            format::format_table(&columns, &rows, execution_time_ms)
        );
        if resp["truncated"].as_bool() == Some(true) {
            println!("(result truncated; add a LIMIT or raise EXSPEED_QUERY_MAX_ROWS)");
        }
        return Ok(());
    }
    if let Some(id) = resp["query_id"].as_str() {
        println!(
            "Query {id}: {} {} ({})",
            resp["kind"].as_str().unwrap_or(""),
            resp["name"].as_str().unwrap_or(""),
            resp["status"].as_str().unwrap_or("")
        );
        return Ok(());
    }
    if resp["status"] == "dropped" {
        println!(
            "Dropped {} {}",
            resp["kind"].as_str().unwrap_or(""),
            resp["name"].as_str().unwrap_or("")
        );
        return Ok(());
    }
    println!("{}", serde_json::to_string_pretty(&resp)?);
    Ok(())
}
