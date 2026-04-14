use anyhow::{anyhow, Result};
use serde_json::json;

use crate::cli::client::CliClient;
use crate::cli::format;

/// Run a SQL query against the HTTP API.
///
/// - `continuous`: POST to /api/v1/queries/continuous and print the query_id.
/// - bounded: POST to /api/v1/queries, extract columns/rows, print table or JSON.
pub async fn run(
    client: &CliClient,
    sql: &str,
    continuous: bool,
    json_output: bool,
) -> Result<()> {
    let body = json!({ "sql": sql });

    if continuous {
        let (status, resp) = client.post("/api/v1/queries/continuous", &body).await?;
        if !(200..300).contains(&status) {
            let msg = resp["error"]
                .as_str()
                .unwrap_or("unknown error")
                .to_string();
            return Err(anyhow!("failed to start continuous query: {msg}"));
        }

        if json_output {
            println!("{}", serde_json::to_string_pretty(&resp)?);
        } else {
            let query_id = resp["query_id"]
                .as_str()
                .or_else(|| resp["id"].as_str())
                .unwrap_or("(unknown)");
            println!("Continuous query started: {}", query_id);
        }
        return Ok(());
    }

    // Bounded query
    let (status, resp) = client.post("/api/v1/queries", &body).await?;
    if !(200..300).contains(&status) {
        let msg = resp["error"]
            .as_str()
            .unwrap_or("unknown error")
            .to_string();
        return Err(anyhow!("failed to run query: {msg}"));
    }

    if json_output {
        println!("{}", serde_json::to_string_pretty(&resp)?);
        return Ok(());
    }

    let (columns, rows) = format::extract_table_data(&resp);
    let execution_time_ms = resp["execution_time_ms"].as_u64().unwrap_or(0);
    let table = format::format_table(&columns, &rows, execution_time_ms);
    println!("{}", table);

    Ok(())
}
