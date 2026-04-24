use anyhow::{anyhow, Result};
use serde_json::json;

use crate::cli::client::CliClient;
use crate::cli::format;

/// Run a SQL query against the HTTP API.
///
/// - `continuous`: POST to /api/v1/queries/continuous and print the query_id.
/// - bounded: POST to /api/v1/queries, extract columns/rows, print table or JSON.
pub async fn run(client: &CliClient, sql: &str, continuous: bool, json_output: bool) -> Result<()> {
    let body = json!({ "sql": sql });
    let upper = sql.trim().to_uppercase();

    // Route CREATE INDEX to the indexes API.
    if upper.starts_with("CREATE INDEX") {
        let (status, resp) = client.post("/api/v1/indexes", &body).await?;
        if !(200..300).contains(&status) {
            let msg = resp["error"]
                .as_str()
                .unwrap_or("unknown error")
                .to_string();
            return Err(anyhow!("failed to create index: {msg}"));
        }
        if json_output {
            println!("{}", serde_json::to_string_pretty(&resp)?);
        } else {
            println!(
                "Index created: {}",
                resp["name"].as_str().unwrap_or("?")
            );
        }
        return Ok(());
    }

    // Route DROP INDEX to the indexes API.
    if upper.starts_with("DROP INDEX") {
        let name = sql
            .trim()
            .split_whitespace()
            .nth(2)
            .unwrap_or("")
            .trim_matches('"');
        let (status, resp) = client
            .delete_parsed(&format!("/api/v1/indexes/{name}"))
            .await?;
        if !(200..300).contains(&status) {
            let msg = resp["error"]
                .as_str()
                .unwrap_or("unknown error")
                .to_string();
            return Err(anyhow!("failed to drop index: {msg}"));
        }
        if json_output {
            println!("{}", serde_json::to_string_pretty(&resp)?);
        } else {
            println!("Index dropped: {name}");
        }
        return Ok(());
    }

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
