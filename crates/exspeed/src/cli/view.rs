use anyhow::Result;

use crate::cli::client::CliClient;
use crate::cli::format;

/// List all materialized views via the HTTP API.
pub async fn list(client: &CliClient, json_output: bool) -> Result<()> {
    let resp = client.get("/api/v1/views").await?;

    if json_output {
        println!("{}", serde_json::to_string_pretty(&resp)?);
        return Ok(());
    }

    let views = resp
        .as_array()
        .or_else(|| resp["views"].as_array())
        .cloned()
        .unwrap_or_default();

    let columns = vec![
        "name".to_string(),
        "row_count".to_string(),
        "columns".to_string(),
    ];

    let rows: Vec<Vec<String>> = views
        .iter()
        .map(|v| {
            let name = v["name"].as_str().unwrap_or("").to_string();
            let row_count = v["row_count"]
                .as_u64()
                .map(|n| n.to_string())
                .unwrap_or_else(|| v["row_count"].as_str().unwrap_or("").to_string());
            let col_list = if let Some(arr) = v["columns"].as_array() {
                arr.iter()
                    .filter_map(|c| c.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            } else {
                v["columns"].as_str().unwrap_or("").to_string()
            };
            vec![name, row_count, col_list]
        })
        .collect();

    let table = format::format_table(&columns, &rows, 0);
    println!("{}", table);

    Ok(())
}

/// Query a materialized view by name and display results as a table.
pub async fn get(client: &CliClient, name: &str, json_output: bool) -> Result<()> {
    let path = format!("/api/v1/views/{}", name);
    let resp = client.get(&path).await?;

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
