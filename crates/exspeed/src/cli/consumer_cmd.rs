use anyhow::Result;

use crate::cli::client::CliClient;
use crate::cli::format;

/// List all consumers via the HTTP API.
pub async fn list(client: &CliClient, json_output: bool) -> Result<()> {
    let resp = client.get("/api/v1/consumers").await?;

    if json_output {
        println!("{}", serde_json::to_string_pretty(&resp)?);
        return Ok(());
    }

    let consumers = resp
        .as_array()
        .or_else(|| resp["consumers"].as_array())
        .cloned()
        .unwrap_or_default();

    let columns = vec![
        "name".to_string(),
        "stream".to_string(),
        "group".to_string(),
        "offset".to_string(),
    ];

    let rows: Vec<Vec<String>> = consumers
        .iter()
        .map(|c| {
            vec![
                c["name"].as_str().unwrap_or("").to_string(),
                c["stream"].as_str().unwrap_or("").to_string(),
                c["group"].as_str().unwrap_or("").to_string(),
                c["offset"]
                    .as_u64()
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| c["offset"].as_str().unwrap_or("").to_string()),
            ]
        })
        .collect();

    let table = format::format_table(&columns, &rows, 0);
    println!("{}", table);

    Ok(())
}

/// Get detailed info about a single consumer.
pub async fn info(client: &CliClient, name: &str, json_output: bool) -> Result<()> {
    let path = format!("/api/v1/consumers/{}", name);
    let resp = client.get(&path).await?;

    if json_output {
        println!("{}", serde_json::to_string_pretty(&resp)?);
        return Ok(());
    }

    println!("Consumer: {}", resp["name"].as_str().unwrap_or(name));
    if let Some(stream) = resp["stream"].as_str() {
        println!("  Stream:  {}", stream);
    }
    if let Some(group) = resp["group"].as_str() {
        println!("  Group:   {}", group);
    }
    if let Some(offset) = resp["offset"].as_u64() {
        println!("  Offset:  {}", offset);
    }
    if let Some(lag) = resp["lag"].as_u64() {
        println!("  Lag:     {}", lag);
    }

    Ok(())
}
