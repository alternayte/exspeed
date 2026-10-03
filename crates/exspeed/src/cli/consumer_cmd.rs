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

    let columns = [
        "name",
        "stream",
        "ack_floor",
        "unacked",
        "lag",
        "subscribers",
    ]
    .map(String::from)
    .to_vec();
    let num = |v: &serde_json::Value| v.as_u64().map(|n| n.to_string()).unwrap_or_default();
    let rows: Vec<Vec<String>> = consumers
        .iter()
        .map(|c| {
            vec![
                c["spec"]["name"].as_str().unwrap_or("").to_string(),
                c["spec"]["stream"].as_str().unwrap_or("").to_string(),
                num(&c["ack_floor"]),
                num(&c["num_unacked"]),
                num(&c["lag"]),
                num(&c["subscribers"]),
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

    let spec = &resp["spec"];
    println!("Consumer: {}", spec["name"].as_str().unwrap_or(name));
    println!("  Stream:        {}", spec["stream"].as_str().unwrap_or(""));
    if let Some(f) = spec["filter_subjects"].as_array().filter(|f| !f.is_empty()) {
        let f: Vec<&str> = f.iter().filter_map(|v| v.as_str()).collect();
        println!("  Filters:       {}", f.join(", "));
    }
    for (label, key) in [
        ("Next offset", "next_offset"),
        ("Ack floor", "ack_floor"),
        ("Unacked", "num_unacked"),
        ("Waiting redel.", "num_waiting"),
        ("Lag", "lag"),
        ("Subscribers", "subscribers"),
        ("Pull waiters", "pull_waiters"),
    ] {
        if let Some(v) = resp[key].as_u64() {
            println!("  {label:<14} {v}");
        }
    }
    if let Some(dlq) = spec["dlq_stream"].as_str() {
        println!("  DLQ stream:    {dlq}");
    }
    Ok(())
}
