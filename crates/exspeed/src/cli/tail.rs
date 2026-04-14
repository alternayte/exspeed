use crate::cli::client::CliClient;
use crate::cli::format;
use anyhow::Result;

pub async fn run(
    client: &CliClient,
    stream: &str,
    last: Option<usize>,
    no_follow: bool,
    subject: Option<&str>,
    from_beginning: bool,
    json_output: bool,
) -> Result<()> {
    // Get current head offset
    let info = client.get(&format!("/api/v1/streams/{stream}")).await?;
    let head = info
        .get("head_offset")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);

    let mut current_offset: i64 = if from_beginning {
        0
    } else if let Some(n) = last {
        (head - n as i64).max(0)
    } else {
        head // start from latest
    };

    // Subject filter for SQL WHERE clause
    let subject_clause = subject
        .map(|s| format!(" AND subject_matches(subject, '{}')", s))
        .unwrap_or_default();

    loop {
        let sql = format!(
            "SELECT offset, timestamp, subject, key, payload FROM \"{}\" WHERE offset >= {}{} ORDER BY offset LIMIT 100",
            stream, current_offset, subject_clause
        );

        let body = serde_json::json!({"sql": sql});
        let (status, result) = client.post("/api/v1/queries", &body).await?;

        if status != 200 {
            let msg = result
                .get("error")
                .and_then(|e| e.as_str())
                .unwrap_or("query failed");
            anyhow::bail!("Tail error: {}", msg);
        }

        let columns = result.get("columns").and_then(|c| c.as_array());
        let rows = result.get("rows").and_then(|r| r.as_array());

        if let (Some(cols), Some(rows)) = (columns, rows) {
            for row_arr in rows {
                if let Some(cells) = row_arr.as_array() {
                    // Build a record object from columns + cells
                    let mut record = serde_json::Map::new();
                    for (i, col) in cols.iter().enumerate() {
                        if let (Some(col_name), Some(val)) = (col.as_str(), cells.get(i)) {
                            record.insert(col_name.to_string(), val.clone());
                        }
                    }
                    let record_json = serde_json::Value::Object(record);

                    if json_output {
                        println!("{}", serde_json::to_string(&record_json)?);
                    } else {
                        println!("{}", format::format_tail_line(&record_json));
                    }

                    // Advance offset
                    if let Some(offset) = cells.first().and_then(|v| v.as_i64()) {
                        current_offset = offset + 1;
                    }
                }
            }
        }

        // Exit conditions
        if no_follow {
            break;
        }

        // Sleep before next poll
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    Ok(())
}
