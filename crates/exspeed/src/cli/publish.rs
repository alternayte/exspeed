use crate::cli::client::CliClient;
use anyhow::Result;

pub async fn run(
    client: &CliClient,
    stream: &str,
    data: &str,
    subject: Option<&str>,
    key: Option<&str>,
) -> Result<()> {
    // Parse data as JSON (if it's valid JSON), otherwise wrap as a JSON string
    let json_data: serde_json::Value =
        serde_json::from_str(data).unwrap_or(serde_json::Value::String(data.to_string()));

    let mut body = serde_json::json!({ "data": json_data });
    if let Some(s) = subject {
        body["subject"] = serde_json::Value::String(s.to_string());
    }
    if let Some(k) = key {
        body["key"] = serde_json::Value::String(k.to_string());
    }

    let (status, resp) = client
        .post(&format!("/api/v1/streams/{stream}/publish"), &body)
        .await?;
    if status == 201 || status == 200 {
        let offset = resp.get("offset").and_then(|v| v.as_u64()).unwrap_or(0);
        println!("Published to '{}' at offset {}", stream, offset);
    } else {
        let msg = resp
            .get("error")
            .and_then(|e| e.as_str())
            .unwrap_or("unknown error");
        anyhow::bail!("Publish failed: {}", msg);
    }
    Ok(())
}
