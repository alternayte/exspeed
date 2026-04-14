use serde_json::Value;

/// Render an ASCII table (psql-style) with box-drawing characters.
///
/// `columns` — header names.
/// `rows` — each inner Vec corresponds to one row, same length as `columns`.
/// `execution_time_ms` — shown in the footer.
pub fn format_table(columns: &[String], rows: &[Vec<String>], execution_time_ms: u64) -> String {
    if columns.is_empty() {
        return format!("0 row(s) ({execution_time_ms}ms)");
    }

    // Compute max width per column (at least header width).
    let mut widths: Vec<usize> = columns.iter().map(|c| c.len()).collect();
    for row in rows {
        for (i, cell) in row.iter().enumerate() {
            if i < widths.len() && cell.len() > widths[i] {
                widths[i] = cell.len();
            }
        }
    }

    let mut out = String::new();

    // Top border: ┌──┬──┐
    out.push('┌');
    for (i, w) in widths.iter().enumerate() {
        for _ in 0..w + 2 {
            out.push('─');
        }
        if i + 1 < widths.len() {
            out.push('┬');
        }
    }
    out.push('┐');
    out.push('\n');

    // Header row
    out.push('│');
    for (i, col) in columns.iter().enumerate() {
        out.push(' ');
        out.push_str(&format!("{:<width$}", col, width = widths[i]));
        out.push(' ');
        out.push('│');
    }
    out.push('\n');

    // Separator: ├──┼──┤
    out.push('├');
    for (i, w) in widths.iter().enumerate() {
        for _ in 0..w + 2 {
            out.push('─');
        }
        if i + 1 < widths.len() {
            out.push('┼');
        }
    }
    out.push('┤');
    out.push('\n');

    // Data rows
    for row in rows {
        out.push('│');
        for (i, cell) in row.iter().enumerate() {
            let w = if i < widths.len() { widths[i] } else { cell.len() };
            out.push(' ');
            out.push_str(&format!("{:<width$}", cell, width = w));
            out.push(' ');
            out.push('│');
        }
        out.push('\n');
    }

    // Bottom border: └──┴──┘
    out.push('└');
    for (i, w) in widths.iter().enumerate() {
        for _ in 0..w + 2 {
            out.push('─');
        }
        if i + 1 < widths.len() {
            out.push('┴');
        }
    }
    out.push('┘');
    out.push('\n');

    // Footer
    let row_count = rows.len();
    out.push_str(&format!("{row_count} row(s) ({execution_time_ms}ms)"));

    out
}

/// Format a single tail record as a log-style line.
///
/// Format: `[timestamp] subject key=value payload`
///
/// - `timestamp`: converted from unix nanoseconds to `seconds.millis`
/// - `payload`: truncated to 200 characters
pub fn format_tail_line(record: &Value) -> String {
    let timestamp_nanos = record["timestamp"].as_u64().unwrap_or(0);
    let secs = timestamp_nanos / 1_000_000_000;
    let millis = (timestamp_nanos % 1_000_000_000) / 1_000_000;

    let subject = record["subject"].as_str().unwrap_or("");
    let key = record["key"].as_str().unwrap_or("");

    let payload = match &record["payload"] {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    };

    let truncated_payload = if payload.len() > 200 {
        format!("{}...", &payload[..200])
    } else {
        payload
    };

    format!(
        "[{secs}.{millis:03}] {subject} key={key} {truncated_payload}"
    )
}

/// Extract columns and rows from a query result JSON value.
///
/// Expects the shape: `{ "columns": [...], "rows": [[...], ...] }`.
/// Converts each cell to a display string (null -> "NULL", etc.).
pub fn extract_table_data(result: &Value) -> (Vec<String>, Vec<Vec<String>>) {
    let columns: Vec<String> = match result["columns"].as_array() {
        Some(arr) => arr.iter().map(|v| value_to_display(v)).collect(),
        None => return (vec![], vec![]),
    };

    let rows: Vec<Vec<String>> = match result["rows"].as_array() {
        Some(arr) => arr
            .iter()
            .map(|row| {
                match row.as_array() {
                    Some(cells) => cells.iter().map(|v| value_to_display(v)).collect(),
                    None => vec![],
                }
            })
            .collect(),
        None => vec![],
    };

    (columns, rows)
}

/// Convert a JSON value to a human-friendly display string.
fn value_to_display(v: &Value) -> String {
    match v {
        Value::Null => "NULL".to_string(),
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_format_table_two_columns_two_rows() {
        let columns = vec!["name".to_string(), "age".to_string()];
        let rows = vec![
            vec!["Alice".to_string(), "30".to_string()],
            vec!["Bob".to_string(), "25".to_string()],
        ];
        let table = format_table(&columns, &rows, 42);

        // Verify structure
        assert!(table.contains("┌"));
        assert!(table.contains("┐"));
        assert!(table.contains("├"));
        assert!(table.contains("┤"));
        assert!(table.contains("└"));
        assert!(table.contains("┘"));
        assert!(table.contains("│ name  │ age │"));
        assert!(table.contains("│ Alice │ 30  │"));
        assert!(table.contains("│ Bob   │ 25  │"));
        assert!(table.ends_with("2 row(s) (42ms)"));
    }

    #[test]
    fn test_format_table_empty_rows() {
        let columns = vec!["id".to_string(), "value".to_string()];
        let rows: Vec<Vec<String>> = vec![];
        let table = format_table(&columns, &rows, 7);

        // Should still have header + borders, but no data rows
        assert!(table.contains("│ id │ value │"));
        assert!(table.ends_with("0 row(s) (7ms)"));
    }

    #[test]
    fn test_format_tail_line() {
        let record = json!({
            "timestamp": 1_700_000_000_123_000_000u64,
            "subject": "orders.created",
            "key": "order-42",
            "payload": "hello world"
        });

        let line = format_tail_line(&record);
        assert_eq!(line, "[1700000000.123] orders.created key=order-42 hello world");
    }
}
