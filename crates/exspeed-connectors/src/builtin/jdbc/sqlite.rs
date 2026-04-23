use super::dialect::{ColumnSpec, Dialect};

pub struct SqliteDialect;

impl Dialect for SqliteDialect {
    fn quote_ident(&self, name: &str) -> String {
        format!("\"{}\"", name)
    }
    fn placeholder(&self, _n: usize) -> String {
        "?".to_string()
    }
    fn json_blob_type(&self) -> &'static str { "TEXT" }
    fn timestamptz_type(&self) -> &'static str { "TEXT" }
    fn double_type(&self) -> &'static str { "REAL" }

    fn create_table_blob_sql(&self, table: &str) -> String {
        let t = self.quote_ident(table);
        format!(
            "CREATE TABLE IF NOT EXISTS {t} (\n\
             \"offset\" INTEGER PRIMARY KEY,\n\
             \"ingested_at\" TEXT NOT NULL DEFAULT (datetime('now')),\n\
             \"subject\" TEXT,\n\
             \"key\" TEXT,\n\
             \"value\" TEXT NOT NULL\n\
             )"
        )
    }

    fn create_table_typed_sql(
        &self,
        table: &str,
        cols: &[ColumnSpec],
        pk_cols: &[&str],
    ) -> String {
        use crate::builtin::jdbc::dialect::JsonType;
        let t = self.quote_ident(table);
        let col_lines: Vec<String> = cols
            .iter()
            .map(|c| {
                // SQLite has dynamic typing but we annotate for clarity and
                // for tools that inspect schema.
                let sql_type = match c.json_type {
                    JsonType::Text => "TEXT",
                    JsonType::Bigint => "INTEGER",
                    JsonType::Double => self.double_type(),
                    JsonType::Boolean => "INTEGER",  // 0/1
                    JsonType::Timestamptz => self.timestamptz_type(),
                    JsonType::Jsonb => self.json_blob_type(),
                };
                let nullability = if c.nullable { "" } else { " NOT NULL" };
                format!("    {} {}{}", self.quote_ident(&c.name), sql_type, nullability)
            })
            .collect();
        let mut body = col_lines.join(",\n");
        if !pk_cols.is_empty() {
            let pks: Vec<String> = pk_cols.iter().map(|c| self.quote_ident(c)).collect();
            body.push_str(&format!(",\n    PRIMARY KEY ({})", pks.join(", ")));
        }
        format!("CREATE TABLE IF NOT EXISTS {t} (\n{body}\n)")
    }

    fn insert_sql(&self, table: &str, cols: &[&str]) -> String {
        let t = self.quote_ident(table);
        let col_sql: Vec<String> = cols.iter().map(|c| self.quote_ident(c)).collect();
        let placeholders: Vec<String> = (0..cols.len()).map(|_| "?".to_string()).collect();
        format!(
            "INSERT INTO {t} ({}) VALUES ({})",
            col_sql.join(", "),
            placeholders.join(", ")
        )
    }

    fn upsert_sql(&self, table: &str, cols: &[&str], keys: &[&str]) -> String {
        // SQLite 3.24+ supports ON CONFLICT ... DO UPDATE SET with `excluded`
        // reference. Same shape as Postgres.
        let insert = self.insert_sql(table, cols);
        let key_sql: Vec<String> = keys.iter().map(|k| self.quote_ident(k)).collect();
        let update_cols: Vec<&&str> = cols.iter().filter(|c| !keys.contains(c)).collect();
        if update_cols.is_empty() {
            format!("{insert} ON CONFLICT ({}) DO NOTHING", key_sql.join(", "))
        } else {
            let set: Vec<String> = update_cols
                .iter()
                .map(|c| {
                    let q = self.quote_ident(c);
                    format!("{q} = excluded.{q}")
                })
                .collect();
            format!(
                "{insert} ON CONFLICT ({}) DO UPDATE SET {}",
                key_sql.join(", "),
                set.join(", ")
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builtin::jdbc::dialect::JsonType;

    fn spec(name: &str, t: JsonType, nullable: bool) -> ColumnSpec {
        ColumnSpec { name: name.to_string(), json_type: t, nullable }
    }

    #[test]
    fn placeholder_uses_question_mark() {
        assert_eq!(SqliteDialect.placeholder(1), "?");
        assert_eq!(SqliteDialect.placeholder(7), "?");
    }

    #[test]
    fn blob_create_table_shape() {
        let sql = SqliteDialect.create_table_blob_sql("events");
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS \"events\""));
        assert!(sql.contains("\"offset\" INTEGER PRIMARY KEY"));
        assert!(sql.contains("\"ingested_at\" TEXT"));
        assert!(sql.contains("\"value\" TEXT NOT NULL"));
    }

    #[test]
    fn typed_create_table_shape() {
        let cols = vec![
            spec("id", JsonType::Bigint, false),
            spec("email", JsonType::Text, true),
            spec("price", JsonType::Double, false),
        ];
        let sql = SqliteDialect.create_table_typed_sql("t", &cols, &["id"]);
        assert!(sql.contains("\"id\" INTEGER NOT NULL"));
        assert!(sql.contains("\"email\" TEXT"));
        assert!(sql.contains("\"price\" REAL NOT NULL"));
        assert!(sql.contains("PRIMARY KEY (\"id\")"));
    }

    #[test]
    fn insert_sql_uses_question_mark_placeholders() {
        let sql = SqliteDialect.insert_sql("t", &["a", "b", "c"]);
        assert_eq!(sql, "INSERT INTO \"t\" (\"a\", \"b\", \"c\") VALUES (?, ?, ?)");
    }

    #[test]
    fn upsert_sql_uses_on_conflict_excluded() {
        let sql = SqliteDialect.upsert_sql("t", &["id", "email"], &["id"]);
        assert!(sql.contains("ON CONFLICT (\"id\") DO UPDATE SET"));
        assert!(sql.contains("\"email\" = excluded.\"email\""));
        assert!(!sql.contains("\"id\" = excluded.\"id\""));
    }

    #[test]
    fn upsert_sql_all_keys_is_do_nothing() {
        let sql = SqliteDialect.upsert_sql("t", &["id"], &["id"]);
        assert!(sql.contains("ON CONFLICT (\"id\") DO NOTHING"));
    }
}
