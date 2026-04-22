use super::dialect::{ColumnSpec, Dialect};

pub struct PostgresDialect;

impl Dialect for PostgresDialect {
    fn quote_ident(&self, name: &str) -> String {
        format!("\"{}\"", name)
    }
    fn placeholder(&self, n: usize) -> String {
        format!("${}", n)
    }
    fn json_blob_type(&self) -> &'static str { "JSONB" }
    fn timestamptz_type(&self) -> &'static str { "TIMESTAMPTZ" }
    fn double_type(&self) -> &'static str { "DOUBLE PRECISION" }

    fn create_table_blob_sql(&self, table: &str) -> String {
        let t = self.quote_ident(table);
        format!(
            "CREATE TABLE IF NOT EXISTS {t} (\n\
             \"offset\" BIGINT PRIMARY KEY,\n\
             \"ingested_at\" TIMESTAMPTZ NOT NULL DEFAULT NOW(),\n\
             \"subject\" TEXT,\n\
             \"key\" TEXT,\n\
             \"value\" JSONB NOT NULL\n\
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
                let sql_type = match c.json_type {
                    JsonType::Text => "TEXT",
                    JsonType::Bigint => "BIGINT",
                    JsonType::Double => self.double_type(),
                    JsonType::Boolean => "BOOLEAN",
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
        let placeholders: Vec<String> = (1..=cols.len()).map(|i| self.placeholder(i)).collect();
        format!(
            "INSERT INTO {t} ({}) VALUES ({})",
            col_sql.join(", "),
            placeholders.join(", ")
        )
    }

    fn upsert_sql(&self, table: &str, cols: &[&str], keys: &[&str]) -> String {
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
                    format!("{q} = EXCLUDED.{q}")
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
    fn blob_create_table_shape() {
        let sql = PostgresDialect.create_table_blob_sql("events");
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS \"events\""));
        assert!(sql.contains("\"offset\" BIGINT PRIMARY KEY"));
        assert!(sql.contains("\"ingested_at\" TIMESTAMPTZ"));
        assert!(sql.contains("\"value\" JSONB NOT NULL"));
    }

    #[test]
    fn typed_create_table_shape() {
        let cols = vec![
            spec("id", JsonType::Bigint, false),
            spec("email", JsonType::Text, true),
            spec("price", JsonType::Double, false),
            spec("created_at", JsonType::Timestamptz, false),
            spec("payload", JsonType::Jsonb, false),
        ];
        let sql = PostgresDialect.create_table_typed_sql("t", &cols, &["id"]);
        assert!(sql.contains("\"id\" BIGINT NOT NULL"));
        assert!(sql.contains("\"email\" TEXT"));
        assert!(sql.contains("\"price\" DOUBLE PRECISION NOT NULL"));
        assert!(sql.contains("\"created_at\" TIMESTAMPTZ NOT NULL"));
        assert!(sql.contains("\"payload\" JSONB NOT NULL"));
        assert!(sql.contains("PRIMARY KEY (\"id\")"));
    }

    #[test]
    fn typed_create_table_no_pk_when_empty() {
        let cols = vec![spec("id", JsonType::Bigint, false)];
        let sql = PostgresDialect.create_table_typed_sql("t", &cols, &[]);
        assert!(!sql.contains("PRIMARY KEY"), "sql = {}", sql);
    }

    #[test]
    fn insert_sql_uses_numbered_placeholders() {
        let sql = PostgresDialect.insert_sql("t", &["a", "b", "c"]);
        assert_eq!(
            sql,
            "INSERT INTO \"t\" (\"a\", \"b\", \"c\") VALUES ($1, $2, $3)"
        );
    }

    #[test]
    fn upsert_sql_uses_on_conflict_excluded() {
        let sql = PostgresDialect.upsert_sql("t", &["id", "email", "price"], &["id"]);
        assert!(sql.starts_with(
            "INSERT INTO \"t\" (\"id\", \"email\", \"price\") VALUES ($1, $2, $3) ON CONFLICT (\"id\") DO UPDATE SET"
        ), "sql was: {sql}");
        assert!(sql.contains("\"email\" = EXCLUDED.\"email\""));
        assert!(sql.contains("\"price\" = EXCLUDED.\"price\""));
        assert!(!sql.contains("\"id\" = EXCLUDED.\"id\""),
            "PK column must not appear in DO UPDATE SET");
    }

    #[test]
    fn upsert_sql_when_all_cols_are_keys_uses_do_nothing() {
        let sql = PostgresDialect.upsert_sql("t", &["id"], &["id"]);
        assert!(sql.contains("ON CONFLICT (\"id\") DO NOTHING"));
    }
}
