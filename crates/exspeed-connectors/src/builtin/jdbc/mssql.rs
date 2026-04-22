use super::dialect::{ColumnSpec, Dialect};

pub struct MssqlDialect;

impl Dialect for MssqlDialect {
    fn quote_ident(&self, name: &str) -> String {
        format!("[{}]", name)
    }
    fn placeholder(&self, n: usize) -> String {
        format!("@P{}", n)
    }
    fn json_blob_type(&self) -> &'static str { "NVARCHAR(MAX)" }
    fn timestamptz_type(&self) -> &'static str { "DATETIMEOFFSET(6)" }
    fn double_type(&self) -> &'static str { "FLOAT(53)" }

    fn create_table_blob_sql(&self, table: &str) -> String {
        let t_lit = format!("[{}]", table);
        let t_q = self.quote_ident(table);
        let df_name = format!("DF_{}_ingested_at", table);
        format!(
            "IF OBJECT_ID(N'{t_lit}', N'U') IS NULL\n\
             BEGIN\n\
             CREATE TABLE {t_q} (\n\
                 [offset]       BIGINT         NOT NULL PRIMARY KEY,\n\
                 [ingested_at]  DATETIMEOFFSET NOT NULL CONSTRAINT {df_name} DEFAULT SYSUTCDATETIME(),\n\
                 [subject]      NVARCHAR(MAX)  NULL,\n\
                 [key]          NVARCHAR(MAX)  NULL,\n\
                 [value]        NVARCHAR(MAX)  NOT NULL CHECK (ISJSON([value]) = 1)\n\
             )\n\
             END"
        )
    }

    fn create_table_typed_sql(
        &self,
        table: &str,
        cols: &[ColumnSpec],
        pk_cols: &[&str],
    ) -> String {
        use crate::builtin::jdbc::dialect::JsonType;
        let t_lit = format!("[{}]", table);
        let t_q = self.quote_ident(table);
        let col_lines: Vec<String> = cols
            .iter()
            .map(|c| {
                let sql_type = match c.json_type {
                    JsonType::Text => "NVARCHAR(MAX)",
                    JsonType::Bigint => "BIGINT",
                    JsonType::Double => self.double_type(),
                    JsonType::Boolean => "BIT",
                    JsonType::Timestamptz => self.timestamptz_type(),
                    JsonType::Jsonb => self.json_blob_type(),
                };
                let nullability = if c.nullable { " NULL" } else { " NOT NULL" };
                format!("    {} {}{}", self.quote_ident(&c.name), sql_type, nullability)
            })
            .collect();
        let mut body = col_lines.join(",\n");
        if !pk_cols.is_empty() {
            let pks: Vec<String> = pk_cols.iter().map(|c| self.quote_ident(c)).collect();
            body.push_str(&format!(",\n    PRIMARY KEY ({})", pks.join(", ")));
        }
        format!(
            "IF OBJECT_ID(N'{t_lit}', N'U') IS NULL\n\
             BEGIN\n\
             CREATE TABLE {t_q} (\n{body}\n)\n\
             END"
        )
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
        // MSSQL has no ON CONFLICT. Use MERGE + WITH (HOLDLOCK) to defend
        // against phantom-insert races under concurrency. Statement
        // terminator `;` is MANDATORY on MERGE.
        let t_q = self.quote_ident(table);
        let col_sql: Vec<String> = cols.iter().map(|c| self.quote_ident(c)).collect();
        let placeholders: Vec<String> = (1..=cols.len()).map(|i| self.placeholder(i)).collect();
        let on_clause: Vec<String> = keys
            .iter()
            .map(|k| {
                let q = self.quote_ident(k);
                format!("t.{q} = s.{q}")
            })
            .collect();
        let update_cols: Vec<&&str> = cols.iter().filter(|c| !keys.contains(c)).collect();
        let insert_col_list = col_sql.join(", ");
        let insert_values_list = col_sql
            .iter()
            .map(|c| format!("s.{c}"))
            .collect::<Vec<_>>()
            .join(", ");

        let matched_clause = if update_cols.is_empty() {
            String::new()
        } else {
            let set: Vec<String> = update_cols
                .iter()
                .map(|c| {
                    let q = self.quote_ident(c);
                    format!("{q} = s.{q}")
                })
                .collect();
            format!("WHEN MATCHED THEN\n    UPDATE SET {}\n", set.join(", "))
        };

        format!(
            "MERGE INTO {t_q} WITH (HOLDLOCK) AS t\n\
             USING (VALUES ({values})) AS s ({cols})\n\
                ON {on_}\n\
             {matched}\
             WHEN NOT MATCHED THEN\n    INSERT ({cols}) VALUES ({inserts});",
            values = placeholders.join(", "),
            cols = insert_col_list,
            on_ = on_clause.join(" AND "),
            matched = matched_clause,
            inserts = insert_values_list,
        )
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
    fn placeholder_uses_at_p_n() {
        assert_eq!(MssqlDialect.placeholder(1), "@P1");
        assert_eq!(MssqlDialect.placeholder(7), "@P7");
    }

    #[test]
    fn quote_ident_uses_brackets() {
        assert_eq!(MssqlDialect.quote_ident("events"), "[events]");
        assert_eq!(MssqlDialect.quote_ident("order"), "[order]");
    }

    #[test]
    fn blob_create_table_shape() {
        let sql = MssqlDialect.create_table_blob_sql("events");
        assert!(sql.contains("IF OBJECT_ID(N'[events]', N'U') IS NULL"));
        assert!(sql.contains("CREATE TABLE [events]"));
        assert!(sql.contains("[offset]       BIGINT         NOT NULL PRIMARY KEY"));
        assert!(sql.contains("DATETIMEOFFSET"));
        assert!(sql.contains("CONSTRAINT DF_events_ingested_at DEFAULT SYSUTCDATETIME()"));
        assert!(sql.contains("CHECK (ISJSON([value]) = 1)"));
    }

    #[test]
    fn typed_create_table_shape() {
        let cols = vec![
            spec("id", JsonType::Bigint, false),
            spec("email", JsonType::Text, true),
            spec("price", JsonType::Double, false),
            spec("active", JsonType::Boolean, false),
            spec("created_at", JsonType::Timestamptz, false),
            spec("payload", JsonType::Jsonb, false),
        ];
        let sql = MssqlDialect.create_table_typed_sql("t", &cols, &["id"]);
        assert!(sql.contains("[id] BIGINT NOT NULL"));
        assert!(sql.contains("[email] NVARCHAR(MAX) NULL"));
        assert!(sql.contains("[price] FLOAT(53) NOT NULL"));
        assert!(sql.contains("[active] BIT NOT NULL"));
        assert!(sql.contains("[created_at] DATETIMEOFFSET(6) NOT NULL"));
        assert!(sql.contains("[payload] NVARCHAR(MAX) NOT NULL"));
        assert!(sql.contains("PRIMARY KEY ([id])"));
    }

    #[test]
    fn typed_create_table_no_pk_when_empty() {
        let cols = vec![spec("id", JsonType::Bigint, false)];
        let sql = MssqlDialect.create_table_typed_sql("t", &cols, &[]);
        assert!(!sql.contains("PRIMARY KEY"), "sql = {}", sql);
    }

    #[test]
    fn insert_sql_uses_at_p_placeholders() {
        let sql = MssqlDialect.insert_sql("t", &["a", "b", "c"]);
        assert_eq!(
            sql,
            "INSERT INTO [t] ([a], [b], [c]) VALUES (@P1, @P2, @P3)"
        );
    }

    #[test]
    fn upsert_sql_uses_merge_with_holdlock() {
        let sql = MssqlDialect.upsert_sql("events", &["offset", "subject", "key", "value"], &["offset"]);
        assert!(sql.starts_with("MERGE INTO [events] WITH (HOLDLOCK) AS t"), "sql: {sql}");
        assert!(sql.contains("USING (VALUES (@P1, @P2, @P3, @P4)) AS s ([offset], [subject], [key], [value])"));
        assert!(sql.contains("ON t.[offset] = s.[offset]"));
        assert!(sql.contains("WHEN MATCHED THEN\n    UPDATE SET [subject] = s.[subject], [key] = s.[key], [value] = s.[value]"));
        assert!(sql.contains("WHEN NOT MATCHED THEN\n    INSERT ([offset], [subject], [key], [value]) VALUES (s.[offset], s.[subject], s.[key], s.[value])"));
        assert!(sql.trim_end().ends_with(';'), "MERGE must terminate with ; — sql: {sql}");
    }

    #[test]
    fn upsert_sql_all_keys_has_no_matched_clause() {
        let sql = MssqlDialect.upsert_sql("t", &["id"], &["id"]);
        assert!(!sql.contains("WHEN MATCHED"), "no update clause when every column is a key — sql: {sql}");
        assert!(sql.contains("WHEN NOT MATCHED"));
    }

    #[test]
    fn upsert_sql_composite_key() {
        let sql = MssqlDialect.upsert_sql("t", &["a", "b", "v"], &["a", "b"]);
        assert!(sql.contains("ON t.[a] = s.[a] AND t.[b] = s.[b]"), "sql: {sql}");
        assert!(sql.contains("UPDATE SET [v] = s.[v]"));
    }
}
