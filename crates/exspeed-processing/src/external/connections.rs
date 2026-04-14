use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::RwLock;

use serde::{Deserialize, Serialize};

/// Configuration for a single external database connection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionConfig {
    pub name: String,
    pub driver: String,
    pub url: String,
}

/// Wrapper used when deserializing TOML files with a `[connection]` table.
#[derive(Deserialize)]
struct TomlWrapper {
    connection: ConnectionConfig,
}

/// Registry that holds named connection configurations.
///
/// Connections can be loaded from:
/// 1. Environment variables (`EXSPEED_CONNECTION_{NAME}_DRIVER` / `_URL`)
/// 2. TOML files in `{data_dir}/connections.d/*.toml`
/// 3. JSON files in `{data_dir}/connections/*.json`
///
/// Priority: env vars override TOML, TOML overrides JSON.
pub struct ConnectionRegistry {
    connections: RwLock<HashMap<String, ConnectionConfig>>,
    data_dir: PathBuf,
}

impl ConnectionRegistry {
    /// Create a new, empty registry backed by the given data directory.
    pub fn new(data_dir: PathBuf) -> Self {
        Self {
            connections: RwLock::new(HashMap::new()),
            data_dir,
        }
    }

    /// Load connections from all sources (JSON, then TOML, then env vars).
    /// Later sources override earlier ones so that env vars win.
    pub fn load_all(&self) {
        let mut map = self.connections.write().unwrap();

        // --- 1. JSON files in {data_dir}/connections/*.json ---
        let json_dir = self.data_dir.join("connections");
        if json_dir.is_dir() {
            if let Ok(entries) = fs::read_dir(&json_dir) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.extension().and_then(|e| e.to_str()) == Some("json") {
                        if let Ok(content) = fs::read_to_string(&path) {
                            if let Ok(cfg) = serde_json::from_str::<ConnectionConfig>(&content) {
                                let cfg = ConnectionConfig {
                                    url: resolve_env_vars(&cfg.url),
                                    ..cfg
                                };
                                map.insert(cfg.name.clone(), cfg);
                            }
                        }
                    }
                }
            }
        }

        // --- 2. TOML files in {data_dir}/connections.d/*.toml ---
        let toml_dir = self.data_dir.join("connections.d");
        if toml_dir.is_dir() {
            if let Ok(entries) = fs::read_dir(&toml_dir) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.extension().and_then(|e| e.to_str()) == Some("toml") {
                        if let Ok(content) = fs::read_to_string(&path) {
                            if let Ok(wrapper) = toml::from_str::<TomlWrapper>(&content) {
                                let cfg = ConnectionConfig {
                                    url: resolve_env_vars(&wrapper.connection.url),
                                    ..wrapper.connection
                                };
                                map.insert(cfg.name.clone(), cfg);
                            }
                        }
                    }
                }
            }
        }

        // --- 3. Environment variables ---
        // Scan for EXSPEED_CONNECTION_*_DRIVER / EXSPEED_CONNECTION_*_URL pairs.
        // The name portion is lowercased and underscores become hyphens.
        let prefix = "EXSPEED_CONNECTION_";
        let suffix_driver = "_DRIVER";
        let suffix_url = "_URL";

        // Collect all env var names that match the prefix.
        let mut driver_map: HashMap<String, String> = HashMap::new();
        let mut url_map: HashMap<String, String> = HashMap::new();

        for (key, value) in std::env::vars() {
            if let Some(rest) = key.strip_prefix(prefix) {
                if let Some(name_part) = rest.strip_suffix(suffix_driver) {
                    let name = name_part.to_lowercase().replace('_', "-");
                    driver_map.insert(name, value);
                } else if let Some(name_part) = rest.strip_suffix(suffix_url) {
                    let name = name_part.to_lowercase().replace('_', "-");
                    url_map.insert(name, resolve_env_vars(&value));
                }
            }
        }

        // Merge: only insert when both driver and url are present.
        for (name, driver) in &driver_map {
            if let Some(url) = url_map.get(name) {
                map.insert(
                    name.clone(),
                    ConnectionConfig {
                        name: name.clone(),
                        driver: driver.clone(),
                        url: url.clone(),
                    },
                );
            }
        }
    }

    /// Look up a connection by name.
    pub fn get(&self, name: &str) -> Option<ConnectionConfig> {
        self.connections.read().unwrap().get(name).cloned()
    }

    /// Add a new connection, persisting it as a JSON file.
    pub fn add(&self, config: ConnectionConfig) -> Result<(), String> {
        let json_dir = self.data_dir.join("connections");
        fs::create_dir_all(&json_dir)
            .map_err(|e| format!("failed to create connections dir: {e}"))?;

        let file_path = json_dir.join(format!("{}.json", config.name));
        let json =
            serde_json::to_string_pretty(&config).map_err(|e| format!("serialize: {e}"))?;
        fs::write(&file_path, json).map_err(|e| format!("write {}: {e}", file_path.display()))?;

        self.connections
            .write()
            .unwrap()
            .insert(config.name.clone(), config);
        Ok(())
    }

    /// Remove a connection from the registry and delete its JSON file if present.
    pub fn remove(&self, name: &str) -> Result<(), String> {
        self.connections.write().unwrap().remove(name);

        let file_path = self.data_dir.join("connections").join(format!("{name}.json"));
        if file_path.exists() {
            fs::remove_file(&file_path)
                .map_err(|e| format!("delete {}: {e}", file_path.display()))?;
        }

        Ok(())
    }

    /// List all registered connections as `(name, driver)` pairs.
    /// The URL is intentionally omitted (masked) for security.
    pub fn list(&self) -> Vec<(String, String)> {
        self.connections
            .read()
            .unwrap()
            .values()
            .map(|c| (c.name.clone(), c.driver.clone()))
            .collect()
    }
}

/// Resolve `${VAR}` placeholders in a string by looking up environment variables.
fn resolve_env_vars(input: &str) -> String {
    let mut result = input.to_string();
    // Simple approach: repeatedly find ${...} and replace.
    loop {
        let start = match result.find("${") {
            Some(i) => i,
            None => break,
        };
        let end = match result[start..].find('}') {
            Some(i) => start + i,
            None => break,
        };
        let var_name = &result[start + 2..end];
        let replacement = std::env::var(var_name).unwrap_or_default();
        result = format!("{}{}{}", &result[..start], replacement, &result[end + 1..]);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_from_env_vars() {
        // Set env vars for a test connection named "test-db"
        // Name = TEST_DB → lowercase, underscores → hyphens → "test-db"
        std::env::set_var("EXSPEED_CONNECTION_TEST_DB_DRIVER", "postgres");
        std::env::set_var(
            "EXSPEED_CONNECTION_TEST_DB_URL",
            "postgresql://localhost/test",
        );

        let dir = tempfile::tempdir().unwrap();
        let registry = ConnectionRegistry::new(dir.path().to_path_buf());
        registry.load_all();

        let cfg = registry.get("test-db").expect("should find test-db");
        assert_eq!(cfg.driver, "postgres");
        assert_eq!(cfg.url, "postgresql://localhost/test");

        // Clean up env
        std::env::remove_var("EXSPEED_CONNECTION_TEST_DB_DRIVER");
        std::env::remove_var("EXSPEED_CONNECTION_TEST_DB_URL");
    }

    #[test]
    fn load_from_toml_file() {
        let dir = tempfile::tempdir().unwrap();
        let toml_dir = dir.path().join("connections.d");
        fs::create_dir_all(&toml_dir).unwrap();
        fs::write(
            toml_dir.join("mydb.toml"),
            r#"
[connection]
name = "mydb"
driver = "postgres"
url = "postgresql://localhost/mydb"
"#,
        )
        .unwrap();

        let registry = ConnectionRegistry::new(dir.path().to_path_buf());
        registry.load_all();

        let cfg = registry.get("mydb").expect("should find mydb");
        assert_eq!(cfg.driver, "postgres");
        assert_eq!(cfg.url, "postgresql://localhost/mydb");
    }

    #[test]
    fn load_from_json_file() {
        let dir = tempfile::tempdir().unwrap();
        let json_dir = dir.path().join("connections");
        fs::create_dir_all(&json_dir).unwrap();
        fs::write(
            json_dir.join("warehouse.json"),
            r#"{"name":"warehouse","driver":"postgres","url":"postgresql://localhost/wh"}"#,
        )
        .unwrap();

        let registry = ConnectionRegistry::new(dir.path().to_path_buf());
        registry.load_all();

        let cfg = registry.get("warehouse").expect("should find warehouse");
        assert_eq!(cfg.driver, "postgres");
        assert_eq!(cfg.url, "postgresql://localhost/wh");
    }

    #[test]
    fn add_and_get() {
        let dir = tempfile::tempdir().unwrap();
        let registry = ConnectionRegistry::new(dir.path().to_path_buf());

        registry
            .add(ConnectionConfig {
                name: "my-pg".into(),
                driver: "postgres".into(),
                url: "postgresql://localhost/test".into(),
            })
            .unwrap();

        let cfg = registry.get("my-pg").expect("should find my-pg");
        assert_eq!(cfg.driver, "postgres");

        // JSON file should exist
        let file = dir.path().join("connections/my-pg.json");
        assert!(file.exists());
    }

    #[test]
    fn remove_deletes_file() {
        let dir = tempfile::tempdir().unwrap();
        let registry = ConnectionRegistry::new(dir.path().to_path_buf());

        registry
            .add(ConnectionConfig {
                name: "rm-test".into(),
                driver: "postgres".into(),
                url: "postgresql://localhost/rm".into(),
            })
            .unwrap();

        let file = dir.path().join("connections/rm-test.json");
        assert!(file.exists());

        registry.remove("rm-test").unwrap();
        assert!(registry.get("rm-test").is_none());
        assert!(!file.exists());
    }

    #[test]
    fn list_returns_name_driver_pairs() {
        let dir = tempfile::tempdir().unwrap();
        let registry = ConnectionRegistry::new(dir.path().to_path_buf());

        registry
            .add(ConnectionConfig {
                name: "a".into(),
                driver: "postgres".into(),
                url: "postgresql://localhost/a".into(),
            })
            .unwrap();
        registry
            .add(ConnectionConfig {
                name: "b".into(),
                driver: "mssql".into(),
                url: "mssql://localhost/b".into(),
            })
            .unwrap();

        let mut pairs = registry.list();
        pairs.sort();
        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0], ("a".to_string(), "postgres".to_string()));
        assert_eq!(pairs[1], ("b".to_string(), "mssql".to_string()));
    }

    #[test]
    fn env_vars_override_toml() {
        let dir = tempfile::tempdir().unwrap();
        let toml_dir = dir.path().join("connections.d");
        fs::create_dir_all(&toml_dir).unwrap();
        fs::write(
            toml_dir.join("override.toml"),
            r#"
[connection]
name = "override-db"
driver = "postgres"
url = "postgresql://toml-host/db"
"#,
        )
        .unwrap();

        std::env::set_var("EXSPEED_CONNECTION_OVERRIDE_DB_DRIVER", "postgres");
        std::env::set_var(
            "EXSPEED_CONNECTION_OVERRIDE_DB_URL",
            "postgresql://env-host/db",
        );

        let registry = ConnectionRegistry::new(dir.path().to_path_buf());
        registry.load_all();

        let cfg = registry.get("override-db").expect("should find override-db");
        // env var should win
        assert_eq!(cfg.url, "postgresql://env-host/db");

        std::env::remove_var("EXSPEED_CONNECTION_OVERRIDE_DB_DRIVER");
        std::env::remove_var("EXSPEED_CONNECTION_OVERRIDE_DB_URL");
    }

    #[test]
    fn resolve_env_var_in_url() {
        std::env::set_var("TEST_PG_HOST", "my-host");
        std::env::set_var("TEST_PG_PORT", "5433");

        let dir = tempfile::tempdir().unwrap();
        let json_dir = dir.path().join("connections");
        fs::create_dir_all(&json_dir).unwrap();
        fs::write(
            json_dir.join("envurl.json"),
            r#"{"name":"envurl","driver":"postgres","url":"postgresql://${TEST_PG_HOST}:${TEST_PG_PORT}/db"}"#,
        )
        .unwrap();

        let registry = ConnectionRegistry::new(dir.path().to_path_buf());
        registry.load_all();

        let cfg = registry.get("envurl").expect("should find envurl");
        assert_eq!(cfg.url, "postgresql://my-host:5433/db");

        std::env::remove_var("TEST_PG_HOST");
        std::env::remove_var("TEST_PG_PORT");
    }
}
