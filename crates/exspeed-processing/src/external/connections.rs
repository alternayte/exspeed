use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::RwLock;

use bytes::Bytes;
use exspeed_broker::catalog::{CatalogStore, Migration};
use exspeed_broker::log::LogError;
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::error::ExqlError;

/// Internal stream holding API-created connections (key = name, value =
/// [`ConnectionConfig`] JSON with `${VAR}` references unresolved).
pub const CONNECTIONS_STREAM: &str = "__exql_connections";

/// Configuration for a single external database connection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

/// Where a connection is defined.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Origin {
    Api,
    File,
    Env,
}

/// Registry that holds named connection configurations.
///
/// Connections come from:
/// 1. The HTTP API, stored in the replicated internal stream
///    `__exql_connections` (see [`ConnectionRegistry::with_store`]); a
///    registry without a store keeps API connections in memory only.
/// 2. TOML files in `{data_dir}/connections.d/*.toml` (operator-managed,
///    shipped to every node).
/// 3. Environment variables (`EXSPEED_CONNECTION_{NAME}_DRIVER` / `_URL`).
///
/// Priority: env vars override TOML, TOML overrides API. `${VAR}` in URLs is
/// resolved from the server's environment in memory; the stored definition
/// keeps the reference.
pub struct ConnectionRegistry {
    /// Effective, resolved connections.
    connections: RwLock<HashMap<String, (ConnectionConfig, Origin)>>,
    /// API-created connections as stored (unresolved).
    api: RwLock<BTreeMap<String, ConnectionConfig>>,
    data_dir: PathBuf,
    store: Option<CatalogStore>,
}

/// Map a catalog write error; `NotLeader` stays `NotLeader` (503).
pub(crate) fn catalog_err(e: LogError) -> ExqlError {
    match e {
        LogError::NotLeader => ExqlError::NotLeader,
        e => ExqlError::from(e),
    }
}

impl ConnectionRegistry {
    /// A registry without a catalog store: API connections live in memory.
    pub fn new(data_dir: PathBuf) -> Self {
        Self {
            connections: RwLock::new(HashMap::new()),
            api: RwLock::new(BTreeMap::new()),
            data_dir,
            store: None,
        }
    }

    /// A registry whose API connections are stored in `store` (the
    /// `__exql_connections` stream).
    pub fn with_store(data_dir: PathBuf, store: CatalogStore) -> Self {
        Self {
            store: Some(store),
            ..Self::new(data_dir)
        }
    }

    /// Where older versions kept API-created connections; imported once by
    /// [`Self::reload`].
    pub fn legacy_dir(&self) -> PathBuf {
        self.data_dir.join("connections")
    }

    /// (Re)load every source: API connections from the catalog stream
    /// (importing legacy `connections/*.json` files first when the stream is
    /// empty and this node can write), then `connections.d/*.toml` and the
    /// environment. Safe to call repeatedly (e.g. at the start of every
    /// leader tenure); the in-memory catalog is replaced.
    pub async fn reload(&self) -> Result<(), ExqlError> {
        if let Some(store) = &self.store {
            let migrated = store
                .migrate_dir(&self.legacy_dir(), |_, bytes| {
                    let cfg: ConnectionConfig =
                        serde_json::from_slice(bytes).map_err(|e| e.to_string())?;
                    validate_connection_name(&cfg.name)?;
                    let value = serde_json::to_vec(&cfg).map_err(|e| e.to_string())?;
                    Ok((cfg.name, Bytes::from(value)))
                })
                .await;
            match migrated {
                Ok(Migration::Deferred) => warn!(
                    "legacy connections/ directory found; it is imported when this node leads"
                ),
                Ok(_) => {}
                Err(e) => warn!("could not migrate legacy connections: {e}"),
            }
            let mut api = BTreeMap::new();
            for (name, value) in store.load().await.map_err(catalog_err)? {
                match serde_json::from_slice::<ConnectionConfig>(&value) {
                    Ok(cfg) if cfg.name == name => {
                        api.insert(name, cfg);
                    }
                    Ok(_) => warn!(connection = %name, "connection record name mismatch; ignored"),
                    Err(e) => warn!(connection = %name, "unreadable connection record: {e}"),
                }
            }
            *self.api.write().unwrap() = api;
        }
        self.rebuild();
        Ok(())
    }

    /// Recompute the effective set from the API catalog, TOML files and env.
    fn rebuild(&self) {
        let mut map: HashMap<String, (ConnectionConfig, Origin)> = HashMap::new();

        // --- 1. API-created ---
        for cfg in self.api.read().unwrap().values() {
            map.insert(cfg.name.clone(), (resolved(cfg.clone()), Origin::Api));
        }

        // --- 2. TOML files in {data_dir}/connections.d/*.toml ---
        let toml_dir = self.data_dir.join("connections.d");
        if let Ok(entries) = fs::read_dir(&toml_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().and_then(|e| e.to_str()) != Some("toml") {
                    continue;
                }
                match load_toml(&path) {
                    Ok(cfg) => {
                        map.insert(cfg.name.clone(), (resolved(cfg), Origin::File));
                    }
                    Err(e) => warn!(path = %path.display(), "ignoring connection file: {e}"),
                }
            }
        }

        // --- 3. Environment variables ---
        // Scan for EXSPEED_CONNECTION_*_DRIVER / EXSPEED_CONNECTION_*_URL pairs.
        // The name portion is lowercased and underscores become hyphens.
        let prefix = "EXSPEED_CONNECTION_";
        let mut driver_map: HashMap<String, String> = HashMap::new();
        let mut url_map: HashMap<String, String> = HashMap::new();
        for (key, value) in std::env::vars() {
            if let Some(rest) = key.strip_prefix(prefix) {
                if let Some(name_part) = rest.strip_suffix("_DRIVER") {
                    let name = name_part.to_lowercase().replace('_', "-");
                    driver_map.insert(name, value);
                } else if let Some(name_part) = rest.strip_suffix("_URL") {
                    let name = name_part.to_lowercase().replace('_', "-");
                    url_map.insert(name, resolve_env_vars(&value));
                }
            }
        }
        // Merge: only insert when both driver and url are present.
        for (name, driver) in driver_map {
            if let Some(url) = url_map.remove(&name) {
                let cfg = ConnectionConfig {
                    name: name.clone(),
                    driver,
                    url,
                };
                map.insert(name, (cfg, Origin::Env));
            }
        }
        *self.connections.write().unwrap() = map;
    }

    /// Names of every registered connection.
    pub fn names(&self) -> Vec<String> {
        self.connections.read().unwrap().keys().cloned().collect()
    }

    /// Look up a connection by name.
    pub fn get(&self, name: &str) -> Option<ConnectionConfig> {
        self.connections
            .read()
            .unwrap()
            .get(name)
            .map(|(c, _)| c.clone())
    }

    /// Add (or replace) an API connection. With a store it is written to
    /// `__exql_connections` first, which only the leader can do.
    pub async fn add(&self, config: ConnectionConfig) -> Result<(), ExqlError> {
        validate_connection_name(&config.name).map_err(ExqlError::Plan)?;
        if !SUPPORTED_DRIVERS.contains(&config.driver.as_str()) {
            return Err(ExqlError::Plan(format!(
                "unsupported driver '{}'; supported: {}",
                config.driver,
                SUPPORTED_DRIVERS.join(", ")
            )));
        }
        if let Some(store) = &self.store {
            let value =
                serde_json::to_vec(&config).map_err(|e| ExqlError::Internal(e.to_string()))?;
            store
                .put(&config.name, Bytes::from(value))
                .await
                .map_err(catalog_err)?;
        }
        self.api
            .write()
            .unwrap()
            .insert(config.name.clone(), config);
        self.rebuild();
        Ok(())
    }

    /// Remove an API connection (a tombstone in `__exql_connections`).
    /// Connections defined in `connections.d/` or the environment can't be
    /// removed through the API.
    pub async fn remove(&self, name: &str) -> Result<(), ExqlError> {
        validate_connection_name(name).map_err(ExqlError::Plan)?;
        let in_api = self.api.read().unwrap().contains_key(name);
        if !in_api {
            let origin = self.connections.read().unwrap().get(name).map(|(_, o)| *o);
            return Err(match origin {
                Some(Origin::Env) => ExqlError::Conflict(format!(
                    "connection '{name}' is defined by environment variables; remove it there"
                )),
                Some(_) => ExqlError::Conflict(format!(
                    "connection '{name}' is defined in connections.d/; remove the file instead"
                )),
                None => ExqlError::NotFound(format!("connection '{name}' not found")),
            });
        }
        if let Some(store) = &self.store {
            store.delete(name).await.map_err(catalog_err)?;
        }
        self.api.write().unwrap().remove(name);
        self.rebuild();
        Ok(())
    }

    /// List all registered connections as `(name, driver)` pairs.
    /// The URL is intentionally omitted (masked) for security.
    pub fn list(&self) -> Vec<(String, String)> {
        self.connections
            .read()
            .unwrap()
            .values()
            .map(|(c, _)| (c.name.clone(), c.driver.clone()))
            .collect()
    }
}

fn resolved(cfg: ConnectionConfig) -> ConnectionConfig {
    ConnectionConfig {
        url: resolve_env_vars(&cfg.url),
        ..cfg
    }
}

fn load_toml(path: &Path) -> Result<ConnectionConfig, String> {
    let content = fs::read_to_string(path).map_err(|e| e.to_string())?;
    let wrapper = toml::from_str::<TomlWrapper>(&content).map_err(|e| e.to_string())?;
    Ok(wrapper.connection)
}

/// Drivers external tables can read from.
pub const SUPPORTED_DRIVERS: &[&str] = &["postgres", "postgresql"];

/// Connection names become SQL schema names: keep them to
/// `[A-Za-z0-9_-]{1,64}`.
pub fn validate_connection_name(name: &str) -> Result<(), String> {
    if name.is_empty()
        || name.len() > 64
        || !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return Err(format!(
            "invalid connection name '{name}' (allowed: 1-64 of a-z, A-Z, 0-9, -, _)"
        ));
    }
    Ok(())
}

/// Resolve `${VAR}` placeholders in a string by looking up environment variables.
fn resolve_env_vars(input: &str) -> String {
    let mut result = input.to_string();
    // Simple approach: repeatedly find ${...} and replace.
    while let Some(start) = result.find("${") {
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
    use crate::test_util::World;

    fn pg(name: &str, url: &str) -> ConnectionConfig {
        ConnectionConfig {
            name: name.into(),
            driver: "postgres".into(),
            url: url.into(),
        }
    }

    fn stored(world: &World, dir: &Path) -> ConnectionRegistry {
        ConnectionRegistry::with_store(
            dir.to_path_buf(),
            CatalogStore::new(world.log.clone(), CONNECTIONS_STREAM, "exql.connection"),
        )
    }

    #[tokio::test]
    async fn load_from_env_vars() {
        // Name = TEST_DB → lowercase, underscores → hyphens → "test-db"
        std::env::set_var("EXSPEED_CONNECTION_TEST_DB_DRIVER", "postgres");
        std::env::set_var(
            "EXSPEED_CONNECTION_TEST_DB_URL",
            "postgresql://localhost/test",
        );

        let dir = tempfile::tempdir().unwrap();
        let registry = ConnectionRegistry::new(dir.path().to_path_buf());
        registry.reload().await.unwrap();

        let cfg = registry.get("test-db").expect("should find test-db");
        assert_eq!(cfg.driver, "postgres");
        assert_eq!(cfg.url, "postgresql://localhost/test");
        assert!(matches!(
            registry.remove("test-db").await,
            Err(ExqlError::Conflict(_))
        ));

        std::env::remove_var("EXSPEED_CONNECTION_TEST_DB_DRIVER");
        std::env::remove_var("EXSPEED_CONNECTION_TEST_DB_URL");
    }

    #[tokio::test]
    async fn load_from_toml_file() {
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
        registry.reload().await.unwrap();

        let cfg = registry.get("mydb").expect("should find mydb");
        assert_eq!(cfg.driver, "postgres");
        assert_eq!(cfg.url, "postgresql://localhost/mydb");
        assert!(matches!(
            registry.remove("mydb").await,
            Err(ExqlError::Conflict(_))
        ));
    }

    #[tokio::test]
    async fn api_connections_live_in_the_catalog_stream() {
        let world = World::new().await;
        let dir = tempfile::tempdir().unwrap();
        let registry = stored(&world, dir.path());
        registry.reload().await.unwrap();
        registry
            .add(pg("my-pg", "postgresql://localhost/test"))
            .await
            .unwrap();
        registry
            .add(pg("other", "postgresql://localhost/other"))
            .await
            .unwrap();
        assert_eq!(registry.get("my-pg").unwrap().driver, "postgres");
        assert!(!dir.path().join("connections").exists(), "no files");

        // Another node (or a restart) reads the same catalog.
        let fresh = stored(&world, dir.path());
        fresh.reload().await.unwrap();
        let mut names = fresh.names();
        names.sort();
        assert_eq!(names, vec!["my-pg", "other"]);

        registry.remove("my-pg").await.unwrap();
        assert!(registry.get("my-pg").is_none());
        assert!(matches!(
            registry.remove("my-pg").await,
            Err(ExqlError::NotFound(_))
        ));
        // Reloading replaces the in-memory catalog.
        fresh.reload().await.unwrap();
        assert!(fresh.get("my-pg").is_none());
        assert!(fresh.get("other").is_some());
    }

    #[tokio::test]
    async fn legacy_json_files_are_migrated_once() {
        std::env::set_var("TEST_PG_HOST", "my-host");
        std::env::set_var("TEST_PG_PORT", "5433");
        let world = World::new().await;
        let dir = tempfile::tempdir().unwrap();
        let json_dir = dir.path().join("connections");
        fs::create_dir_all(&json_dir).unwrap();
        fs::write(
            json_dir.join("envurl.json"),
            r#"{"name":"envurl","driver":"postgres","url":"postgresql://${TEST_PG_HOST}:${TEST_PG_PORT}/db"}"#,
        )
        .unwrap();
        fs::write(json_dir.join("broken.json"), "{").unwrap();

        let registry = stored(&world, dir.path());
        registry.reload().await.unwrap();
        // Resolved in memory, stored with the reference.
        let cfg = registry.get("envurl").expect("should find envurl");
        assert_eq!(cfg.url, "postgresql://my-host:5433/db");
        assert!(!json_dir.exists());
        assert!(dir.path().join("connections.migrated/envurl.json").exists());
        let store = CatalogStore::new(world.log.clone(), CONNECTIONS_STREAM, "x");
        let raw = store.load().await.unwrap();
        assert!(String::from_utf8_lossy(&raw["envurl"]).contains("${TEST_PG_HOST}"));
        assert_eq!(raw.len(), 1);

        std::env::remove_var("TEST_PG_HOST");
        std::env::remove_var("TEST_PG_PORT");
    }

    #[tokio::test]
    async fn list_returns_name_driver_pairs() {
        let dir = tempfile::tempdir().unwrap();
        let registry = ConnectionRegistry::new(dir.path().to_path_buf());
        registry
            .add(pg("a", "postgresql://localhost/a"))
            .await
            .unwrap();
        registry
            .add(ConnectionConfig {
                name: "b".into(),
                driver: "postgresql".into(),
                url: "postgresql://localhost/b".into(),
            })
            .await
            .unwrap();

        let mut pairs = registry.list();
        pairs.sort();
        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0], ("a".to_string(), "postgres".to_string()));
        assert_eq!(pairs[1], ("b".to_string(), "postgresql".to_string()));
    }

    #[tokio::test]
    async fn rejects_bad_names_and_drivers() {
        let dir = tempfile::tempdir().unwrap();
        let registry = ConnectionRegistry::new(dir.path().to_path_buf());
        for name in ["../x", "", "a/b", "a.b"] {
            assert!(registry
                .add(pg(name, "postgresql://localhost/a"))
                .await
                .is_err());
        }
        assert!(registry
            .add(ConnectionConfig {
                name: "ok".into(),
                driver: "mssql".into(),
                url: "mssql://localhost/a".into(),
            })
            .await
            .is_err());
        assert!(registry.remove("../consumers/foo").await.is_err());
    }

    #[tokio::test]
    async fn env_vars_override_toml_and_toml_overrides_api() {
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

        let registry = ConnectionRegistry::new(dir.path().to_path_buf());
        registry
            .add(pg("override-db", "postgresql://api-host/db"))
            .await
            .unwrap();
        assert_eq!(
            registry.get("override-db").unwrap().url,
            "postgresql://toml-host/db"
        );

        std::env::set_var("EXSPEED_CONNECTION_OVERRIDE_DB_DRIVER", "postgres");
        std::env::set_var(
            "EXSPEED_CONNECTION_OVERRIDE_DB_URL",
            "postgresql://env-host/db",
        );
        registry.reload().await.unwrap();
        let cfg = registry
            .get("override-db")
            .expect("should find override-db");
        // env var should win
        assert_eq!(cfg.url, "postgresql://env-host/db");

        std::env::remove_var("EXSPEED_CONNECTION_OVERRIDE_DB_DRIVER");
        std::env::remove_var("EXSPEED_CONNECTION_OVERRIDE_DB_URL");
    }
}
