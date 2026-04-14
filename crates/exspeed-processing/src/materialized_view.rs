use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::types::Row;

pub struct MaterializedViewRegistry {
    views: RwLock<HashMap<String, MaterializedView>>,
}

pub struct MaterializedView {
    pub name: String,
    pub state: Arc<RwLock<HashMap<String, Row>>>,
    pub columns: Vec<String>,
    pub query_id: String,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct MvInfo {
    pub name: String,
    pub columns: Vec<String>,
    pub row_count: usize,
    pub query_id: String,
}

impl MaterializedViewRegistry {
    /// Create a new, empty registry.
    pub fn new() -> Self {
        Self {
            views: RwLock::new(HashMap::new()),
        }
    }

    /// Register a new materialized view and return its shared state handle.
    ///
    /// The returned `Arc<RwLock<HashMap<String, Row>>>` is the same backing
    /// store held by the registry, so a continuous executor can write rows
    /// directly through this handle and they will be visible to readers.
    pub fn register(
        &self,
        name: impl Into<String>,
        columns: Vec<String>,
        query_id: impl Into<String>,
    ) -> Arc<RwLock<HashMap<String, Row>>> {
        let name = name.into();
        let query_id = query_id.into();
        let state: Arc<RwLock<HashMap<String, Row>>> = Arc::new(RwLock::new(HashMap::new()));
        let mv = MaterializedView {
            name: name.clone(),
            state: Arc::clone(&state),
            columns,
            query_id,
        };
        self.views.write().unwrap().insert(name, mv);
        state
    }

    /// Return `(columns, all_rows)` for a named materialized view, or `None`
    /// if the view does not exist.
    pub fn get_rows(&self, name: &str) -> Option<(Vec<String>, Vec<Row>)> {
        let views = self.views.read().unwrap();
        let mv = views.get(name)?;
        let columns = mv.columns.clone();
        let rows: Vec<Row> = mv.state.read().unwrap().values().cloned().collect();
        Some((columns, rows))
    }

    /// Return a single row by its group key, or `None` if the view or key does
    /// not exist.
    pub fn get_row(&self, name: &str, key: &str) -> Option<Row> {
        let views = self.views.read().unwrap();
        let mv = views.get(name)?;
        let row = mv.state.read().unwrap().get(key).cloned();
        row
    }

    /// Returns `true` if a materialized view with this name is registered.
    pub fn is_view(&self, name: &str) -> bool {
        self.views.read().unwrap().contains_key(name)
    }

    /// Return metadata for all registered materialized views.
    pub fn list(&self) -> Vec<MvInfo> {
        let views = self.views.read().unwrap();
        views
            .values()
            .map(|mv| MvInfo {
                name: mv.name.clone(),
                columns: mv.columns.clone(),
                row_count: mv.state.read().unwrap().len(),
                query_id: mv.query_id.clone(),
            })
            .collect()
    }

    /// Remove a materialized view by name.
    ///
    /// Returns `true` if the view existed and was removed, `false` otherwise.
    pub fn remove(&self, name: &str) -> bool {
        self.views.write().unwrap().remove(name).is_some()
    }
}

impl Default for MaterializedViewRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Value;

    fn make_row(key: &str, val: i64) -> Row {
        Row {
            columns: vec!["key".into(), "val".into()],
            values: vec![Value::Text(key.into()), Value::Int(val)],
        }
    }

    #[test]
    fn register_and_get_rows_roundtrip() {
        let registry = MaterializedViewRegistry::new();
        let state = registry.register(
            "mv_test",
            vec!["key".into(), "val".into()],
            "q-001",
        );

        // Write two rows via the returned state handle.
        state
            .write()
            .unwrap()
            .insert("a".into(), make_row("a", 1));
        state
            .write()
            .unwrap()
            .insert("b".into(), make_row("b", 2));

        let (cols, rows) = registry.get_rows("mv_test").expect("view should exist");
        assert_eq!(cols, vec!["key", "val"]);
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn register_and_get_row_by_key() {
        let registry = MaterializedViewRegistry::new();
        let state = registry.register(
            "mv_key",
            vec!["key".into(), "val".into()],
            "q-002",
        );

        state
            .write()
            .unwrap()
            .insert("hello".into(), make_row("hello", 42));

        let row = registry.get_row("mv_key", "hello").expect("row should exist");
        assert_eq!(row.get("val"), Some(&Value::Int(42)));

        assert!(registry.get_row("mv_key", "missing").is_none());
        assert!(registry.get_row("no_such_view", "hello").is_none());
    }

    #[test]
    fn is_view_true_and_false() {
        let registry = MaterializedViewRegistry::new();
        assert!(!registry.is_view("nonexistent"));

        registry.register("my_view", vec!["x".into()], "q-003");
        assert!(registry.is_view("my_view"));
        assert!(!registry.is_view("other_view"));
    }

    #[test]
    fn list_returns_all_mvs_with_row_counts() {
        let registry = MaterializedViewRegistry::new();

        let s1 = registry.register("view_a", vec!["col1".into()], "q-010");
        let s2 = registry.register("view_b", vec!["col2".into()], "q-011");

        s1.write().unwrap().insert("k1".into(), make_row("k1", 1));
        s1.write().unwrap().insert("k2".into(), make_row("k2", 2));
        s2.write().unwrap().insert("k3".into(), make_row("k3", 3));

        let mut infos = registry.list();
        infos.sort_by(|a, b| a.name.cmp(&b.name));

        assert_eq!(infos.len(), 2);
        assert_eq!(infos[0].name, "view_a");
        assert_eq!(infos[0].row_count, 2);
        assert_eq!(infos[1].name, "view_b");
        assert_eq!(infos[1].row_count, 1);
    }

    #[test]
    fn remove_returns_true_and_is_view_false_after() {
        let registry = MaterializedViewRegistry::new();
        registry.register("to_remove", vec!["x".into()], "q-020");

        assert!(registry.is_view("to_remove"));
        assert!(registry.remove("to_remove"));
        assert!(!registry.is_view("to_remove"));

        // Removing again returns false.
        assert!(!registry.remove("to_remove"));
    }
}
