//! Name resolution for SQL.
//!
//! - `exspeed.public.<name>` (the default, so plain `<name>`): a
//!   materialized table if one has that name, otherwise a stream.
//! - `<conn>.<table>`: table `<table>` in schema `public` of registered
//!   connection `<conn>`; `<conn>.<schema>.<table>` names the remote schema.
//!
//! Anything else is "not found", which DataFusion reports as an error.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, CatalogProviderList, SchemaProvider, TableProvider};
use datafusion::common::{exec_err, Result as DFResult};
use exspeed_common::StreamName;
use exspeed_streams::{StorageEngine, StorageError};

use crate::error::ExqlError;
use crate::external::ExternalTables;
use crate::stream_table::StreamTable;
use crate::tables::TableRegistry;

pub const CATALOG: &str = "exspeed";
pub const SCHEMA: &str = "public";

#[derive(Clone)]
pub struct Resolver {
    pub storage: Arc<dyn StorageEngine>,
    pub tables: Arc<TableRegistry>,
    pub external: Arc<ExternalTables>,
    /// False while planning continuous queries (external tables are
    /// bounded-only).
    pub allow_external: bool,
}

impl fmt::Debug for Resolver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Resolver")
    }
}

impl Resolver {
    /// Look up a stream or table by plain name.
    pub async fn local(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, ExqlError> {
        if let Some(t) = self.tables.get(name) {
            return Ok(Some(t));
        }
        let Ok(stream) = StreamName::try_from(name) else {
            return Ok(None);
        };
        match self.storage.stream_bounds(&stream).await {
            Ok(_) => Ok(Some(Arc::new(StreamTable::new(
                self.storage.clone(),
                stream,
            )))),
            Err(StorageError::StreamNotFound(_)) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
}

#[derive(Debug)]
pub struct ExspeedCatalogList {
    resolver: Resolver,
}

impl ExspeedCatalogList {
    pub fn new(resolver: Resolver) -> Self {
        Self { resolver }
    }
}

impl CatalogProviderList for ExspeedCatalogList {
    fn register_catalog(
        &self,
        _name: String,
        _catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        None
    }

    fn catalog_names(&self) -> Vec<String> {
        let mut names = vec![CATALOG.to_string()];
        names.extend(
            self.resolver
                .external
                .registry()
                .list()
                .into_iter()
                .map(|(n, _)| n),
        );
        names
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        if name == CATALOG {
            return Some(Arc::new(ExspeedCatalog {
                resolver: self.resolver.clone(),
            }));
        }
        if self.resolver.external.has_connection(name) {
            return Some(Arc::new(ConnectionCatalog {
                resolver: self.resolver.clone(),
                conn: name.to_string(),
            }));
        }
        None
    }
}

#[derive(Debug)]
struct ExspeedCatalog {
    resolver: Resolver,
}

impl CatalogProvider for ExspeedCatalog {
    fn schema_names(&self) -> Vec<String> {
        vec![SCHEMA.to_string()]
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        if name == SCHEMA {
            return Some(Arc::new(LocalSchema {
                resolver: self.resolver.clone(),
            }));
        }
        if self.resolver.external.has_connection(name) {
            return Some(Arc::new(ConnectionSchema {
                resolver: self.resolver.clone(),
                conn: name.to_string(),
                remote_schema: "public".to_string(),
            }));
        }
        None
    }
}

#[derive(Debug)]
struct ConnectionCatalog {
    resolver: Resolver,
    conn: String,
}

impl CatalogProvider for ConnectionCatalog {
    fn schema_names(&self) -> Vec<String> {
        vec![]
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        Some(Arc::new(ConnectionSchema {
            resolver: self.resolver.clone(),
            conn: self.conn.clone(),
            remote_schema: name.to_string(),
        }))
    }
}

#[derive(Debug)]
struct LocalSchema {
    resolver: Resolver,
}

#[async_trait]
impl SchemaProvider for LocalSchema {
    fn table_names(&self) -> Vec<String> {
        self.resolver
            .tables
            .list()
            .iter()
            .map(|t| t.name.clone())
            .collect()
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        self.resolver.local(name).await.map_err(|e| e.into_df())
    }

    fn table_exist(&self, name: &str) -> bool {
        self.resolver.tables.get(name).is_some()
    }
}

#[derive(Debug)]
struct ConnectionSchema {
    resolver: Resolver,
    conn: String,
    remote_schema: String,
}

#[async_trait]
impl SchemaProvider for ConnectionSchema {
    fn table_names(&self) -> Vec<String> {
        vec![]
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        if !self.resolver.allow_external {
            return exec_err!(
                "external table {}.{name}: external databases are only available in bounded queries",
                self.conn
            );
        }
        match self
            .resolver
            .external
            .table(&self.conn, &self.remote_schema, name)
            .await
        {
            Ok(Some(t)) => Ok(Some(t as Arc<dyn TableProvider>)),
            Ok(None) => Ok(None),
            Err(e) => Err(e.into_df()),
        }
    }

    fn table_exist(&self, _name: &str) -> bool {
        false
    }
}
