//! Built-in adapter registry: per-kind name → factory maps so a mount can
//! select a built-in Rust backend with `store.adapter = "builtin:<name>"`, the
//! Rust-side sibling of the JS `code:<name>@<version>` loadable adapters.
//!
//! A factory builds an **un-scoped** capability handle from the mount's `store`
//! config; the caller (`tenant.rs`) applies tenant scoping uniformly, exactly
//! as it does for the node default and the `code:` path. Factories are
//! tenant-agnostic, so the `mem` factory hands back a single shared instance
//! and every `builtin:mem` mount shares its state with the node default.

use std::collections::HashMap;
use std::sync::Arc;

use crate::capabilities::{DataStore, FileStore, HttpOut, MessageGateway, QueryStore};
use crate::error::RsError;

/// Builds an un-scoped [`DataStore`] from a mount's `store` config.
pub type DataFactory =
    Arc<dyn Fn(&serde_json::Value) -> Result<Arc<dyn DataStore>, RsError> + Send + Sync>;
/// Builds an un-scoped [`FileStore`] from a mount's `store` config.
pub type FileFactory =
    Arc<dyn Fn(&serde_json::Value) -> Result<Arc<dyn FileStore>, RsError> + Send + Sync>;
/// Builds an un-scoped [`QueryStore`] from a mount's `store` config.
pub type QueryFactory =
    Arc<dyn Fn(&serde_json::Value) -> Result<Arc<dyn QueryStore>, RsError> + Send + Sync>;
/// Builds an un-scoped [`MessageGateway`] from a mount's `store` config. Unlike
/// the store kinds, a provider adapter reaches the network, so it is handed the
/// node's [`HttpOut`] rather than opening its own client — every provider call
/// then crosses the one audited egress choke point.
pub type MessageFactory = Arc<
    dyn Fn(&serde_json::Value, Arc<dyn HttpOut>) -> Result<Arc<dyn MessageGateway>, RsError>
        + Send
        + Sync,
>;

/// Node-level registry of built-in adapters, keyed per capability kind. Rides
/// on [`crate::tenant::Adapters`] (hence `Clone`); seeded in `Adapters::new`
/// with the node's own built-ins (`mem`, `local`, `reference`).
#[derive(Clone, Default)]
pub struct BuiltinRegistry {
    data: HashMap<String, DataFactory>,
    files: HashMap<String, FileFactory>,
    query: HashMap<String, QueryFactory>,
    message: HashMap<String, MessageFactory>,
}

impl BuiltinRegistry {
    pub fn register_data(&mut self, name: &str, f: DataFactory) {
        self.data.insert(name.to_string(), f);
    }
    pub fn register_files(&mut self, name: &str, f: FileFactory) {
        self.files.insert(name.to_string(), f);
    }
    pub fn register_query(&mut self, name: &str, f: QueryFactory) {
        self.query.insert(name.to_string(), f);
    }
    pub fn register_message(&mut self, name: &str, f: MessageFactory) {
        self.message.insert(name.to_string(), f);
    }

    /// Sorted built-in names per kind (for "unknown adapter" diagnostics).
    pub fn data_names(&self) -> Vec<&str> {
        sorted_keys(&self.data)
    }
    pub fn files_names(&self) -> Vec<&str> {
        sorted_keys(&self.files)
    }
    pub fn query_names(&self) -> Vec<&str> {
        sorted_keys(&self.query)
    }
    pub fn message_names(&self) -> Vec<&str> {
        sorted_keys(&self.message)
    }

    /// Build a built-in adapter by name; `Ok(None)` means no such name for this
    /// kind (the caller renders the BAD_REQUEST listing available names).
    pub fn build_data(
        &self,
        name: &str,
        config: &serde_json::Value,
    ) -> Result<Option<Arc<dyn DataStore>>, RsError> {
        self.data.get(name).map(|f| f(config)).transpose()
    }
    pub fn build_files(
        &self,
        name: &str,
        config: &serde_json::Value,
    ) -> Result<Option<Arc<dyn FileStore>>, RsError> {
        self.files.get(name).map(|f| f(config)).transpose()
    }
    pub fn build_query(
        &self,
        name: &str,
        config: &serde_json::Value,
    ) -> Result<Option<Arc<dyn QueryStore>>, RsError> {
        self.query.get(name).map(|f| f(config)).transpose()
    }
    pub fn build_message(
        &self,
        name: &str,
        config: &serde_json::Value,
        http: Arc<dyn HttpOut>,
    ) -> Result<Option<Arc<dyn MessageGateway>>, RsError> {
        self.message.get(name).map(|f| f(config, http)).transpose()
    }
}

fn sorted_keys<V>(map: &HashMap<String, V>) -> Vec<&str> {
    let mut names: Vec<&str> = map.keys().map(String::as_str).collect();
    names.sort_unstable();
    names
}
