//! In-memory [`DataStore`]: the default for tests and small single-node
//! deployments. Keys are ordered (BTreeMap) so listings paginate stably.

use std::collections::BTreeMap;

use async_trait::async_trait;
use tokio::sync::RwLock;

use crate::capabilities::DataStore;
use crate::error::RsError;

#[derive(Default)]
struct Dataset {
    records: BTreeMap<String, serde_json::Value>,
    schema: Option<serde_json::Value>,
}

#[derive(Default)]
pub struct MemDataStore {
    // tenant → dataset name → dataset
    tenants: RwLock<BTreeMap<String, BTreeMap<String, Dataset>>>,
}

impl MemDataStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl DataStore for MemDataStore {
    async fn get(&self, tenant: &str, dataset: &str, key: &str) -> Result<serde_json::Value, RsError> {
        self.tenants
            .read()
            .await
            .get(tenant)
            .and_then(|t| t.get(dataset))
            .and_then(|ds| ds.records.get(key))
            .cloned()
            .ok_or_else(|| RsError::not_found(format!("no record '{key}' in dataset '{dataset}'")))
    }

    async fn put(&self, tenant: &str, dataset: &str, key: &str, value: serde_json::Value) -> Result<bool, RsError> {
        let mut tenants = self.tenants.write().await;
        let ds = tenants
            .entry(tenant.to_string())
            .or_default()
            .entry(dataset.to_string())
            .or_default();
        Ok(ds.records.insert(key.to_string(), value).is_none())
    }

    async fn delete(&self, tenant: &str, dataset: &str, key: &str) -> Result<(), RsError> {
        let mut tenants = self.tenants.write().await;
        tenants
            .get_mut(tenant)
            .and_then(|t| t.get_mut(dataset))
            .and_then(|ds| ds.records.remove(key))
            .map(|_| ())
            .ok_or_else(|| RsError::not_found(format!("no record '{key}' in dataset '{dataset}'")))
    }

    async fn list_keys(&self, tenant: &str, dataset: &str, take: usize, skip: usize) -> Result<(Vec<String>, u64), RsError> {
        let tenants = self.tenants.read().await;
        let ds = tenants
            .get(tenant)
            .and_then(|t| t.get(dataset))
            .ok_or_else(|| RsError::not_found(format!("no dataset '{dataset}'")))?;
        let total = ds.records.len() as u64;
        let keys = ds.records.keys().skip(skip).take(take).cloned().collect();
        Ok((keys, total))
    }

    async fn list_datasets(&self, tenant: &str, take: usize, skip: usize) -> Result<(Vec<String>, u64), RsError> {
        let tenants = self.tenants.read().await;
        let Some(t) = tenants.get(tenant) else {
            return Ok((vec![], 0));
        };
        let total = t.len() as u64;
        let names = t.keys().skip(skip).take(take).cloned().collect();
        Ok((names, total))
    }

    async fn get_schema(&self, tenant: &str, dataset: &str) -> Result<Option<serde_json::Value>, RsError> {
        Ok(self
            .tenants
            .read()
            .await
            .get(tenant)
            .and_then(|t| t.get(dataset))
            .and_then(|ds| ds.schema.clone()))
    }

    async fn put_schema(&self, tenant: &str, dataset: &str, schema: serde_json::Value) -> Result<(), RsError> {
        let mut tenants = self.tenants.write().await;
        tenants
            .entry(tenant.to_string())
            .or_default()
            .entry(dataset.to_string())
            .or_default()
            .schema = Some(schema);
        Ok(())
    }

    async fn delete_dataset(&self, tenant: &str, dataset: &str) -> Result<(), RsError> {
        let mut tenants = self.tenants.write().await;
        tenants
            .get_mut(tenant)
            .and_then(|t| t.remove(dataset))
            .map(|_| ())
            .ok_or_else(|| RsError::not_found(format!("no dataset '{dataset}'")))
    }

    /// One pass under one read lock, cloning only the records that match —
    /// the default (`list_keys` + `get`) took the lock and deep-cloned once
    /// per record, matching or not.
    async fn scan_matching(
        &self,
        tenant: &str,
        dataset: &str,
        keep: &(dyn for<'a> Fn(&'a serde_json::Value) -> Result<bool, RsError> + Send + Sync),
    ) -> Result<Vec<(String, serde_json::Value)>, RsError> {
        let tenants = self.tenants.read().await;
        let ds = tenants
            .get(tenant)
            .and_then(|t| t.get(dataset))
            .ok_or_else(|| RsError::not_found(format!("no dataset '{dataset}'")))?;
        let mut out = Vec::new();
        for (key, record) in &ds.records {
            if keep(record)? {
                out.push((key.clone(), record.clone()));
            }
        }
        Ok(out)
    }
}
