use stor_port::{
    pstor::{
        etcd::Etcd, key_prefix_obj, ObjectKey, StorableObject, StorableObjectType, StoreKv,
        StoreObj, API_VERSION,
    },
    types::v0::store::dr::DrClusterPairSpec,
};

use futures::lock::Mutex;
use http::Uri;
use std::{sync::Arc, time::Duration};
use tracing::error;

/// The dr agent's own handle on the persistent store.
///
/// Core is not in this path: the pair is the dr agent's resource, and the dr agent reads
/// it on every cycle, so putting it behind another agent's api would buy nothing and cost
/// a hop. This mirrors what the ha agent does with its switchover requests.
#[derive(Debug, Clone)]
pub(crate) struct PairStore {
    store: Arc<Mutex<Etcd>>,
    timeout: Duration,
}

impl PairStore {
    /// Connect to the persistent store.
    pub(crate) async fn new(endpoint: &Uri, timeout: Duration) -> anyhow::Result<Self> {
        let store = tokio::time::timeout(timeout, Etcd::new(&endpoint.to_string()))
            .await
            .map_err(|_| anyhow::anyhow!("timed out connecting to the persistent store"))??;
        Ok(Self {
            store: Arc::new(Mutex::new(store)),
            timeout,
        })
    }

    /// Write a pair.
    pub(crate) async fn put(&self, spec: &DrClusterPairSpec) -> anyhow::Result<()> {
        let mut store = self.store.lock().await;
        tokio::time::timeout(self.timeout, store.put_obj(spec))
            .await
            .map_err(|_| anyhow::anyhow!("timed out writing to the persistent store"))??;
        Ok(())
    }

    /// Remove a pair.
    pub(crate) async fn delete(&self, spec: &DrClusterPairSpec) -> anyhow::Result<()> {
        let mut store = self.store.lock().await;
        tokio::time::timeout(self.timeout, store.delete_kv(&spec.key().key()))
            .await
            .map_err(|_| anyhow::anyhow!("timed out deleting from the persistent store"))??;
        Ok(())
    }

    /// Every pair this cluster has stored.
    ///
    /// Read once at startup: without it a restarted agent does not know the relationships
    /// it is already party to.
    pub(crate) async fn list(&self) -> anyhow::Result<Vec<DrClusterPairSpec>> {
        let mut store = self.store.lock().await;
        let prefix = key_prefix_obj(StorableObjectType::DrClusterPairSpec, API_VERSION);
        let entries = tokio::time::timeout(self.timeout, store.get_values_prefix(&prefix))
            .await
            .map_err(|_| anyhow::anyhow!("timed out reading the persistent store"))??;
        Ok(entries
            .into_iter()
            .filter_map(|(key, value)| match serde_json::from_value(value) {
                Ok(spec) => Some(spec),
                Err(error) => {
                    // One unreadable entry should not stop the agent starting, but it must
                    // be visible: the relationship it describes will not be served.
                    error!(%key, %error, "Ignoring an unreadable dr cluster pair");
                    None
                }
            })
            .collect())
    }
}
