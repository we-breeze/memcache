//! One-shot Vintage-backed loading of a cache-service namespace config.
//!
//! This is a thin helper for the static construction path
//! ([`crate::CacheService::new`]): it performs a single Vintage
//! `lookup_config` and parses the requested namespace. It does not poll or
//! update — use [`crate::service::vintage_live::VintageCacheServiceConfigSource`]
//! with [`crate::CacheService::new_live`] for a hot-swapping cache.

use crate::{CacheNamespaceConf, CacheServiceConfig, CacheServiceError, Result};

/// Loads one cache-service namespace from a single Vintage statics-config lookup.
#[derive(Clone)]
pub struct VintageCacheServiceFactory {
    client: vintage::Client,
    group: String,
    namespace: String,
}

impl VintageCacheServiceFactory {
    /// Creates a loader for one statics-config group and cache namespace.
    pub fn new(
        client: vintage::Client,
        group: impl Into<String>,
        namespace: impl Into<String>,
    ) -> Self {
        Self {
            client,
            group: group.into(),
            namespace: namespace.into(),
        }
    }

    /// Performs one Vintage lookup and returns the parsed namespace config.
    pub async fn load(&self) -> Result<CacheNamespaceConf> {
        let snapshot = self
            .client
            .lookup_config(&self.group)
            .await
            .map_err(|e| crate::Error::Protocol(format!("vintage lookup failed: {e}")))?;
        let yaml = snapshot.all_entry_value().ok_or_else(|| {
            crate::Error::Protocol(format!(
                "vintage statics config for group {:?} has no \"all\" entry",
                self.group
            ))
        })?;
        let config = CacheServiceConfig::from_yaml_str(yaml)?;
        config.namespace(&self.namespace).cloned().ok_or_else(|| {
            crate::Error::from(CacheServiceError::MissingNamespace(self.namespace.clone()))
        })
    }
}
