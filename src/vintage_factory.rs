//! Vintage-backed construction for the application-facing CacheService.

use async_trait::async_trait;

use crate::{
    CacheNamespaceConf, CacheServiceConfig, CacheServiceError, CacheServiceFactory, Result,
};

/// Loads one cache-service namespace from Vintage statics config.
///
/// This implementation performs a single lookup during
/// [`crate::CacheService::new`]. It does not poll or update the resulting
/// cache after construction.
#[derive(Clone)]
pub struct VintageCacheServiceFactory {
    client: vintage::Client,
    group: String,
    namespace: String,
}

impl VintageCacheServiceFactory {
    /// Creates a factory for one statics-config group and cache namespace.
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
}

/// Errors produced while loading CacheService configuration from Vintage.
#[derive(Debug, thiserror::Error)]
pub enum VintageCacheServiceFactoryError {
    /// The Vintage lookup failed.
    #[error("vintage lookup failed: {0}")]
    Vintage(#[from] vintage::Error),

    /// The statics-config response did not contain its conventional `all`
    /// YAML entry.
    #[error("vintage statics config for group {group:?} has no \"all\" entry")]
    MissingAllEntry {
        /// The requested Vintage statics-config group.
        group: String,
    },

    /// The YAML was invalid or did not contain the requested namespace.
    #[error(transparent)]
    Config(#[from] CacheServiceError),
}

#[async_trait]
impl CacheServiceFactory for VintageCacheServiceFactory {
    async fn load(&self) -> Result<CacheNamespaceConf> {
        let snapshot = self
            .client
            .lookup_config(&self.group)
            .await
            .map_err(VintageCacheServiceFactoryError::from)?;
        let yaml = snapshot.all_entry_value().ok_or_else(|| {
            VintageCacheServiceFactoryError::MissingAllEntry {
                group: self.group.clone(),
            }
        })?;
        let config = CacheServiceConfig::from_yaml_str(yaml)
            .map_err(VintageCacheServiceFactoryError::from)?;
        config
            .namespace(&self.namespace)
            .cloned()
            .ok_or_else(|| {
                VintageCacheServiceFactoryError::Config(CacheServiceError::MissingNamespace(
                    self.namespace.clone(),
                ))
            })
            .map_err(crate::Error::from)
    }
}
