//! Application-facing memcache access through the local breeze sidecar.

use async_trait::async_trait;
use bytes::Bytes;

use crate::sidecar::{MeshConfig, SidecarClient};
use crate::value::Value;
use crate::{CacheEntry, CacheServiceOptions, Memcache, Result, SetOptions};

/// A [`Memcache`] implementation routed through the local breeze sidecar.
///
/// [`SidecarMemcache::new`] discovers the endpoint for an exact
/// `(group, namespace)` pair under `/data1/breeze/socks`. A numeric `mc` entry
/// connects to `<MESH_CONNECT_HOST-or-127.0.0.1>:<port>`; backend sharding and
/// failover remain owned by the sidecar.
#[derive(Clone)]
pub struct SidecarMemcache {
    client: SidecarClient,
    options: CacheServiceOptions,
}

impl SidecarMemcache {
    /// Discovers and constructs a sidecar-backed cache with default options.
    pub fn new(group: impl Into<String>, namespace: impl Into<String>) -> Result<Self> {
        Self::with_options(group, namespace, CacheServiceOptions::default())
    }

    /// Discovers and constructs a sidecar-backed cache with an instance-level
    /// default expiration.
    pub fn with_options(
        group: impl Into<String>,
        namespace: impl Into<String>,
        options: CacheServiceOptions,
    ) -> Result<Self> {
        let config = MeshConfig::new(namespace).with_group(group);
        Self::from_mesh_config(config, options)
    }

    /// Constructs the application facade from explicit sidecar settings.
    ///
    /// Normal application code should use [`SidecarMemcache::new`]. This
    /// constructor supports non-default socks directories and pool/timeout
    /// tuning in development and infrastructure composition code.
    pub fn from_mesh_config(config: MeshConfig, options: CacheServiceOptions) -> Result<Self> {
        Ok(Self {
            client: SidecarClient::from_config(config)?,
            options,
        })
    }
}

#[async_trait]
impl Memcache for SidecarMemcache {
    async fn get(&self, key: &str) -> Result<Option<CacheEntry>> {
        SidecarClient::get(&self.client, key)
            .await
            .map(|value| value.map(CacheEntry::from))
    }

    async fn set_with(&self, key: &str, value: Bytes, options: SetOptions) -> Result<bool> {
        let expiration = options
            .expiration
            .unwrap_or(self.options.default_expiration);
        let value = Value::new(value, options.flags.unwrap_or_default());
        SidecarClient::set(&self.client, key, value, expiration).await
    }
}
