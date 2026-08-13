//! Application-facing memcache access to one explicit backend endpoint.

use async_trait::async_trait;
use bytes::Bytes;

use crate::direct::{DirectClient, ServerConfig};
use crate::value::Value;
use crate::{CacheEntry, Expiration, Memcache, Result, SetOptions};

/// A [`Memcache`] implementation connected directly to one explicit endpoint.
///
/// This facade is intended for tests, validation tools, and other cases where
/// CacheService topology and sidecar discovery are deliberately bypassed.
#[derive(Clone)]
pub struct DirectMemcache {
    client: DirectClient,
    default_expiration: Expiration,
}

impl DirectMemcache {
    /// Connects to `host:port` using direct-mode defaults.
    pub fn new(endpoint: &str) -> Result<Self> {
        Self::from_server_config(ServerConfig::new(endpoint)?)
    }

    /// Connects using an explicit direct-backend configuration.
    pub fn from_server_config(config: ServerConfig) -> Result<Self> {
        Ok(Self {
            client: DirectClient::connect(config)?,
            default_expiration: Expiration::default(),
        })
    }

    /// Sets the expiration used when [`SetOptions::expiration`] is `None`.
    #[must_use]
    pub fn with_default_expiration(mut self, expiration: Expiration) -> Self {
        self.default_expiration = expiration;
        self
    }
}

#[async_trait]
impl Memcache for DirectMemcache {
    async fn get(&self, key: &str) -> Result<Option<CacheEntry>> {
        self.client
            .get(key)
            .await
            .map(|value| value.map(CacheEntry::from))
    }

    async fn set_with(&self, key: &str, value: Bytes, options: SetOptions) -> Result<bool> {
        let expiration = options.expiration.unwrap_or(self.default_expiration);
        let value = Value::new(value, options.flags.unwrap_or_default());
        self.client.set(key, value, expiration).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn implements_application_contract() {
        fn assert_memcache<T: Memcache>() {}
        assert_memcache::<DirectMemcache>();
    }

    #[test]
    fn rejects_an_invalid_endpoint_before_connecting() {
        assert!(DirectMemcache::new("missing-port").is_err());
    }
}
