//! The application-facing memcache contract and CacheService implementation.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;

use crate::cacheservice::CacheNamespaceConf;
use crate::service::{Cacheable, MemCacheTemplate as Topology, PoolOptions};
use crate::value::Value;
use crate::{Expiration, Protocol, Result};

/// A value returned by [`Memcache::get`].
///
/// Memcached uses `0` for an untagged value on the wire. The public API maps
/// that common case to `None`, so callers only need to handle flags when their
/// serialization format actually uses them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CacheEntry {
    /// The stored bytes, without a copy from the protocol value.
    pub data: Bytes,
    /// The stored memcached flags, or `None` when the wire value is `0`.
    pub flags: Option<u32>,
}

impl From<Value> for CacheEntry {
    fn from(value: Value) -> Self {
        let flags = value.flags();
        Self {
            data: value.into_bytes(),
            flags: (flags != 0).then_some(flags),
        }
    }
}

/// Optional per-write overrides for [`Memcache::set_with`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SetOptions {
    /// Override the [`CacheService`] instance's default expiration.
    pub expiration: Option<Expiration>,
    /// Memcached flags; `None` is written as the normal untagged value `0`.
    pub flags: Option<u32>,
}

impl SetOptions {
    /// Sets the expiration override for this write.
    pub fn with_expiration(mut self, expiration: Expiration) -> Self {
        self.expiration = Some(expiration);
        self
    }

    /// Sets the memcached flags for this write.
    pub fn with_flags(mut self, flags: u32) -> Self {
        self.flags = Some(flags);
        self
    }
}

/// The small memcache contract consumed by application code.
#[async_trait]
pub trait Memcache: Send + Sync {
    /// Fetches one entry; a cache miss is `Ok(None)`.
    async fn get(&self, key: &str) -> Result<Option<CacheEntry>>;

    /// Stores bytes using the implementation's default expiration and no
    /// memcached flags.
    async fn set(&self, key: &str, value: Bytes) -> Result<bool> {
        self.set_with(key, value, SetOptions::default()).await
    }

    /// Stores bytes with optional per-write expiration and flag overrides.
    async fn set_with(&self, key: &str, value: Bytes, options: SetOptions) -> Result<bool>;
}

/// Supplies the parsed YAML namespace used to construct a [`CacheService`].
///
/// Applications normally use the SDK's `VintageCacheServiceFactory` (feature
/// `service`). The trait keeps construction testable and permits other
/// SDK-owned configuration sources.
#[async_trait]
pub trait CacheServiceFactory: Send + Sync {
    /// Loads the already-selected cache-service namespace configuration.
    async fn load(&self) -> Result<CacheNamespaceConf>;
}

/// Instance-level settings for [`CacheService`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CacheServiceOptions {
    /// Expiration used when [`SetOptions::expiration`] is `None`.
    pub default_expiration: Expiration,
    /// Wire protocol used by the direct topology built by [`CacheService`].
    ///
    /// This does not override the protocol of an explicitly supplied sidecar
    /// [`crate::sidecar::MeshConfig`].
    pub protocol: Protocol,
}

impl Default for CacheServiceOptions {
    fn default() -> Self {
        Self {
            default_expiration: Expiration::default(),
            protocol: Protocol::Text,
        }
    }
}

impl CacheServiceOptions {
    /// Creates options with an explicit default expiration.
    pub fn new(default_expiration: Expiration) -> Self {
        Self {
            default_expiration,
            ..Self::default()
        }
    }

    /// Selects the wire protocol for the direct cache-service topology.
    #[must_use]
    pub fn with_protocol(mut self, protocol: Protocol) -> Self {
        self.protocol = protocol;
        self
    }
}

/// A CacheService-backed [`Memcache`] implementation.
///
/// This is the stable application facade. It delegates topology behavior to
/// the Java-compatible master/slave/L1 implementation without exposing its
/// pools, builders, or broader `Cacheable` operation surface.
#[derive(Clone)]
pub struct CacheService {
    backend: Arc<dyn Cacheable>,
    options: CacheServiceOptions,
}

impl CacheService {
    /// Builds a cache with [`CacheServiceOptions::default`].
    pub async fn new(factory: impl CacheServiceFactory) -> Result<Self> {
        Self::with_options(factory, CacheServiceOptions::default()).await
    }

    /// Builds a cache with explicit instance-level options.
    pub async fn with_options(
        factory: impl CacheServiceFactory,
        options: CacheServiceOptions,
    ) -> Result<Self> {
        let namespace = factory.load().await?;
        let pool_options = PoolOptions::default().with_protocol(options.protocol);
        let topology = Topology::from_namespace_conf(&namespace, pool_options)?
            .with_default_expiration(options.default_expiration);
        Ok(Self::from_backend(Arc::new(topology), options))
    }

    fn from_backend(backend: Arc<dyn Cacheable>, options: CacheServiceOptions) -> Self {
        Self { backend, options }
    }
}

#[async_trait]
impl Memcache for CacheService {
    async fn get(&self, key: &str) -> Result<Option<CacheEntry>> {
        self.backend
            .get(key)
            .await
            .map(|value| value.map(CacheEntry::from))
    }

    async fn set_with(&self, key: &str, value: Bytes, options: SetOptions) -> Result<bool> {
        let expiration = options
            .expiration
            .unwrap_or(self.options.default_expiration);
        let value = Value::new(value, options.flags.unwrap_or_default());
        self.backend.set(key, value, expiration).await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use crate::value::CasValue;

    use super::*;

    #[test]
    fn cache_service_options_default_to_text_protocol() {
        assert_eq!(CacheServiceOptions::default().protocol, Protocol::Text);
    }

    #[test]
    fn cache_service_options_can_select_binary_protocol() {
        let options = CacheServiceOptions::default().with_protocol(Protocol::Binary);

        assert_eq!(options.protocol, Protocol::Binary);
    }

    #[derive(Default)]
    struct RecordingBackend {
        get_result: Mutex<Option<Value>>,
        set_call: Mutex<Option<(String, Value, Expiration)>>,
    }

    #[async_trait]
    impl Cacheable for RecordingBackend {
        async fn get(&self, _key: &str) -> Result<Option<Value>> {
            Ok(self.get_result.lock().unwrap().clone())
        }

        async fn get_multi(&self, _keys: &[&str]) -> Result<HashMap<String, Value>> {
            unreachable!("the public API does not expose get_multi")
        }

        async fn set(&self, key: &str, value: Value, expire: Expiration) -> Result<bool> {
            *self.set_call.lock().unwrap() = Some((key.to_owned(), value, expire));
            Ok(true)
        }

        async fn set_with_noreply(
            &self,
            _key: &str,
            _value: Value,
            _expire: Expiration,
        ) -> Result<()> {
            unreachable!("the public API does not expose set_with_noreply")
        }

        async fn add(&self, _key: &str, _value: Value, _expire: Expiration) -> Result<bool> {
            unreachable!("the public API does not expose add")
        }

        async fn get_cas(&self, _key: &str) -> Result<Option<CasValue>> {
            unreachable!("the public API does not expose get_cas")
        }

        async fn cas(&self, _key: &str, _value: &CasValue, _expire: Expiration) -> Result<bool> {
            unreachable!("the public API does not expose cas")
        }

        async fn delete(&self, _key: &str) -> Result<bool> {
            unreachable!("the public API does not expose delete")
        }

        async fn delete_with_noreply(&self, _key: &str) -> Result<()> {
            unreachable!("the public API does not expose delete_with_noreply")
        }
    }

    #[tokio::test]
    async fn set_uses_instance_expiration_and_zero_flags_by_default() {
        let backend = Arc::new(RecordingBackend::default());
        let cache = CacheService::from_backend(
            backend.clone(),
            CacheServiceOptions::new(Expiration::Seconds(90)),
        );

        assert!(
            cache
                .set("user:1", Bytes::from_static(b"value"))
                .await
                .unwrap()
        );

        let call = backend.set_call.lock().unwrap().clone().unwrap();
        assert_eq!(call.0, "user:1");
        assert_eq!(call.1.as_bytes(), b"value");
        assert_eq!(call.1.flags(), 0);
        assert_eq!(call.2, Expiration::Seconds(90));
    }

    #[tokio::test]
    async fn set_with_overrides_expiration_and_flags() {
        let backend = Arc::new(RecordingBackend::default());
        let cache = CacheService::from_backend(
            backend.clone(),
            CacheServiceOptions::new(Expiration::Seconds(90)),
        );
        let options = SetOptions::default()
            .with_expiration(Expiration::Seconds(5))
            .with_flags(32);

        cache
            .set_with("user:2", Bytes::from_static(b"value"), options)
            .await
            .unwrap();

        let call = backend.set_call.lock().unwrap().clone().unwrap();
        assert_eq!(call.1.flags(), 32);
        assert_eq!(call.2, Expiration::Seconds(5));
    }

    #[tokio::test]
    async fn get_maps_wire_zero_flag_to_none_without_copying_data() {
        let backend = Arc::new(RecordingBackend::default());
        *backend.get_result.lock().unwrap() = Some(Value::new(Bytes::from_static(b"value"), 0));
        let cache = CacheService::from_backend(backend, CacheServiceOptions::default());

        let entry = cache.get("user:1").await.unwrap().unwrap();

        assert_eq!(entry.data, Bytes::from_static(b"value"));
        assert_eq!(entry.flags, None);
    }

    #[tokio::test]
    async fn public_contract_is_object_safe() {
        let backend = Arc::new(RecordingBackend::default());
        *backend.get_result.lock().unwrap() = Some(Value::new(Bytes::new(), 7));
        let cache: Arc<dyn Memcache> = Arc::new(CacheService::from_backend(
            backend,
            CacheServiceOptions::default(),
        ));

        assert_eq!(cache.get("key").await.unwrap().unwrap().flags, Some(7));
    }
}
