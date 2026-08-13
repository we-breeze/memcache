//! Direct backend access (no mesh), the byTcp path: the client connects to
//! memcached backends directly and owns shard routing.
//!
//! - [`DirectClient`]: one `host:port` backend over the same pooled engine
//!   as the mesh path ([`crate::sidecar::SidecarClient`]), minus discovery.
//! - [`Shards`]: client-side shard routing across several backends using the
//!   same hash/distribution algorithms as the breeze mesh
//!   ([`crate::direct::sharding`]), so a key lands on the same backend
//!   whether it is routed by this client or by the mesh.
//!
//! For mesh access (discovery, no client-side sharding), see
//! [`crate::sidecar`]; for replay/comparison topologies, see
//! [`crate::replay`].

pub mod sharding;

pub mod ha;

use std::collections::HashMap;
use std::time::Duration;

use crate::config::{Config, Endpoint, Protocol};
use crate::error::Result;
use crate::expiration::Expiration;
use crate::sidecar::SidecarClient;
use crate::value::{CasValue, ToMemcacheValue, Value};

use sharding::Sharding;

pub use ha::{HaClient, HaConfig};

/// Configuration for one direct memcached backend.
#[derive(Clone, Debug)]
pub struct ServerConfig {
    /// Server host (IP or hostname resolvable at connect time).
    pub host: String,
    /// Server port.
    pub port: u16,
    /// Wire protocol (binary by default).
    pub protocol: Protocol,
    /// Minimum pooled connections (established at startup, kept topped up
    /// by the shared maintenance task; 0 = fully lazy).
    pub min_connections: usize,
    /// Maximum pooled connections.
    pub max_connections: usize,
    /// Timeout for establishing a new connection.
    pub connect_timeout: Duration,
    /// Per-operation timeout.
    pub op_timeout: Duration,
    /// Maximum time to wait for a pooled connection.
    pub pool_wait_timeout: Duration,
    /// Enable TCP keepalive.
    pub tcp_keepalive: bool,
    /// Idle time after which TCP keepalive probes start.
    pub keepalive_interval: Duration,
    /// Reject invalid keys client-side.
    pub validate_keys: bool,
}

impl ServerConfig {
    /// Parse `host:port` with direct-mode defaults.
    pub fn new(host_port: &str) -> Result<Self> {
        let (host, port) = host_port.rsplit_once(':').ok_or_else(|| {
            crate::error::Error::Client(format!(
                "invalid server address '{host_port}', expect host:port"
            ))
        })?;
        let port = port
            .parse::<u16>()
            .map_err(|_| crate::error::Error::Client(format!("invalid port in '{host_port}'")))?;
        Ok(ServerConfig {
            host: host.to_string(),
            port,
            protocol: Protocol::default(),
            min_connections: 2,
            max_connections: 128,
            connect_timeout: Duration::from_millis(500),
            op_timeout: Duration::from_millis(400),
            pool_wait_timeout: Duration::from_millis(500),
            tcp_keepalive: true,
            keepalive_interval: Duration::from_secs(10),
            validate_keys: true,
        })
    }

    /// Select the wire protocol.
    pub fn with_protocol(mut self, protocol: Protocol) -> Self {
        self.protocol = protocol;
        self
    }

    /// Override the minimum pooled connection count (0 = fully lazy).
    pub fn with_min_connections(mut self, n: usize) -> Self {
        self.min_connections = n;
        self
    }

    /// Override the maximum pooled connection count.
    pub fn with_max_connections(mut self, n: usize) -> Self {
        self.max_connections = n.max(1);
        self
    }

    /// Set the per-operation timeout.
    pub fn with_op_timeout(mut self, timeout: Duration) -> Self {
        self.op_timeout = timeout;
        self
    }

    /// The `host:port` label used in logs.
    pub fn label(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    /// Build the low-level engine [`Config`] (no mesh discovery).
    pub(crate) fn resolve(&self) -> Config {
        let mut config = Config::new(Endpoint {
            host: self.host.clone(),
            port: self.port,
        })
        .with_namespace(self.label());
        config.protocol = self.protocol;
        config.min_connections = self.min_connections;
        config.max_connections = self.max_connections;
        config.connect_timeout = self.connect_timeout;
        config.op_timeout = self.op_timeout;
        config.pool_wait_timeout = self.pool_wait_timeout;
        config.tcp_keepalive = self.tcp_keepalive;
        config.keepalive_interval = self.keepalive_interval;
        config.validate_keys = self.validate_keys;
        config
    }
}

/// One direct memcached backend. Cheap to clone (shares one pool).
#[derive(Clone)]
pub struct DirectClient {
    inner: SidecarClient,
}

impl DirectClient {
    /// Connect to one backend.
    pub fn connect(config: ServerConfig) -> Result<Self> {
        Ok(DirectClient {
            inner: SidecarClient::new(config.resolve())?,
        })
    }

    /// Fetch a single value.
    pub async fn get(&self, key: &str) -> Result<Option<Value>> {
        self.inner.get(key).await
    }

    /// Fetch multiple values in one round-trip.
    pub async fn get_multi(&self, keys: &[&str]) -> Result<HashMap<String, Value>> {
        self.inner.get_multi(keys).await
    }

    /// Fetch a value together with its CAS token.
    pub async fn get_cas(&self, key: &str) -> Result<Option<CasValue>> {
        self.inner.get_cas(key).await
    }

    /// Store a value unconditionally.
    pub async fn set(
        &self,
        key: &str,
        value: impl ToMemcacheValue,
        expire: impl Into<Expiration>,
    ) -> Result<bool> {
        self.inner.set(key, value, expire).await
    }

    /// Store only if the key does not already exist.
    pub async fn add(
        &self,
        key: &str,
        value: impl ToMemcacheValue,
        expire: impl Into<Expiration>,
    ) -> Result<bool> {
        self.inner.add(key, value, expire).await
    }

    /// Store only if the key already exists.
    pub async fn replace(
        &self,
        key: &str,
        value: impl ToMemcacheValue,
        expire: impl Into<Expiration>,
    ) -> Result<bool> {
        self.inner.replace(key, value, expire).await
    }

    /// Append data to an existing value.
    pub async fn append(&self, key: &str, value: impl ToMemcacheValue) -> Result<bool> {
        self.inner.append(key, value).await
    }

    /// Prepend data to an existing value.
    pub async fn prepend(&self, key: &str, value: impl ToMemcacheValue) -> Result<bool> {
        self.inner.prepend(key, value).await
    }

    /// Compare-and-swap.
    pub async fn cas(
        &self,
        key: &str,
        value: &CasValue,
        expire: impl Into<Expiration>,
    ) -> Result<bool> {
        self.inner.cas(key, value, expire).await
    }

    /// Delete a key.
    pub async fn delete(&self, key: &str) -> Result<bool> {
        self.inner.delete(key).await
    }

    /// Atomically increment a counter.
    pub async fn incr(&self, key: &str, delta: u64) -> Result<Option<u64>> {
        self.inner.incr(key, delta).await
    }

    /// Atomically decrement a counter.
    pub async fn decr(&self, key: &str, delta: u64) -> Result<Option<u64>> {
        self.inner.decr(key, delta).await
    }

    /// Update a key's expiration without fetching its value.
    pub async fn touch(&self, key: &str, expire: impl Into<Expiration>) -> Result<bool> {
        self.inner.touch(key, expire).await
    }

    /// Invalidate all items on the backend.
    pub async fn flush_all(&self) -> Result<()> {
        self.inner.flush_all().await
    }

    /// Query the backend version string.
    pub async fn version(&self) -> Result<String> {
        self.inner.version().await
    }
}

/// Client-side shard routing across several direct backends (the byTcp
/// `shardingSupport` pattern). Routes each key with the same hash and
/// distribution algorithms the mesh would use.
#[derive(Clone)]
pub struct Shards {
    sharding: Sharding,
    backends: Vec<DirectClient>,
}

impl Shards {
    /// Build from the resource's sharding configuration: the hash algorithm
    /// name (e.g. `crc32`), the distribution name (e.g. `modula`,
    /// `ketama`), the backend names (used as ketama ring nodes), and the
    /// connected per-backend clients.
    ///
    /// # Panics
    ///
    /// Panics if `names` and `clients` differ in length or are empty.
    pub fn new(
        hash: &str,
        distribution: &str,
        names: Vec<String>,
        clients: Vec<DirectClient>,
    ) -> Self {
        assert!(
            !clients.is_empty() && names.len() == clients.len(),
            "shards: names and clients must be non-empty and equally long"
        );
        Shards {
            sharding: Sharding::new(hash, distribution, &names),
            backends: clients,
        }
    }

    /// The client responsible for `key`.
    pub fn get_client(&self, key: &str) -> &DirectClient {
        &self.backends[self.sharding.shard_idx(key.as_bytes())]
    }

    /// The number of backends.
    pub fn len(&self) -> usize {
        self.backends.len()
    }

    /// Whether there are no backends (always false after construction).
    pub fn is_empty(&self) -> bool {
        self.backends.is_empty()
    }
}
