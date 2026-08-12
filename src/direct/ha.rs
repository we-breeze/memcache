//! High-availability direct access: a master/slave topology client
//! (the Java `MemcachedClient` HA pattern).
//!
//! A cacheservice namespace configures up to three tiers (see
//! [`crate::cacheservice::CacheNamespaceConf`]):
//!
//! - **master** — the write tier (and the last read resort);
//! - **slave_l1** — the L1 read tier (same-DC slaves), read first;
//! - **slave** — the L2 read tier, read when L1 misses.
//!
//! [`HaClient`] routes **writes to the master tier** (optionally
//! double-writing the slave tier) and **reads down the fallback chain**
//! `slave_l1 → slave → master`, so a stale or missing L1 copy degrades to a
//! slower read instead of an error. Each tier with several endpoints is
//! sharded with the resource's hash/distribution (via [`super::Shards`]),
//! matching the mesh's routing.

use std::collections::HashMap;

use crate::error::Result;
use crate::expiration::Expiration;
use crate::value::{CasValue, ToMemcacheValue, Value};

use super::{DirectClient, ServerConfig, Shards};

/// Configuration for an [`HaClient`]: endpoint lists per tier plus the
/// resource's sharding names. Build one per tier pool with
/// [`HaConfig::server`].
#[derive(Clone, Debug)]
pub struct HaConfig {
    /// Master (write) endpoints, `host:port`, in configured order.
    pub masters: Vec<String>,
    /// L1 slave (read-first) endpoints, flattened from the cacheservice
    /// `slave_l1` groups. May be empty.
    pub slave_l1: Vec<String>,
    /// L2 slave (read-next) endpoints. May be empty.
    pub slaves: Vec<String>,
    /// Hash algorithm name (e.g. `crc32`).
    pub hash: String,
    /// Distribution name (e.g. `modula`, `ketama`).
    pub distribution: String,
    /// Also write the slave tier (`set`/`add`/`replace`/`delete`), keeping
    /// the read tiers warm. Slave-write failures are logged, not fatal.
    pub write_slave: bool,
    /// Per-server pool/timeout settings template.
    pub server: ServerConfig,
}

impl HaConfig {
    /// A config from endpoint lists with defaults (crc32/modula, no
    /// double-write).
    pub fn new(masters: Vec<String>) -> Self {
        HaConfig {
            masters,
            slave_l1: Vec::new(),
            slaves: Vec::new(),
            hash: "crc32".to_string(),
            distribution: "modula".to_string(),
            write_slave: false,
            server: ServerConfig::new("127.0.0.1:11211").expect("default address"),
        }
    }

    /// Set the L1 slave endpoints.
    pub fn with_slave_l1(mut self, endpoints: Vec<String>) -> Self {
        self.slave_l1 = endpoints;
        self
    }

    /// Set the L2 slave endpoints.
    pub fn with_slaves(mut self, endpoints: Vec<String>) -> Self {
        self.slaves = endpoints;
        self
    }

    /// Set the hash/distribution names (from the cacheservice config).
    pub fn with_sharding(mut self, hash: &str, distribution: &str) -> Self {
        self.hash = hash.to_string();
        self.distribution = distribution.to_string();
        self
    }

    /// Enable or disable slave double-write.
    pub fn with_write_slave(mut self, write_slave: bool) -> Self {
        self.write_slave = write_slave;
        self
    }

    /// Set the per-server settings template (pool sizes, timeouts, ...).
    pub fn with_server(mut self, server: ServerConfig) -> Self {
        self.server = server;
        self
    }

    /// Build from a parsed cacheservice namespace block: masters, flattened
    /// L1 groups, slaves, and the sharding names.
    pub fn from_namespace(conf: &crate::cacheservice::CacheNamespaceConf) -> Result<Self> {
        let mut config = Self::new(conf.masters().to_vec());
        if config.masters.is_empty() {
            return Err(crate::error::Error::Client(
                "cacheservice namespace has no master endpoints".into(),
            ));
        }
        config.slave_l1 = conf.slave_l1().concat();
        config.slaves = conf.slaves().to_vec();
        if let (Some(hash), Some(distribution)) = (conf.hash(), conf.distribution()) {
            config.hash = hash.to_string();
            config.distribution = distribution.to_string();
        }
        Ok(config)
    }
}

/// A master/slave HA client. Cheap to clone (all tiers share their pools).
#[derive(Clone)]
pub struct HaClient {
    /// The write tier (and final read resort).
    master: Shards,
    /// L1 read tier, read first when present.
    slave_l1: Option<Shards>,
    /// L2 read tier, read next when present.
    slave: Option<Shards>,
    write_slave: bool,
}

impl HaClient {
    /// Connect all tiers.
    pub fn connect(config: HaConfig) -> Result<Self> {
        let build = |endpoints: &[String]| -> Result<Shards> {
            let mut clients = Vec::with_capacity(endpoints.len());
            for addr in endpoints {
                let server = ServerConfig::new(addr)?;
                let server = ServerConfig {
                    protocol: config.server.protocol,
                    min_connections: config.server.min_connections,
                    max_connections: config.server.max_connections,
                    connect_timeout: config.server.connect_timeout,
                    op_timeout: config.server.op_timeout,
                    pool_wait_timeout: config.server.pool_wait_timeout,
                    tcp_keepalive: config.server.tcp_keepalive,
                    keepalive_interval: config.server.keepalive_interval,
                    validate_keys: config.server.validate_keys,
                    ..server
                };
                clients.push(DirectClient::connect(server)?);
            }
            Ok(Shards::new(
                &config.hash,
                &config.distribution,
                endpoints.to_vec(),
                clients,
            ))
        };
        Ok(HaClient {
            master: build(&config.masters)?,
            slave_l1: if config.slave_l1.is_empty() {
                None
            } else {
                Some(build(&config.slave_l1)?)
            },
            slave: if config.slaves.is_empty() {
                None
            } else {
                Some(build(&config.slaves)?)
            },
            write_slave: config.write_slave,
        })
    }

    /// Fetch a single value down the read chain `slave_l1 → slave → master`.
    pub async fn get(&self, key: &str) -> Result<Option<Value>> {
        if let Some(l1) = &self.slave_l1 {
            match l1.get_client(key).get(key).await {
                hit @ Ok(Some(_)) => return hit,
                Ok(None) => {}
                Err(err) => {
                    tracing::warn!(error = %err, key, "mc ha: l1 read failed, falling back")
                }
            }
        }
        if let Some(slave) = &self.slave {
            match slave.get_client(key).get(key).await {
                hit @ Ok(Some(_)) => return hit,
                Ok(None) => {}
                Err(err) => {
                    tracing::warn!(error = %err, key, "mc ha: slave read failed, falling back")
                }
            }
        }
        self.master.get_client(key).get(key).await
    }

    /// Fetch several values; each key follows the read chain.
    pub async fn get_multi(&self, keys: &[&str]) -> Result<HashMap<String, Value>> {
        let mut map = HashMap::with_capacity(keys.len());
        for key in keys {
            if let Some(value) = self.get(key).await? {
                map.insert((*key).to_string(), value);
            }
        }
        Ok(map)
    }

    /// Fetch a value with its CAS token (from the master tier, since CAS
    /// pairs with a later write).
    pub async fn get_cas(&self, key: &str) -> Result<Option<CasValue>> {
        self.master.get_client(key).get_cas(key).await
    }

    /// Store unconditionally: master tier, plus the slave tier when
    /// double-write is enabled.
    pub async fn set(
        &self,
        key: &str,
        value: impl ToMemcacheValue,
        expire: impl Into<Expiration>,
    ) -> Result<bool> {
        let value = value.to_memcache_value();
        let expire = expire.into();
        if self.write_slave
            && let Some(slave) = &self.slave
        {
            let slave = slave.clone();
            let key = key.to_string();
            let value = value.clone();
            // Double-write is best-effort: log and continue on failure.
            tokio::spawn(async move {
                if let Err(err) = slave.get_client(&key).set(&key, value, expire).await {
                    tracing::warn!(error = %err, key, "mc ha: slave double-write failed");
                }
            });
        }
        self.master.get_client(key).set(key, value, expire).await
    }

    /// Store only if the key does not already exist (master tier).
    pub async fn add(
        &self,
        key: &str,
        value: impl ToMemcacheValue,
        expire: impl Into<Expiration>,
    ) -> Result<bool> {
        self.master.get_client(key).add(key, value, expire).await
    }

    /// Store only if the key already exists (master tier).
    pub async fn replace(
        &self,
        key: &str,
        value: impl ToMemcacheValue,
        expire: impl Into<Expiration>,
    ) -> Result<bool> {
        self.master
            .get_client(key)
            .replace(key, value, expire)
            .await
    }

    /// Compare-and-swap (master tier).
    pub async fn cas(
        &self,
        key: &str,
        value: &CasValue,
        expire: impl Into<Expiration>,
    ) -> Result<bool> {
        self.master.get_client(key).cas(key, value, expire).await
    }

    /// Delete a key from the master tier (and the slave tier when
    /// double-write is enabled).
    pub async fn delete(&self, key: &str) -> Result<bool> {
        if self.write_slave
            && let Some(slave) = &self.slave
        {
            let slave = slave.clone();
            let key = key.to_string();
            tokio::spawn(async move {
                if let Err(err) = slave.get_client(&key).delete(&key).await {
                    tracing::warn!(error = %err, key, "mc ha: slave delete failed");
                }
            });
        }
        self.master.get_client(key).delete(key).await
    }

    /// Increment a counter (master tier).
    pub async fn incr(&self, key: &str, delta: u64) -> Result<Option<u64>> {
        self.master.get_client(key).incr(key, delta).await
    }

    /// Decrement a counter (master tier).
    pub async fn decr(&self, key: &str, delta: u64) -> Result<Option<u64>> {
        self.master.get_client(key).decr(key, delta).await
    }

    /// Update expiration (master tier).
    pub async fn touch(&self, key: &str, expire: impl Into<Expiration>) -> Result<bool> {
        self.master.get_client(key).touch(key, expire).await
    }

    /// The master tier (for diagnostics).
    pub fn master(&self) -> &Shards {
        &self.master
    }
}
