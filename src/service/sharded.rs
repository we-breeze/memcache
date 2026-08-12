//! [`ShardedCache`] — a [`Cacheable`] over client-side sharded direct
//! memcached backends, built from a cache-service namespace configuration.
//!
//! This is the Rust analogue of the Java `MemCacheTemplate` backup built
//! from a `CacheServerPoolConf`: keys are routed to backends with the same
//! hash/distribution algorithms the mesh uses, via
//! [`crate::direct::Shards`].

use std::collections::HashMap;

use crate::cacheservice::CacheNamespaceConf;
use crate::direct::{DirectClient, ServerConfig, Shards};
use crate::value::{CasValue, Value};
use crate::{Error, Expiration, Result};
use async_trait::async_trait;

use super::Cacheable;

/// A [`Cacheable`] that routes each key to its shard's direct client.
#[derive(Clone)]
pub struct ShardedCache {
    shards: Shards,
}

impl ShardedCache {
    /// Wraps an existing shard router.
    pub fn new(shards: Shards) -> Self {
        ShardedCache { shards }
    }

    /// Builds a sharded backup from a cache-service namespace configuration:
    /// one direct client per `master` endpoint, routed with the namespace's
    /// `hash` / `distribution` (defaulting to `crc32` / `modula`, the mesh
    /// defaults).
    ///
    /// Note: the Java backup also wired slave / L1 pools; the Rust direct
    /// mode has no L1 concept, so only the master list is used.
    pub fn from_namespace_conf(conf: &CacheNamespaceConf) -> Result<Self> {
        let servers = conf.masters();
        if servers.is_empty() {
            return Err(Error::Client(
                "cache-service namespace has an empty master list".to_string(),
            ));
        }
        let mut clients = Vec::with_capacity(servers.len());
        for server in servers {
            clients.push(DirectClient::connect(ServerConfig::new(server)?)?);
        }
        let hash = conf.hash().unwrap_or("crc32");
        let distribution = conf.distribution().unwrap_or("modula");
        Ok(Self::new(Shards::new(
            hash,
            distribution,
            servers.to_vec(),
            clients,
        )))
    }

    fn client(&self, key: &str) -> DirectClient {
        self.shards.get_client(key).clone()
    }

    /// Groups keys by their shard. The references returned by `get_client`
    /// point into `Shards` and are stable, so the raw pointer identifies a
    /// shard; this helper keeps pointers out of the async future (raw
    /// pointers are not `Send`).
    fn group_by_shard<'k>(&self, keys: &[&'k str]) -> (Vec<DirectClient>, Vec<Vec<&'k str>>) {
        let mut shard_ptrs: Vec<*const DirectClient> = Vec::new();
        let mut shard_clients: Vec<DirectClient> = Vec::new();
        let mut buckets: Vec<Vec<&str>> = Vec::new();
        for key in keys.iter().copied() {
            let client = self.shards.get_client(key);
            let ptr = client as *const DirectClient;
            let bucket = match shard_ptrs.iter().position(|&p| p == ptr) {
                Some(index) => index,
                None => {
                    shard_ptrs.push(ptr);
                    shard_clients.push(client.clone());
                    buckets.push(Vec::new());
                    buckets.len() - 1
                }
            };
            buckets[bucket].push(key);
        }
        (shard_clients, buckets)
    }
}

#[async_trait]
impl Cacheable for ShardedCache {
    async fn get(&self, key: &str) -> Result<Option<Value>> {
        self.client(key).get(key).await
    }

    async fn get_multi(&self, keys: &[&str]) -> Result<HashMap<String, Value>> {
        // Group keys by shard so each backend gets one get_multi round-trip;
        // the per-shard requests run concurrently (sequential awaits would
        // make the call N shards x RTT).
        let (shard_clients, buckets) = self.group_by_shard(keys);

        let results = futures_util::future::join_all(
            shard_clients
                .iter()
                .zip(&buckets)
                .map(|(client, bucket)| client.get_multi(bucket)),
        )
        .await;
        let mut result = HashMap::with_capacity(keys.len());
        for partial in results {
            result.extend(partial?);
        }
        Ok(result)
    }

    async fn set(&self, key: &str, value: Value, expire: Expiration) -> Result<bool> {
        self.client(key).set(key, value, expire).await
    }

    async fn set_with_noreply(&self, key: &str, value: Value, expire: Expiration) -> Result<()> {
        self.client(key).set(key, value, expire).await?;
        Ok(())
    }

    async fn add(&self, key: &str, value: Value, expire: Expiration) -> Result<bool> {
        self.client(key).add(key, value, expire).await
    }

    async fn get_cas(&self, key: &str) -> Result<Option<CasValue>> {
        self.client(key).get_cas(key).await
    }

    async fn cas(&self, key: &str, value: &CasValue, expire: Expiration) -> Result<bool> {
        self.client(key).cas(key, value, expire).await
    }

    async fn delete(&self, key: &str) -> Result<bool> {
        self.client(key).delete(key).await
    }

    async fn delete_with_noreply(&self, key: &str) -> Result<()> {
        self.client(key).delete(key).await?;
        Ok(())
    }
}

impl From<Shards> for ShardedCache {
    fn from(shards: Shards) -> Self {
        Self::new(shards)
    }
}
