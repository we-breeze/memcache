//! [`MemCacheTemplate`] — the Rust port of the Java `commons-memcache`
//! `MemCacheTemplate`: a multi-tier (master / slave / L1) backup cache with
//! the same read cascade, write fan-out, and policy switches.
//!
//! Tier model ("m" = master + masterL1, "s" = slave + slaveL1):
//!
//! - **get**: try one master-L1 pool (round-robin, with `master_as_one_l1`
//!   giving the master itself a 1/(n+1) share), then master, then slave;
//!   slave hits are set back into master (`setback_master`) and master/slave
//!   hits are set back into the consulted L1 with the L1 expire.
//! - **set/add/cas**: master first; on master failure `set` aborts unless
//!   `force_write_all`; slave and L1 pools follow the [`WritePolicy`]
//!   (`update_master_l1` / `update_slave_l1` gate the L1 fan-out).
//! - **delete**: master, slave, master-L1 (always), slave-L1 (gated),
//!   extend-slave-L1.
//!
//! The Java local-cache hot-key layer (`CacheWrapper` / `LocalCacheMonitor`)
//! is not ported: it depends on infrastructure with no Rust equivalent, and
//! its switcher defaults to off upstream.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crate::cacheservice::CacheNamespaceConf;
use crate::direct::ServerConfig;
use crate::value::{CasValue, Value};
use crate::{Error, Expiration, Protocol, Result};
use async_trait::async_trait;
use tracing::warn;

use super::{Cacheable, ShardedCache};

/// How writes propagate to the L1 pools (the Java `wirtePolicy` strings).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum WritePolicy {
    /// `writeAll` — set every L1 pool.
    #[default]
    WriteAll,
    /// `writeAndDeleteL1` — set master/slave, delete the L1 pools (keeps L1
    /// hot by forcing a re-read).
    WriteAndDeleteL1,
    /// `writeAndIfExistL1` — set master/slave, set an L1 pool only if the key
    /// already exists there.
    WriteAndIfExistL1,
}

impl WritePolicy {
    /// Parses the Java policy strings; unknown values fall back to
    /// [`WritePolicy::WriteAll`], like the Java `else` branch.
    pub fn parse(policy: &str) -> Self {
        match policy {
            "writeAndDeleteL1" => WritePolicy::WriteAndDeleteL1,
            "writeAndIfExistL1" => WritePolicy::WriteAndIfExistL1,
            _ => WritePolicy::WriteAll,
        }
    }
}

/// Per-pool connection tuning applied when building from a namespace
/// configuration (the Java `minConnections` / `maxConnections`).
#[derive(Clone, Copy, Debug, Default)]
pub struct PoolOptions {
    /// Minimum pooled connections per backend (0 = fully lazy).
    pub min_connections: Option<usize>,
    /// Maximum pooled connections per backend.
    pub max_connections: Option<usize>,
    /// Wire protocol used by every backend in the topology (binary by
    /// default for callers that construct [`MemCacheTemplate`] directly).
    pub protocol: Protocol,
}

impl PoolOptions {
    /// Selects the wire protocol for every backend in the topology.
    #[must_use]
    pub fn with_protocol(mut self, protocol: Protocol) -> Self {
        self.protocol = protocol;
        self
    }

    fn apply(self, mut config: ServerConfig) -> ServerConfig {
        config = config.with_protocol(self.protocol);
        if let Some(n) = self.min_connections {
            config = config.with_min_connections(n);
        }
        if let Some(n) = self.max_connections {
            config = config.with_max_connections(n);
        }
        config
    }
}

type Pool = Arc<dyn Cacheable>;

/// The multi-tier backup cache, ported from the Java `MemCacheTemplate`.
///
/// Pools are `Arc<dyn Cacheable>`: built from direct sharded clients via
/// [`MemCacheTemplate::from_namespace_conf`], or injected directly for
/// tests and custom topologies.
#[derive(Clone)]
pub struct MemCacheTemplate {
    master: Option<Pool>,
    slave: Option<Pool>,
    master_l1: Vec<Pool>,
    slave_l1: Vec<Pool>,
    extend_slave_l1: Vec<Pool>,

    expire: Expiration,
    expire_l1: Option<Expiration>,

    read_only: Arc<AtomicBool>,
    update_slave_l1: Arc<AtomicBool>,
    update_master_l1: Arc<AtomicBool>,
    force_write_all: bool,
    write_policy: WritePolicy,
    setback_master: bool,
    master_as_one_l1: bool,
    async_write_l1: bool,
    async_write_ext_l1: bool,

    /// Round-robin cursor for L1 selection (the Java `point`).
    l1_point: Arc<AtomicU64>,
}

impl MemCacheTemplate {
    /// Starts building a template with empty pools and Java defaults:
    /// `setback_master = true`, `master_as_one_l1 = true`,
    /// `update_master_l1 = update_slave_l1 = true`, `read_only =
    /// force_write_all = async_* = false`, `writeAll` policy.
    pub fn builder() -> MemCacheTemplateBuilder {
        MemCacheTemplateBuilder::default()
    }

    /// Builds a template from a cache-service namespace configuration: one
    /// sharded pool per master / slave endpoint list and per L1 group,
    /// routed with the namespace's hash/distribution.
    pub fn from_namespace_conf(conf: &CacheNamespaceConf, options: PoolOptions) -> Result<Self> {
        let hash = conf.hash().unwrap_or("crc32");
        let distribution = conf.distribution().unwrap_or("modula");
        let pool = |servers: &[String]| -> Result<Pool> {
            let mut clients = Vec::with_capacity(servers.len());
            for server in servers {
                clients.push(crate::direct::DirectClient::connect(
                    options.apply(ServerConfig::new(server)?),
                )?);
            }
            Ok(Arc::new(ShardedCache::new(crate::direct::Shards::new(
                hash,
                distribution,
                servers.to_vec(),
                clients,
            ))))
        };

        if conf.masters().is_empty() {
            return Err(Error::Client(
                "cache-service namespace has an empty master list".to_string(),
            ));
        }

        let mut builder = MemCacheTemplate::builder().master(pool(conf.masters())?);
        if !conf.slaves().is_empty() {
            builder = builder.slave(pool(conf.slaves())?);
        }
        for group in conf.master_l1() {
            if !group.is_empty() {
                builder = builder.master_l1_pool(pool(group)?);
            }
        }
        for group in conf.slave_l1() {
            if !group.is_empty() {
                builder = builder.slave_l1_pool(pool(group)?);
            }
        }
        Ok(builder.build())
    }

    /// Overrides the write and read-repair expiration used by this topology.
    pub(crate) fn with_default_expiration(mut self, expiration: Expiration) -> Self {
        self.expire = expiration;
        self
    }

    /// The default expiration for writes issued through the inherent
    /// methods (the Java `expireTime`).
    pub fn expire(&self) -> Expiration {
        self.expire
    }

    /// The L1 write-back expiration (the Java `getExpireTimeL1`: the L1
    /// expire if set, else the main expire).
    pub fn expire_l1(&self) -> Expiration {
        self.expire_l1.unwrap_or(self.expire)
    }

    /// The Java `readOnly` switch: writes become no-ops returning success.
    pub fn set_read_only(&self, on: bool) {
        self.read_only.store(on, Ordering::Relaxed);
    }

    /// The Java `updateSlaveL1` switch.
    pub fn set_update_slave_l1(&self, on: bool) {
        self.update_slave_l1.store(on, Ordering::Relaxed);
    }

    /// The Java `updateMasterL1` switch.
    pub fn set_update_master_l1(&self, on: bool) {
        self.update_master_l1.store(on, Ordering::Relaxed);
    }

    fn is_read_only(&self) -> bool {
        self.read_only.load(Ordering::Relaxed)
    }

    /// The Java `chooseOneL1Client`: round-robins over the master-L1 pools;
    /// with `master_as_one_l1` one slot in `len + 1` penetrates to the
    /// master (returns `None`).
    fn choose_one_l1(&self) -> Option<&Pool> {
        let len = self.master_l1.len();
        if len == 0 {
            return None;
        }
        // Increment-then-mod, like the Java `point.incrementAndGet()`.
        let v = self.l1_point.fetch_add(1, Ordering::Relaxed) + 1;
        let modulus = if self.master_as_one_l1 { len + 1 } else { len } as u64;
        let slot = (v % modulus) as usize;
        if slot >= len {
            None
        } else {
            Some(&self.master_l1[slot])
        }
    }

    fn require_master(&self) -> Result<&Pool> {
        self.master
            .as_ref()
            .ok_or_else(|| Error::Client("MemCacheTemplate master is null".to_string()))
    }

    /// Read from the master pool only (the Java `getMasterNode`).
    pub async fn get_master_node(&self, key: &str) -> Result<Option<Value>> {
        self.require_master()?.get(key).await
    }

    /// Store with the template's default expiration.
    pub async fn set(&self, key: &str, value: Value) -> Result<bool> {
        Cacheable::set(self, key, value, self.expire).await
    }

    /// Add with the template's default expiration.
    pub async fn add(&self, key: &str, value: Value) -> Result<bool> {
        Cacheable::add(self, key, value, self.expire).await
    }

    /// Compare-and-swap with the template's default expiration.
    pub async fn cas(&self, key: &str, value: &CasValue) -> Result<bool> {
        Cacheable::cas(self, key, value, self.expire).await
    }

    /// Applies the write policy to one L1 pool list, optionally spawning the
    /// work onto the runtime (the Java `asyncWriteL1` / `asyncWriteExtL1`
    /// executor).
    async fn write_l1_pools(
        &self,
        pools: &[Pool],
        key: &str,
        value: &Value,
        expire: Expiration,
        async_write: bool,
    ) {
        if async_write {
            for pool in pools {
                let (pool, key, value) = (pool.clone(), key.to_string(), value.clone());
                let policy = self.write_policy;
                tokio::spawn(async move {
                    if let Err(error) = apply_write_policy(&pool, policy, &key, value, expire).await
                    {
                        warn!(%error, "L1 pool write failed");
                    }
                });
            }
            return;
        }
        // Synchronous path: the pool writes are independent, so run them
        // concurrently instead of paying one RTT per pool.
        futures_util::future::join_all(pools.iter().map(|pool| async move {
            if let Err(error) =
                apply_write_policy(pool, self.write_policy, key, value.clone(), expire).await
            {
                warn!(%error, "L1 pool write failed");
            }
        }))
        .await;
    }

    async fn delete_l1_pools(&self, pools: &[Pool], key: &str, async_write: bool) {
        if async_write {
            for pool in pools {
                let (pool, key) = (pool.clone(), key.to_string());
                tokio::spawn(async move {
                    if let Err(error) = pool.delete(&key).await {
                        warn!(%error, "L1 pool delete failed");
                    }
                });
            }
            return;
        }
        futures_util::future::join_all(pools.iter().map(|pool| async move {
            if let Err(error) = pool.delete(key).await {
                warn!(%error, "L1 pool delete failed");
            }
        }))
        .await;
    }

    /// Fans a write out to slave + L1 tiers after the master succeeded
    /// (shared tail of the Java `set` / `add` / `cas`). The tiers are
    /// independent, so the fan-out runs concurrently — sequential awaits
    /// would stack one RTT per tier onto the write latency.
    async fn fan_out_write(&self, key: &str, value: &Value, expire: Expiration) {
        let mut pending: Vec<std::pin::Pin<Box<dyn Future<Output = ()> + Send + '_>>> = Vec::new();
        if let Some(slave) = &self.slave {
            pending.push(Box::pin(async move {
                if let Err(error) = slave.set(key, value.clone(), expire).await {
                    warn!(%error, "slave pool write failed");
                }
            }));
        }
        if self.update_master_l1.load(Ordering::Relaxed) {
            pending.push(Box::pin(self.write_l1_pools(
                &self.master_l1,
                key,
                value,
                expire,
                self.async_write_l1,
            )));
        }
        if self.update_slave_l1.load(Ordering::Relaxed) {
            pending.push(Box::pin(self.write_l1_pools(
                &self.slave_l1,
                key,
                value,
                expire,
                self.async_write_l1,
            )));
        }
        pending.push(Box::pin(self.write_l1_pools(
            &self.extend_slave_l1,
            key,
            value,
            expire,
            self.async_write_ext_l1,
        )));
        futures_util::future::join_all(pending).await;
    }
}

/// One L1 write under the configured [`WritePolicy`].
async fn apply_write_policy(
    pool: &Pool,
    policy: WritePolicy,
    key: &str,
    value: Value,
    expire: Expiration,
) -> Result<()> {
    match policy {
        WritePolicy::WriteAll => pool.set(key, value, expire).await.map(|_| ()),
        WritePolicy::WriteAndDeleteL1 => pool.delete(key).await.map(|_| ()),
        WritePolicy::WriteAndIfExistL1 => match pool.get(key).await {
            Ok(Some(_)) => pool.set(key, value, expire).await.map(|_| ()),
            Ok(None) => Ok(()),
            Err(e) => Err(e),
        },
    }
}

#[async_trait]
impl Cacheable for MemCacheTemplate {
    async fn get(&self, key: &str) -> Result<Option<Value>> {
        let one_l1 = self.choose_one_l1().cloned();
        if let Some(l1) = &one_l1 {
            match l1.get(key).await {
                // L1 hit returns directly, like the Java fixed path.
                Ok(Some(value)) => return Ok(Some(value)),
                Ok(None) => {}
                // A failed tier degrades the read to the next tier instead
                // of failing the whole operation.
                Err(error) => warn!(%error, key, "L1 read failed; falling back"),
            }
        }

        let mut last_error = None;
        let mut master_answered = false;
        let mut slave_answered = false;
        let mut value = None;
        if let Some(master) = &self.master {
            match master.get(key).await {
                Ok(found) => {
                    master_answered = true;
                    value = found;
                }
                Err(error) => {
                    warn!(%error, key, "master read failed; falling back to slave");
                    last_error = Some(error);
                }
            }
        }

        if value.is_none()
            && let Some(slave) = &self.slave
        {
            match slave.get(key).await {
                Ok(found) => {
                    slave_answered = true;
                    value = found;
                    if value.is_some()
                        && self.setback_master
                        && let Some(master) = &self.master
                    {
                        // Setback is best-effort and off the read path.
                        let master = master.clone();
                        let key = key.to_string();
                        let v = value.clone().expect("checked some");
                        let expire = self.expire;
                        tokio::spawn(async move {
                            if let Err(error) = master.set(&key, v, expire).await {
                                warn!(%error, "setback to master failed");
                            }
                        });
                    }
                }
                Err(error) => {
                    warn!(%error, key, "slave read failed");
                    last_error = Some(error);
                }
            }
        }

        // Distinguish a confirmed miss from a tier failure: only report
        // `None` when some authoritative tier actually answered; if the
        // tiers that were consulted all failed, surface the error instead
        // of a phantom miss.
        if value.is_none() {
            let confirmed_miss = (master_answered && (self.slave.is_none() || slave_answered))
                || (self.master.is_none() && slave_answered);
            if !confirmed_miss && let Some(error) = last_error {
                return Err(error);
            }
        }

        // Set back into the consulted L1 with the L1 expire, off the read
        // path (best-effort; failures were already only logged).
        if let (Some(v), Some(l1)) = (&value, &one_l1) {
            let l1 = l1.clone();
            let key = key.to_string();
            let v = v.clone();
            let expire = self.expire_l1();
            tokio::spawn(async move {
                if let Err(error) = l1.set(&key, v, expire).await {
                    warn!(%error, "setback to L1 failed");
                }
            });
        }

        Ok(value)
    }

    async fn get_multi(&self, keys: &[&str]) -> Result<HashMap<String, Value>> {
        let one_l1 = self.choose_one_l1().cloned();

        let mut values: HashMap<String, Value> = match &one_l1 {
            // A failed L1 tier degrades to the lower tiers, not an error.
            Some(l1) => match l1.get_multi(keys).await {
                Ok(found) => found,
                Err(error) => {
                    warn!(%error, "L1 multi-read failed; falling back");
                    HashMap::new()
                }
            },
            None => HashMap::new(),
        };

        let left_keys = |values: &HashMap<String, Value>| -> Vec<String> {
            keys.iter()
                .filter(|k| !values.contains_key(**k))
                .map(|s| s.to_string())
                .collect()
        };

        let mut l1_hit_keys: Vec<String> = Vec::new();
        if keys.len() > values.len()
            && let Some(master) = &self.master
        {
            let left = left_keys(&values);
            if !left.is_empty() {
                let refs: Vec<&str> = left.iter().map(String::as_str).collect();
                match master.get_multi(&refs).await {
                    Ok(fetched) => {
                        l1_hit_keys = fetched.keys().cloned().collect();
                        values.extend(fetched);
                    }
                    Err(error) => {
                        warn!(%error, "master multi-read failed; falling back to slave")
                    }
                }
            }
        }

        if keys.len() > values.len()
            && let Some(slave) = &self.slave
        {
            let left = left_keys(&values);
            if !left.is_empty() {
                let refs: Vec<&str> = left.iter().map(String::as_str).collect();
                match slave.get_multi(&refs).await {
                    Ok(fetched) => {
                        if self.setback_master
                            && let Some(master) = &self.master
                        {
                            // Setback is best-effort and off the read path:
                            // one spawned task writes back all fetched keys.
                            let master = master.clone();
                            let expire = self.expire;
                            let pairs: Vec<(String, Value)> = fetched
                                .iter()
                                .map(|(k, v)| (k.clone(), v.clone()))
                                .collect();
                            tokio::spawn(async move {
                                futures_util::future::join_all(pairs.iter().map(|(k, v)| {
                                    let master = master.clone();
                                    async move {
                                        if let Err(error) = master.set(k, v.clone(), expire).await {
                                            warn!(%error, "setback to master failed");
                                        }
                                    }
                                }))
                                .await;
                            });
                        }
                        l1_hit_keys.extend(fetched.keys().cloned());
                        values.extend(fetched);
                    }
                    Err(error) => warn!(%error, "slave multi-read failed"),
                }
            }
        }

        // Set back everything the L1 missed into the consulted L1, off the
        // read path.
        if let Some(l1) = &one_l1
            && !l1_hit_keys.is_empty()
        {
            let l1 = l1.clone();
            let expire = self.expire_l1();
            let pairs: Vec<(String, Value)> = l1_hit_keys
                .into_iter()
                .filter_map(|k| values.get(&k).map(|v| (k, v.clone())))
                .collect();
            tokio::spawn(async move {
                futures_util::future::join_all(pairs.iter().map(|(k, v)| {
                    let l1 = l1.clone();
                    async move {
                        if let Err(error) = l1.set(k, v.clone(), expire).await {
                            warn!(%error, "setback to L1 failed");
                        }
                    }
                }))
                .await;
            });
        }

        Ok(values)
    }

    async fn set(&self, key: &str, value: Value, expire: Expiration) -> Result<bool> {
        if self.is_read_only() {
            return Ok(true);
        }
        let rs = self
            .require_master()?
            .set(key, value.clone(), expire)
            .await?;
        if !rs && !self.force_write_all {
            return Ok(rs);
        }
        self.fan_out_write(key, &value, expire).await;
        Ok(rs)
    }

    async fn set_with_noreply(&self, key: &str, value: Value, expire: Expiration) -> Result<()> {
        // Like the Java independent noreply flow: same fan-out, replies
        // discarded.
        if self.is_read_only() {
            return Ok(());
        }
        self.require_master()?
            .set_with_noreply(key, value.clone(), expire)
            .await?;
        if let Some(slave) = &self.slave {
            // Same policy as `set`: a slave failure is logged, not fatal.
            if let Err(error) = slave.set_with_noreply(key, value.clone(), expire).await {
                warn!(%error, "slave pool write failed");
            }
        }
        if self.update_master_l1.load(Ordering::Relaxed) {
            self.write_l1_pools(&self.master_l1, key, &value, expire, false)
                .await;
        }
        if self.update_slave_l1.load(Ordering::Relaxed) {
            self.write_l1_pools(&self.slave_l1, key, &value, expire, false)
                .await;
        }
        self.write_l1_pools(&self.extend_slave_l1, key, &value, expire, false)
            .await;
        Ok(())
    }

    async fn add(&self, key: &str, value: Value, expire: Expiration) -> Result<bool> {
        if self.is_read_only() {
            return Ok(true);
        }
        let rs = self
            .require_master()?
            .add(key, value.clone(), expire)
            .await?;
        if !rs {
            return Ok(rs);
        }
        self.fan_out_write(key, &value, expire).await;
        Ok(rs)
    }

    async fn get_cas(&self, key: &str) -> Result<Option<CasValue>> {
        // Master only: a master cas token can only compare against itself
        // (the Java getCas comment).
        self.require_master()?.get_cas(key).await
    }

    async fn cas(&self, key: &str, value: &CasValue, expire: Expiration) -> Result<bool> {
        if self.is_read_only() {
            return Ok(true);
        }
        let rs = self.require_master()?.cas(key, value, expire).await?;
        if !rs {
            return Ok(rs);
        }
        // Beyond the master everything is a plain set: the master's cas
        // token is not valid on the other pools (the Java cas comment).
        self.fan_out_write(key, &value.value, expire).await;
        Ok(rs)
    }

    async fn delete(&self, key: &str) -> Result<bool> {
        let rs = self.require_master()?.delete(key).await?;
        if let Some(slave) = &self.slave
            && let Err(error) = slave.delete(key).await
        {
            warn!(%error, "slave pool delete failed");
        }
        self.delete_l1_pools(&self.master_l1, key, self.async_write_l1)
            .await;
        if self.update_slave_l1.load(Ordering::Relaxed) {
            self.delete_l1_pools(&self.slave_l1, key, self.async_write_l1)
                .await;
        }
        self.delete_l1_pools(&self.extend_slave_l1, key, self.async_write_ext_l1)
            .await;
        Ok(rs)
    }

    async fn delete_with_noreply(&self, key: &str) -> Result<()> {
        // The Java version substitutes a plain delete here.
        self.delete(key).await.map(|_| ())
    }
}

/// Builder for [`MemCacheTemplate`]; pools are injected as
/// `Arc<dyn Cacheable>` so tests can substitute mocks.
#[derive(Default)]
pub struct MemCacheTemplateBuilder {
    master: Option<Pool>,
    slave: Option<Pool>,
    master_l1: Vec<Pool>,
    slave_l1: Vec<Pool>,
    extend_slave_l1: Vec<Pool>,
    expire_minutes: Option<u64>,
    expire_l1_minutes: Option<u64>,
    force_write_all: bool,
    write_policy: WritePolicy,
    setback_master: Option<bool>,
    master_as_one_l1: Option<bool>,
    async_write_l1: bool,
    async_write_ext_l1: bool,
}

impl MemCacheTemplateBuilder {
    /// The master pool (required for any operation).
    pub fn master(mut self, pool: Pool) -> Self {
        self.master = Some(pool);
        self
    }

    /// The slave pool (read fallback, write fan-out).
    pub fn slave(mut self, pool: Pool) -> Self {
        self.slave = Some(pool);
        self
    }

    /// Adds one master-L1 pool group.
    pub fn master_l1_pool(mut self, pool: Pool) -> Self {
        self.master_l1.push(pool);
        self
    }

    /// Adds one slave-L1 pool group.
    pub fn slave_l1_pool(mut self, pool: Pool) -> Self {
        self.slave_l1.push(pool);
        self
    }

    /// Adds one extend-slave-L1 pool group (async-updated caches).
    pub fn extend_slave_l1_pool(mut self, pool: Pool) -> Self {
        self.extend_slave_l1.push(pool);
        self
    }

    /// Default expiration in minutes (the Java `setExpire`).
    pub fn expire_minutes(mut self, minutes: u64) -> Self {
        self.expire_minutes = Some(minutes);
        self
    }

    /// L1 write-back expiration in minutes (the Java `setExpireL1`).
    pub fn expire_l1_minutes(mut self, minutes: u64) -> Self {
        self.expire_l1_minutes = Some(minutes);
        self
    }

    /// The Java `forceWriteAll`.
    pub fn force_write_all(mut self, on: bool) -> Self {
        self.force_write_all = on;
        self
    }

    /// The Java `wirtePolicy`.
    pub fn write_policy(mut self, policy: WritePolicy) -> Self {
        self.write_policy = policy;
        self
    }

    /// The Java `setbackMaster` (default `true`).
    pub fn setback_master(mut self, on: bool) -> Self {
        self.setback_master = Some(on);
        self
    }

    /// The Java `masterAsOneL1` (default `true`).
    pub fn master_as_one_l1(mut self, on: bool) -> Self {
        self.master_as_one_l1 = Some(on);
        self
    }

    /// The Java `asyncWriteL1`.
    pub fn async_write_l1(mut self, on: bool) -> Self {
        self.async_write_l1 = on;
        self
    }

    /// The Java `asyncWriteExtL1`.
    pub fn async_write_ext_l1(mut self, on: bool) -> Self {
        self.async_write_ext_l1 = on;
        self
    }

    /// Build the template.
    pub fn build(self) -> MemCacheTemplate {
        let to_expire = |minutes: Option<u64>| {
            minutes.map(|m| Expiration::Seconds((m * 60).min(u32::MAX as u64) as u32))
        };
        MemCacheTemplate {
            master: self.master,
            slave: self.slave,
            master_l1: self.master_l1,
            slave_l1: self.slave_l1,
            extend_slave_l1: self.extend_slave_l1,
            expire: to_expire(self.expire_minutes).unwrap_or_default(),
            expire_l1: to_expire(self.expire_l1_minutes),
            read_only: Arc::new(AtomicBool::new(false)),
            update_slave_l1: Arc::new(AtomicBool::new(true)),
            update_master_l1: Arc::new(AtomicBool::new(true)),
            force_write_all: self.force_write_all,
            write_policy: self.write_policy,
            setback_master: self.setback_master.unwrap_or(true),
            master_as_one_l1: self.master_as_one_l1.unwrap_or(true),
            async_write_l1: self.async_write_l1,
            async_write_ext_l1: self.async_write_ext_l1,
            l1_point: Arc::new(AtomicU64::new(0)),
        }
    }
}
