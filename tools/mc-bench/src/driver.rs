//! Workload generation against a pre-generated key/value pool.
//!
//! Workloads are written against [`BenchClient`], a small enum over the
//! SDK's unified [`memcache::Client`] (sidecar or direct) and the
//! client-side shard router (`direct::Shards`), so the same workload code
//! drives every access mode.
//!
//! To keep the per-op allocation count honest (so it reflects the *SDK's*
//! allocations, not the benchmark's), keys and values are pre-generated once
//! into an [`Arc`] and workers index into them with a plain integer — no
//! `format!()` on the hot path.

use std::sync::Arc;

use memcache::direct::HaClient;
use memcache::direct::Shards;
use memcache::{Client, Value};

/// The client the harness drives: either access mode behind the unified
/// [`Client`], or the direct-mode shard router.
#[derive(Clone)]
pub enum BenchClient {
    /// Mesh sidecar or single direct backend (unified proxy).
    Unified(Client),
    /// Client-side shard router across several direct backends.
    Shards(Arc<Shards>),
    /// Master/slave HA topology client.
    Ha(Arc<HaClient>),
    /// The multi-tier backup template (`memcache::service`), behind a thin
    /// adapter so the harness can drive it without the `service` feature
    /// being enabled in every build.
    Template(Arc<dyn TemplateClient>),
}

/// The subset of `MemCacheTemplate` the harness drives (get/set only; the
/// template surface has no `incr`). Implemented for the template when the
/// `template` feature is on.
#[cfg_attr(not(feature = "template"), allow(dead_code))]
#[async_trait::async_trait]
pub trait TemplateClient: Send + Sync {
    async fn get(&self, key: &str) -> memcache::Result<Option<Value>>;
    async fn get_multi(
        &self,
        keys: &[&str],
    ) -> memcache::Result<std::collections::HashMap<String, Value>>;
    async fn set(&self, key: &str, value: Vec<u8>) -> memcache::Result<bool>;
}

#[cfg(feature = "template")]
#[async_trait::async_trait]
impl TemplateClient for memcache::service::MemCacheTemplate {
    async fn get(&self, key: &str) -> memcache::Result<Option<Value>> {
        memcache::service::Cacheable::get(self, key).await
    }
    async fn get_multi(
        &self,
        keys: &[&str],
    ) -> memcache::Result<std::collections::HashMap<String, Value>> {
        memcache::service::Cacheable::get_multi(self, keys).await
    }
    async fn set(&self, key: &str, value: Vec<u8>) -> memcache::Result<bool> {
        memcache::service::Cacheable::set(
            self,
            key,
            memcache::Value::new(value, 0),
            memcache::Expiration::Never,
        )
        .await
    }
}

impl BenchClient {
    pub async fn get(&self, key: &str) -> memcache::Result<Option<Value>> {
        match self {
            BenchClient::Unified(client) => client.get(key).await,
            BenchClient::Shards(shards) => shards.get_client(key).get(key).await,
            BenchClient::Ha(client) => client.get(key).await,
            BenchClient::Template(client) => client.get(key).await,
        }
    }

    pub async fn get_multi(
        &self,
        keys: &[&str],
    ) -> memcache::Result<std::collections::HashMap<String, Value>> {
        match self {
            BenchClient::Unified(client) => client.get_multi(keys).await,
            BenchClient::Ha(client) => client.get_multi(keys).await,
            BenchClient::Template(client) => client.get_multi(keys).await,
            BenchClient::Shards(_) => {
                // Route each key to its owning shard and merge. Sequential
                // per-shard fan-out keeps the harness simple; GET coverage is
                // what this workload measures.
                let mut map = std::collections::HashMap::with_capacity(keys.len());
                for key in keys {
                    if let Some(value) = self.get(key).await? {
                        map.insert((*key).to_string(), value);
                    }
                }
                Ok(map)
            }
        }
    }

    pub async fn set(&self, key: &str, value: &[u8]) -> memcache::Result<bool> {
        match self {
            BenchClient::Unified(client) => client.set(key, value, 0u32).await,
            BenchClient::Shards(shards) => shards.get_client(key).set(key, value, 0u32).await,
            BenchClient::Ha(client) => client.set(key, value, 0u32).await,
            BenchClient::Template(client) => client.set(key, value.to_vec()).await,
        }
    }

    pub async fn incr(&self, key: &str, delta: u64) -> memcache::Result<Option<u64>> {
        match self {
            BenchClient::Unified(client) => client.incr(key, delta).await,
            BenchClient::Shards(shards) => shards.get_client(key).incr(key, delta).await,
            BenchClient::Ha(client) => client.incr(key, delta).await,
            // The template surface has no incr/decr.
            BenchClient::Template(_) => Err(memcache::Error::Client(
                "template mode does not support incr".into(),
            )),
        }
    }
}

/// The kind of workload to run.
#[derive(Clone, Copy, Debug)]
pub enum WorkloadKind {
    /// One GET per op — pure single-round-trip read (ping/pong).
    Get,
    /// One multi-GET of 4 keys per op.
    GetMulti,
    /// One SET per op (overwrite of pre-seeded keys).
    Set,
    /// One INCR per op on pre-seeded decimal counters.
    Incr,
}

impl WorkloadKind {
    pub fn runner(self, pool: Arc<Pool>, verify: bool) -> Box<dyn Workload> {
        match self {
            WorkloadKind::Get => Box::new(GetPing { pool, verify }),
            WorkloadKind::GetMulti => Box::new(GetMultiPing { pool, verify }),
            WorkloadKind::Set => Box::new(SetPing { pool }),
            WorkloadKind::Incr => Box::new(IncrPing { pool }),
        }
    }
}

/// Keys a multi-GET op fetches at once.
pub const MULTI_KEYS: usize = 4;

/// Seeded values always start with the key bytes (self-describing content,
/// optionally padded to a size distribution), so replies can be checked for
/// request/response mixups whenever `--verify` is on. Returns true if
/// `value` carries that prefix. A miss also counts as corruption: everything
/// was seeded, so a miss means routing/storage went wrong.
pub fn expected_value_matches(key: &[u8], value: &Value) -> bool {
    value.as_bytes().len() >= key.len() && value.as_bytes().starts_with(key)
}

/// Build the seed value for `key`: the key bytes, padded with the
/// size-cycled pattern `fill` when it is larger, so the value size spread is
/// preserved while the content stays verifiable.
pub fn seeded_value(key: &[u8], fill: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(fill.len().max(key.len()));
    v.extend_from_slice(key);
    if fill.len() > v.len() {
        v.extend_from_slice(&fill[v.len()..]);
    }
    v
}

/// One logical operation against a client.
pub trait Workload: Send + Sync {
    fn run<'a>(
        &'a self,
        client: &'a BenchClient,
        op: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send + 'a>>;
}

/// Pre-generated keys (fixed length) and values (sizes cycling through
/// `1..=max_value_size`), shared (immutable) across all workers. Built once
/// before measurement so the hot path only indexes into it.
pub struct Pool {
    keys: Vec<String>,
    values: Vec<Vec<u8>>,
    /// Fully seeded values (`key || padding`), one per key; pre-built so
    /// the SET hot path does not allocate in the harness.
    seeded: Vec<Vec<u8>>,
    /// Big-value pattern and stride: every `big_stride`-th key gets the big
    /// value; stride 0 = disabled.
    big_value: Option<Vec<u8>>,
    big_stride: usize,
}

impl Pool {
    /// Build a pool of `num_keys` keys (each `key_len` bytes) and values
    /// whose sizes cycle through `1..=max_value_size`. With
    /// `big_value_rate > 0`, every `1/rate`-th key gets a
    /// `big_value_size`-byte value instead.
    pub fn new(
        num_keys: usize,
        key_len: usize,
        max_value_size: usize,
        big_value_rate: f64,
        big_value_size: usize,
    ) -> Self {
        let keys = (0..num_keys).map(|i| key_string(i, key_len)).collect();
        let values: Vec<Vec<u8>> = if max_value_size == 0 {
            vec![Vec::new()]
        } else {
            (1..=max_value_size).map(value_bytes).collect()
        };
        let (big_value, big_stride) = if big_value_rate > 0.0 {
            (
                Some(value_bytes(big_value_size)),
                (1.0 / big_value_rate).round().max(1.0) as usize,
            )
        } else {
            (None, 0)
        };
        Pool {
            keys,
            values,
            big_value,
            big_stride,
            seeded: Vec::new(),
        }
    }

    /// Pre-build the per-key seeded values (call once after construction).
    pub fn with_seeded(mut self) -> Self {
        self.seeded = (0..self.keys.len())
            .map(|i| seeded_value(self.keys[i].as_bytes(), self.value(i)))
            .collect();
        self
    }

    /// The pre-built seeded value for key `index` (requires `with_seeded`).
    pub fn seeded(&self, index: usize) -> &[u8] {
        &self.seeded[index % self.seeded.len()]
    }

    /// All pre-generated keys, for seeding.
    pub fn keys(&self) -> &[String] {
        &self.keys
    }

    /// Pick key at `index` (caller wraps with modulo).
    pub fn key(&self, index: usize) -> &str {
        &self.keys[index % self.keys.len()]
    }

    /// The fill value for key `index`: the big pattern for big-value keys,
    /// otherwise the size-cycled pattern.
    pub fn value(&self, index: usize) -> &[u8] {
        if self.big_stride > 0 && index.is_multiple_of(self.big_stride) {
            return self.big_value.as_deref().unwrap();
        }
        &self.values[index % self.values.len()]
    }
}

/// Deterministic `key_len`-byte key derived from `i`: a zero-padded hex
/// index prefix, then a repeating fill byte. Keys are pure ASCII so they
/// are valid memcached keys.
fn key_string(i: usize, key_len: usize) -> String {
    let mut buf = format!("{i:016x}");
    while buf.len() < key_len {
        buf.push('k');
    }
    buf.truncate(key_len);
    buf
}

/// A `size`-byte value filled with a stable pattern (no allocation on the
/// hot path — built once here).
fn value_bytes(size: usize) -> Vec<u8> {
    let mut buf = Vec::with_capacity(size);
    let mut b = b'a';
    for _ in 0..size {
        buf.push(b);
        b = b.wrapping_add(17);
        // Keep the fill printable-ish (not required, but eases debugging).
        if !b.is_ascii_alphanumeric() {
            b = b'a';
        }
    }
    buf
}

struct GetPing {
    pool: Arc<Pool>,
    verify: bool,
}
impl Workload for GetPing {
    fn run<'a>(
        &'a self,
        client: &'a BenchClient,
        op: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send + 'a>> {
        let key = self.pool.key(op as usize);
        Box::pin(async move {
            match client.get(key).await {
                Ok(Some(value)) => !self.verify || expected_value_matches(key.as_bytes(), &value),
                Ok(None) => !self.verify,
                Err(_) => false,
            }
        })
    }
}

struct GetMultiPing {
    pool: Arc<Pool>,
    verify: bool,
}
impl Workload for GetMultiPing {
    fn run<'a>(
        &'a self,
        client: &'a BenchClient,
        op: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send + 'a>> {
        let keys: Vec<&str> = (0..MULTI_KEYS)
            .map(|j| self.pool.key(op as usize + j))
            .collect();
        Box::pin(async move {
            match client.get_multi(&keys).await {
                Ok(map) => {
                    if !self.verify {
                        return true;
                    }
                    keys.iter().all(|key| {
                        map.get(*key)
                            .is_some_and(|value| expected_value_matches(key.as_bytes(), value))
                    })
                }
                Err(_) => false,
            }
        })
    }
}

struct SetPing {
    pool: Arc<Pool>,
}
impl Workload for SetPing {
    fn run<'a>(
        &'a self,
        client: &'a BenchClient,
        op: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send + 'a>> {
        let index = op as usize;
        let key = self.pool.key(index);
        let value = self.pool.seeded(index);
        Box::pin(async move { client.set(key, value).await.unwrap_or(false) })
    }
}

struct IncrPing {
    pool: Arc<Pool>,
}
impl Workload for IncrPing {
    fn run<'a>(
        &'a self,
        client: &'a BenchClient,
        op: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send + 'a>> {
        let key = self.pool.key(op as usize);
        Box::pin(async move { client.incr(key, 1).await.is_ok() })
    }
}
