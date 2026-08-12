//! [`MemcacheServiceTemplate`] — the Rust port of the Java
//! `CacheServiceTemplate`.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use crate::value::{CasValue, ToMemcacheValue, Value};
use crate::{Client, Error, Expiration, Result};
use arc_swap::ArcSwapAny;
use tracing::warn;

use super::Cacheable;

/// Largest `expire_minutes` the template accepts; longer values are clamped
/// (mirrors the Java `maxLowerExpire = 43200`, i.e. 30 days in minutes).
pub const MAX_LOWER_EXPIRE_MINUTES: u64 = 1440 * 30;

/// Above this many keys, `get_multi` runs unsplit in the caller's task
/// ("if it's this extreme, only hurt yourself, don't drag others down").
pub const MULTI_GET_MAX_COUNT: usize = 20000;
/// Shard size for split `get_multi` processing.
pub const MULTI_GET_SPLIT_STEP: usize = 300;
/// How long split `get_multi` waits for the assist tasks after the local
/// shard finishes.
pub const MULTI_GET_TIMEOUT: Duration = Duration::from_millis(200);

static GLOBAL_SWITCH: AtomicBool = AtomicBool::new(true);
static SYNC_MULTI_GET_SWITCH: AtomicBool = AtomicBool::new(true);

/// The global on/off switch shared by every template instance (the Java
/// `feature.cacheservice.cacheable.all` switcher).
pub fn global_switch() -> bool {
    GLOBAL_SWITCH.load(Ordering::Relaxed)
}

/// Set the global switch; `false` forces every template onto its backup.
pub fn set_global_switch(on: bool) {
    GLOBAL_SWITCH.store(on, Ordering::Relaxed);
}

/// Whether `get_multi` processes the whole key set in the caller's task
/// (the Java `feature.cacheservice.enable_sync_multiget` switcher).
pub fn sync_multi_get_switch() -> bool {
    SYNC_MULTI_GET_SWITCH.load(Ordering::Relaxed)
}

/// Set the sync-multiget switch.
pub fn set_sync_multi_get_switch(on: bool) {
    SYNC_MULTI_GET_SWITCH.store(on, Ordering::Relaxed);
}

/// Sized wrapper so the backup can live in an `ArcSwapAny` (which requires
/// `Sized` pointees) while callers still see `Arc<dyn Cacheable>`.
struct BackupSlot(Arc<dyn Cacheable>);

/// A template that routes memcached operations to a primary cache-service
/// client with automatic fallback to a backup cache, mirroring the Java
/// `CacheServiceTemplate` routing policy.
#[derive(Clone)]
pub struct MemcacheServiceTemplate {
    primary: Option<Arc<dyn Cacheable>>,
    /// Hot-swappable backup (Java rebuilds it on cache-service config change).
    backup: Option<Arc<ArcSwapAny<Arc<BackupSlot>>>>,
    /// Per-template switch (the Java per-bean `motanMcSwitcher`).
    use_primary: Arc<AtomicBool>,
    /// Consecutive primary failures (reset on success), feeding the circuit.
    primary_failures: Arc<AtomicU64>,
    /// Epoch millis until which the primary is short-circuited (0 = closed).
    primary_open_until: Arc<AtomicU64>,
    expire: Expiration,
}

impl MemcacheServiceTemplate {
    /// Starts building a template.
    pub fn builder() -> MemcacheServiceTemplateBuilder {
        MemcacheServiceTemplateBuilder::default()
    }

    /// Flip the per-template primary switch at runtime.
    pub fn set_use_primary(&self, on: bool) {
        self.use_primary.store(on, Ordering::Relaxed);
    }

    /// The default expiration applied by `set` / `add` / `cas`.
    pub fn expire(&self) -> Expiration {
        self.expire
    }

    /// Set the default expiration in minutes; values above 30 days are
    /// clamped (mirrors the Java `setExpire`).
    pub fn set_expire_minutes(&mut self, minutes: u64) {
        self.expire = clamp_expire(minutes);
    }

    /// The backup cache, if configured.
    pub fn backup(&self) -> Option<Arc<dyn Cacheable>> {
        self.backup.as_ref().map(|slot| slot.load_full().0.clone())
    }

    /// Atomically replace the backup cache (the Java config-change callback
    /// `call(CacheServerPoolConf)` rebuilds the backup; here the new backup
    /// is swapped in and the old one is dropped once in-flight operations
    /// release it).
    pub fn set_backup(&self, backup: Arc<dyn Cacheable>) {
        if let Some(slot) = &self.backup {
            slot.store(Arc::new(BackupSlot(backup)));
        }
    }

    /// Fetch a single value.
    pub async fn get(&self, key: &str) -> Result<Option<Value>> {
        self.dispatch(|c| Box::pin(async move { c.get(key).await }))
            .await
    }

    /// Fetch multiple values.
    ///
    /// Small and huge key sets run inline; mid-size sets are split into
    /// [`MULTI_GET_SPLIT_STEP`] shards processed concurrently, with the
    /// assist tasks bounded by [`MULTI_GET_TIMEOUT`].
    pub async fn get_multi(&self, keys: &[&str]) -> Result<HashMap<String, Value>> {
        if keys.is_empty() {
            return Ok(HashMap::new());
        }

        if sync_multi_get_switch()
            || keys.len() <= MULTI_GET_SPLIT_STEP
            || keys.len() >= MULTI_GET_MAX_COUNT
        {
            return self.do_get_multi(keys).await;
        }

        let mut shards: Vec<Vec<String>> = keys
            .chunks(MULTI_GET_SPLIT_STEP)
            .map(|c| c.iter().map(|s| s.to_string()).collect())
            .collect();

        // The last shard runs on the caller's task; the rest are spawned.
        let local = shards.pop().expect("non-empty keys yield shards");

        let template = self.clone();
        let handles: Vec<_> = shards
            .into_iter()
            .map(|shard| {
                let template = template.clone();
                tokio::spawn(async move {
                    let refs: Vec<&str> = shard.iter().map(String::as_str).collect();
                    template.do_get_multi(&refs).await
                })
            })
            .collect();

        let local_refs: Vec<&str> = local.iter().map(String::as_str).collect();
        let mut result = self.do_get_multi(&local_refs).await?;

        // Collect the assist shards within the bounded window, mirroring the
        // Java waitFuturesDone: each task gets whatever time remains of
        // MULTI_GET_TIMEOUT; expired tasks are cancelled (not interrupted).
        let deadline = tokio::time::Instant::now() + MULTI_GET_TIMEOUT;
        for handle in handles {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                handle.abort();
                continue;
            }
            match tokio::time::timeout(remaining, handle).await {
                Ok(Ok(Ok(partial))) => result.extend(partial),
                Ok(Ok(Err(e))) => warn!(error = %e, "get_multi assist shard failed"),
                Ok(Err(e)) => warn!(error = %e, "get_multi assist task failed"),
                Err(_) => warn!("get_multi assist shard timed out; returning partial result"),
            }
        }

        Ok(result)
    }

    async fn do_get_multi(&self, keys: &[&str]) -> Result<HashMap<String, Value>> {
        self.dispatch(move |c| {
            Box::pin(async move {
                // Misses are simply absent from the map, like the Java
                // doGetMulti cleanup that removes null entries.
                c.get_multi(keys).await
            })
        })
        .await
    }

    /// Store a value with the template's default expiration.
    pub async fn set(&self, key: &str, value: impl ToMemcacheValue) -> Result<bool> {
        self.set_with_expire(key, value, self.expire).await
    }

    /// Store a value with an explicit expiration.
    pub async fn set_with_expire(
        &self,
        key: &str,
        value: impl ToMemcacheValue,
        expire: impl Into<Expiration>,
    ) -> Result<bool> {
        let value = value.to_memcache_value();
        let expire = expire.into();
        let value = &value;
        self.dispatch(|c| Box::pin(async move { c.set(key, value.clone(), expire).await }))
            .await
    }

    /// Store a value, discarding the reply (Java `setWithNoreply`).
    pub async fn set_with_noreply(&self, key: &str, value: impl ToMemcacheValue) -> Result<()> {
        self.set_with_noreply_expire(key, value, self.expire).await
    }

    /// Store with an explicit expiration, discarding the reply.
    pub async fn set_with_noreply_expire(
        &self,
        key: &str,
        value: impl ToMemcacheValue,
        expire: impl Into<Expiration>,
    ) -> Result<()> {
        let value = value.to_memcache_value();
        let expire = expire.into();
        let value = &value;
        self.dispatch(|c| {
            Box::pin(async move { c.set_with_noreply(key, value.clone(), expire).await })
        })
        .await
    }

    /// Store only if the key does not already exist (default expiration).
    pub async fn add(&self, key: &str, value: impl ToMemcacheValue) -> Result<bool> {
        self.add_with_expire(key, value, self.expire).await
    }

    /// Store only if absent, with an explicit expiration.
    pub async fn add_with_expire(
        &self,
        key: &str,
        value: impl ToMemcacheValue,
        expire: impl Into<Expiration>,
    ) -> Result<bool> {
        let value = value.to_memcache_value();
        let expire = expire.into();
        let value = &value;
        self.dispatch(|c| Box::pin(async move { c.add(key, value.clone(), expire).await }))
            .await
    }

    /// Fetch a value together with its CAS token.
    pub async fn get_cas(&self, key: &str) -> Result<Option<CasValue>> {
        self.dispatch(|c| Box::pin(async move { c.get_cas(key).await }))
            .await
    }

    /// Compare-and-swap with the template's default expiration.
    pub async fn cas(&self, key: &str, value: &CasValue) -> Result<bool> {
        self.cas_with_expire(key, value, self.expire).await
    }

    /// Compare-and-swap with an explicit expiration.
    pub async fn cas_with_expire(
        &self,
        key: &str,
        value: &CasValue,
        expire: impl Into<Expiration>,
    ) -> Result<bool> {
        let expire = expire.into();
        self.dispatch(|c| Box::pin(async move { c.cas(key, value, expire).await }))
            .await
    }

    /// Delete a key.
    pub async fn delete(&self, key: &str) -> Result<bool> {
        self.dispatch(|c| Box::pin(async move { c.delete(key).await }))
            .await
    }

    /// Delete a key, discarding the reply (Java `deleteWithNoreply`).
    pub async fn delete_with_noreply(&self, key: &str) -> Result<()> {
        self.dispatch(|c| Box::pin(async move { c.delete_with_noreply(key).await }))
            .await
    }

    /// The routing core: primary when its switches are on, falling back to
    /// the backup on error; otherwise straight to the backup.
    ///
    /// After [`CIRCUIT_FAILURES`] consecutive primary failures the primary
    /// is short-circuited for [`CIRCUIT_COOLDOWN`]: requests go straight to
    /// the backup without paying a failing primary attempt each. One probe
    /// request is let through after the cooldown; a success closes the
    /// circuit again.
    #[allow(clippy::needless_lifetimes)]
    async fn dispatch<'a, T, F>(&'a self, op: F) -> Result<T>
    where
        F: Fn(
            Arc<dyn Cacheable>,
        )
            -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<T>> + Send + 'a>>,
    {
        if self.use_primary.load(Ordering::Relaxed)
            && global_switch()
            && let Some(primary) = &self.primary
        {
            if self.primary_circuit_open() {
                return self.dispatch_backup(op).await;
            }
            match op(primary.clone()).await {
                Ok(value) => {
                    self.primary_failures.store(0, Ordering::Relaxed);
                    return Ok(value);
                }
                Err(e) => {
                    self.note_primary_failure();
                    if let Some(backup) = self.backup() {
                        warn!(error = %e, "primary cache failed; falling back to backup");
                        return op(backup).await;
                    }
                    return Err(e);
                }
            }
        }

        self.dispatch_backup(op).await
    }

    #[allow(clippy::needless_lifetimes)]
    async fn dispatch_backup<'a, T, F>(&'a self, op: F) -> Result<T>
    where
        F: Fn(
            Arc<dyn Cacheable>,
        )
            -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<T>> + Send + 'a>>,
    {
        match self.backup() {
            Some(backup) => op(backup).await,
            None => Err(Error::Protocol(
                "MemcacheServiceTemplate backup cache is null".to_string(),
            )),
        }
    }

    /// Whether the primary circuit is open (short-circuited until the
    /// cooldown instant stored as epoch millis, 0 = closed).
    fn primary_circuit_open(&self) -> bool {
        let until = self.primary_open_until.load(Ordering::Relaxed);
        if until == 0 {
            return false;
        }
        let now = epoch_millis();
        if now < until {
            return true;
        }
        // Cooldown elapsed: half-open — exactly one caller wins the reset
        // and probes the primary; others keep short-circuiting.
        self.primary_open_until
            .compare_exchange(until, 0, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
    }

    fn note_primary_failure(&self) {
        let failures = self.primary_failures.fetch_add(1, Ordering::Relaxed) + 1;
        if failures >= CIRCUIT_FAILURES {
            self.primary_open_until
                .store(epoch_millis() + CIRCUIT_COOLDOWN_MS, Ordering::Relaxed);
            self.primary_failures.store(0, Ordering::Relaxed);
        }
    }
}

/// Consecutive primary failures before the circuit opens.
const CIRCUIT_FAILURES: u64 = 5;
/// How long an open circuit short-circuits the primary, in milliseconds.
const CIRCUIT_COOLDOWN_MS: u64 = 5_000;

fn epoch_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn clamp_expire(minutes: u64) -> Expiration {
    let minutes = if minutes > MAX_LOWER_EXPIRE_MINUTES {
        warn!(minutes, "invalid expire minutes; clamping to 30 days");
        MAX_LOWER_EXPIRE_MINUTES
    } else {
        minutes
    };
    Expiration::Seconds((minutes * 60).min(u32::MAX as u64) as u32)
}

/// Builder for [`MemcacheServiceTemplate`].
#[derive(Default)]
pub struct MemcacheServiceTemplateBuilder {
    primary: Option<Arc<dyn Cacheable>>,
    backup: Option<Arc<dyn Cacheable>>,
    use_primary: bool,
    expire_minutes: Option<u64>,
}

impl MemcacheServiceTemplateBuilder {
    /// The primary cache-service client (mesh or direct SDK client).
    pub fn primary_client(mut self, client: impl Into<Client>) -> Self {
        self.primary = Some(Arc::new(client.into()));
        self
    }

    /// A primary cache behind the [`Cacheable`] trait.
    pub fn primary(mut self, primary: Arc<dyn Cacheable>) -> Self {
        self.primary = Some(primary);
        self
    }

    /// The backup cache (the Java `backupCache`).
    pub fn backup(mut self, backup: Arc<dyn Cacheable>) -> Self {
        self.backup = Some(backup);
        self
    }

    /// Whether operations should try the primary client first (the Java
    /// per-bean `useMotanMcClient` switcher default).
    pub fn use_primary(mut self, on: bool) -> Self {
        self.use_primary = on;
        self
    }

    /// Default expiration in minutes (clamped to 30 days).
    pub fn expire_minutes(mut self, minutes: u64) -> Self {
        self.expire_minutes = Some(minutes);
        self
    }

    /// Build the template.
    pub fn build(self) -> MemcacheServiceTemplate {
        MemcacheServiceTemplate {
            primary: self.primary,
            backup: self
                .backup
                .map(|b| Arc::new(ArcSwapAny::new(Arc::new(BackupSlot(b))))),
            use_primary: Arc::new(AtomicBool::new(self.use_primary)),
            primary_failures: Arc::new(AtomicU64::new(0)),
            primary_open_until: Arc::new(AtomicU64::new(0)),
            expire: self.expire_minutes.map_or(Expiration::Never, clamp_expire),
        }
    }
}
