use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, RwLock};

use deadpool::Runtime;
use deadpool::managed::Object;
use deadpool::managed::Timeouts;
use tokio::time::timeout;

use crate::config::{Config, Endpoint, MAX_KEY_LEN};
use crate::discovery::DirectoryWatcher;
use crate::error::{Error, Result};
use crate::expiration::Expiration;
use crate::pool::{Manager, Pool, SharedEndpoint};
use crate::protocol::StoreCommand;
use crate::value::{CasValue, ToMemcacheValue, Value};

/// An async memcached client backed by a bounded connection pool.
///
/// The API mirrors breeze-sdk-core's `CacheAble` interface: `get` / `get_multi`
/// / `set` / `add` / `get_cas` / `cas` / `delete` and their `*_with_noreply`
/// variants, plus a few extras (`replace`/`append`/`prepend`/`incr`/`decr`/
/// `touch`/`flush_all`/`version`). Every method transparently checks out a
/// pooled connection and applies the configured per-operation timeout.
///
/// Request exceptions are logged at `error` level (via [`tracing`]), matching
/// the mesh `MeshMemcacheTemplate` log format
/// `mc mesh <method> error ,namespace:<ns> ,key: <key>`.
#[derive(Clone)]
pub struct Client {
    pool: Pool,
    config: Arc<Config>,
    endpoint: SharedEndpoint,
    /// Subscription to the shared registry scanner for the socks directory
    /// this client's endpoint was discovered in. `None` for clients built
    /// from an explicit TCP/unix endpoint. Keeping it alive keeps the
    /// (shared, per-directory) scan task running.
    watcher: Option<Arc<DirectoryWatcher>>,
}

impl Client {
    /// Build a client and its connection pool from `config`.
    ///
    /// If the config carries mesh discovery coordinates (set by
    /// [`Config::mesh`] / [`Config::mesh_in`] / [`Config::sock`]), a
    /// lightweight background task periodically rescans the socks registry
    /// and, when the advertised endpoint changes (e.g. a mesh port
    /// reassignment), switches new connections over and drains the old ones.
    pub fn new(config: Config) -> Result<Self> {
        let config = Arc::new(config);
        let endpoint: SharedEndpoint = Arc::new(RwLock::new(config.endpoint.clone()));
        let manager = Manager::new(config.clone(), endpoint.clone());
        let pool = Pool::builder(manager)
            .max_size(config.max_connections)
            .timeouts(Timeouts {
                wait: Some(config.pool_wait_timeout),
                create: Some(config.connect_timeout),
                recycle: Some(config.op_timeout),
            })
            .runtime(Runtime::Tokio1)
            .build()
            .map_err(|err| Error::Pool(err.to_string()))?;
        let watcher = config
            .mesh_discovery
            .as_ref()
            .map(|discovery| Arc::new(crate::discovery::watch(discovery.dir.clone())));
        let client = Client {
            pool,
            config,
            endpoint,
            watcher,
        };
        client.spawn_endpoint_tracker();
        Ok(client)
    }

    // --- reads ---

    /// Fetch a single value.
    pub async fn get(&self, key: &str) -> Result<Option<Value>> {
        self.run("get", key, async {
            self.validate_key(key)?;
            let mut obj = self.pool.get().await?;
            let result = self.timed(obj.get(key)).await;
            Self::discard_on_error(obj, result)
        })
        .await
    }

    /// Fetch multiple values in one round-trip. Missing keys are omitted.
    pub async fn get_multi(&self, keys: &[&str]) -> Result<HashMap<String, Value>> {
        let key_desc = keys.join(",");
        self.run("getMulti", &key_desc, async {
            for key in keys {
                self.validate_key(key)?;
            }
            let mut obj = self.pool.get().await?;
            let result = self.timed(obj.get_multi(keys)).await;
            Self::discard_on_error(obj, result)
        })
        .await
    }

    /// Fetch a value together with its CAS token.
    pub async fn get_cas(&self, key: &str) -> Result<Option<CasValue>> {
        self.run("getCas", key, async {
            self.validate_key(key)?;
            let mut obj = self.pool.get().await?;
            let result = self.timed(obj.get_cas(key)).await;
            Self::discard_on_error(obj, result)
        })
        .await
    }

    // --- writes ---

    /// Store a value unconditionally.
    pub async fn set(
        &self,
        key: &str,
        value: impl ToMemcacheValue,
        expire: impl Into<Expiration>,
    ) -> Result<bool> {
        self.run(
            "set",
            key,
            self.store(StoreCommand::Set, key, value, expire, false),
        )
        .await
    }

    /// Store a value without waiting for a reply (fire-and-forget on the text
    /// protocol; a normal store whose reply is discarded on the binary protocol).
    pub async fn set_with_noreply(
        &self,
        key: &str,
        value: impl ToMemcacheValue,
        expire: impl Into<Expiration>,
    ) -> Result<()> {
        self.run(
            "setWithNoreply",
            key,
            self.store(StoreCommand::Set, key, value, expire, true),
        )
        .await
        .map(|_| ())
    }

    /// Store only if the key does not already exist.
    pub async fn add(
        &self,
        key: &str,
        value: impl ToMemcacheValue,
        expire: impl Into<Expiration>,
    ) -> Result<bool> {
        self.run(
            "add",
            key,
            self.store(StoreCommand::Add, key, value, expire, false),
        )
        .await
    }

    /// Store only if the key already exists.
    pub async fn replace(
        &self,
        key: &str,
        value: impl ToMemcacheValue,
        expire: impl Into<Expiration>,
    ) -> Result<bool> {
        self.run(
            "replace",
            key,
            self.store(StoreCommand::Replace, key, value, expire, false),
        )
        .await
    }

    /// Append data to an existing value.
    pub async fn append(&self, key: &str, value: impl ToMemcacheValue) -> Result<bool> {
        self.run(
            "append",
            key,
            self.store(StoreCommand::Append, key, value, Expiration::Never, false),
        )
        .await
    }

    /// Prepend data to an existing value.
    pub async fn prepend(&self, key: &str, value: impl ToMemcacheValue) -> Result<bool> {
        self.run(
            "prepend",
            key,
            self.store(StoreCommand::Prepend, key, value, Expiration::Never, false),
        )
        .await
    }

    /// Compare-and-swap: store `value.value` only if the CAS token still matches.
    pub async fn cas(
        &self,
        key: &str,
        value: &CasValue,
        expire: impl Into<Expiration>,
    ) -> Result<bool> {
        let expire = expire.into();
        self.run("cas", key, async move {
            self.validate_key(key)?;
            let mut obj = self.pool.get().await?;
            let result = self
                .timed(obj.cas(key, &value.value, expire, value.cas, false))
                .await;
            Self::discard_on_error(obj, result)
        })
        .await
    }

    /// Delete a key. Returns `true` if the key existed.
    pub async fn delete(&self, key: &str) -> Result<bool> {
        self.run("delete", key, async {
            self.validate_key(key)?;
            let mut obj = self.pool.get().await?;
            let result = self.timed(obj.delete(key, false)).await;
            Self::discard_on_error(obj, result)
        })
        .await
    }

    /// Delete a key without waiting for a reply.
    pub async fn delete_with_noreply(&self, key: &str) -> Result<()> {
        self.run("deleteWithNoreply", key, async {
            self.validate_key(key)?;
            let mut obj = self.pool.get().await?;
            let result = self.timed(obj.delete(key, true)).await;
            Self::discard_on_error(obj, result)
        })
        .await
        .map(|_| ())
    }

    /// Atomically increment a counter. Returns `None` if the key is missing.
    pub async fn incr(&self, key: &str, delta: u64) -> Result<Option<u64>> {
        self.run("incr", key, async {
            self.validate_key(key)?;
            let mut obj = self.pool.get().await?;
            let result = self.timed(obj.incr_decr(true, key, delta, false)).await;
            Self::discard_on_error(obj, result)
        })
        .await
    }

    /// Atomically decrement a counter. Returns `None` if the key is missing.
    pub async fn decr(&self, key: &str, delta: u64) -> Result<Option<u64>> {
        self.run("decr", key, async {
            self.validate_key(key)?;
            let mut obj = self.pool.get().await?;
            let result = self.timed(obj.incr_decr(false, key, delta, false)).await;
            Self::discard_on_error(obj, result)
        })
        .await
    }

    /// Update a key's expiration without fetching its value.
    pub async fn touch(&self, key: &str, expire: impl Into<Expiration>) -> Result<bool> {
        let expire = expire.into();
        self.run("touch", key, async move {
            self.validate_key(key)?;
            let mut obj = self.pool.get().await?;
            let result = self.timed(obj.touch(key, expire)).await;
            Self::discard_on_error(obj, result)
        })
        .await
    }

    /// Invalidate all items on the server.
    pub async fn flush_all(&self) -> Result<()> {
        self.run("flushAll", "", async {
            let mut obj = self.pool.get().await?;
            let result = self.timed(obj.flush_all()).await;
            Self::discard_on_error(obj, result)
        })
        .await
    }

    /// Query the server version string.
    pub async fn version(&self) -> Result<String> {
        self.run("version", "", async {
            let mut obj = self.pool.get().await?;
            let result = self.timed(obj.version()).await;
            Self::discard_on_error(obj, result)
        })
        .await
    }

    // --- internals ---

    async fn store(
        &self,
        command: StoreCommand,
        key: &str,
        value: impl ToMemcacheValue,
        expire: impl Into<Expiration>,
        noreply: bool,
    ) -> Result<bool> {
        self.validate_key(key)?;
        let value = value.to_memcache_value();
        let expire = expire.into();
        let mut obj = self.pool.get().await?;
        let result = self
            .timed(obj.store(command, key, &value, expire, noreply))
            .await;
        Self::discard_on_error(obj, result)
    }

    /// Run `op`, logging any error at `error` level in the mesh log format.
    async fn run<T>(
        &self,
        method: &str,
        key: &str,
        op: impl Future<Output = Result<T>>,
    ) -> Result<T> {
        let result = op.await;
        if let Err(ref err) = result {
            tracing::error!(
                error = %err,
                "mc mesh {} error ,namespace:{} ,key: {}",
                method,
                self.config.namespace,
                key
            );
        }
        result
    }

    /// Whether `err` leaves the connection in an unknown state, so it must be
    /// dropped instead of returned to the pool:
    ///
    /// - `Timeout` / `Io` / `Pool`: a timed-out or failed request may leave a
    ///   late response frame in the read buffer, desyncing the stream.
    /// - `Desynced` / `Protocol`: the desync was actually observed, or the
    ///   peer violated the protocol; the stream position can no longer be
    ///   trusted.
    ///
    /// Server/client errors and business results (`InvalidKey`, `Decode`,
    /// `Unsupported`, ...) are complete request/response exchanges and leave
    /// the connection healthy.
    fn is_connection_error(err: &Error) -> bool {
        matches!(
            err,
            Error::Timeout
                | Error::Io(_)
                | Error::Pool(_)
                | Error::Desynced(_)
                | Error::Protocol(_)
        )
    }

    /// Drop `conn` if `result` holds a connection-level error (see
    /// [`Client::is_connection_error`]), then return `result` unchanged.
    fn discard_on_error<T>(conn: Object<Manager>, result: Result<T>) -> Result<T> {
        if let Err(ref err) = result
            && Self::is_connection_error(err)
        {
            // Take the connection out of the pool so it is dropped instead
            // of being recycled with a poisoned read buffer.
            let _ = Object::take(conn);
        }
        result
    }

    /// Await `fut` under the configured per-operation timeout.
    async fn timed<T>(&self, fut: impl Future<Output = Result<T>>) -> Result<T> {
        match timeout(self.config.op_timeout, fut).await {
            Ok(inner) => inner,
            Err(_) => Err(Error::Timeout),
        }
    }

    fn validate_key(&self, key: &str) -> Result<()> {
        if !self.config.validate_keys {
            return Ok(());
        }
        if key.is_empty() {
            return Err(Error::InvalidKey("key must not be empty"));
        }
        if key.len() > MAX_KEY_LEN {
            return Err(Error::InvalidKey("key exceeds 250 bytes"));
        }
        if key.bytes().any(|byte| byte <= b' ' || byte == 0x7f) {
            return Err(Error::InvalidKey(
                "key must not contain spaces or control characters",
            ));
        }
        Ok(())
    }

    /// The endpoint the pool currently dials (updated by mesh rediscovery).
    pub fn current_endpoint(&self) -> Endpoint {
        self.endpoint
            .read()
            .expect("endpoint lock poisoned")
            .clone()
    }

    /// Rescan the mesh registry and, if the advertised endpoint changed,
    /// switch the pool over. Returns `true` if the endpoint changed.
    ///
    /// The rescan is shared: it refreshes the directory-wide snapshot all
    /// clients subscribe to (and wakes their tracker tasks), so one manual
    /// refresh serves every namespace in the same directory. Called with the
    /// latest snapshot by the automatic tracker; exposed so callers can force
    /// a refresh after observing a burst of connect failures.
    pub fn refresh_endpoint(&self) -> Result<bool> {
        if let Some(ref watcher) = self.watcher {
            watcher.rescan();
        }
        self.apply_snapshot()
    }

    /// Apply the latest shared registry snapshot to this client's endpoint,
    /// switching the pool over if it changed. Does not rescan; called by the
    /// tracker task whenever the shared scanner observes a change.
    fn apply_snapshot(&self) -> Result<bool> {
        let Some(ref discovery) = self.config.mesh_discovery else {
            return Ok(false);
        };
        let watcher = self.watcher.as_ref().expect("checked above");
        let found = watcher
            .lookup(discovery.group.as_deref(), &discovery.namespace)
            .ok_or_else(|| {
                Error::MeshDiscovery(format!(
                    "namespace {} no longer advertised in {}",
                    discovery.namespace,
                    discovery.dir.display()
                ))
            })?;
        let changed = {
            let mut current = self.endpoint.write().expect("endpoint lock poisoned");
            if *current == found {
                false
            } else {
                *current = found.clone();
                true
            }
        };
        if changed {
            // Drain idle connections to the old endpoint; in-flight ones fail
            // or complete and are then recycled from the new endpoint.
            self.pool.retain(|_, _| false);
            tracing::info!(
                namespace = %self.config.namespace,
                endpoint = ?found,
                "mc mesh endpoint changed, drained stale connections"
            );
        }
        Ok(changed)
    }

    /// Start a lightweight task that applies registry snapshot changes pushed
    /// by the shared per-directory scanner. The task only wakes when the
    /// snapshot actually changes, so an idle deployment costs nothing.
    fn spawn_endpoint_tracker(&self) {
        let Some(ref watcher) = self.watcher else {
            return;
        };
        let client = self.clone();
        let watcher = watcher.clone();
        tokio::spawn(async move {
            loop {
                watcher.changed().await;
                if let Err(err) = client.apply_snapshot() {
                    tracing::warn!(
                        error = %err,
                        namespace = %client.config.namespace,
                        "mc mesh endpoint refresh failed"
                    );
                }
            }
        });
    }
}
