use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use deadpool::Runtime;
use deadpool::managed::Timeouts;
use tokio::time::timeout;

use crate::config::{Config, MAX_KEY_LEN};
use crate::error::{Error, Result};
use crate::expiration::Expiration;
use crate::pool::{Manager, Pool};
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
}

impl Client {
    /// Build a client and its connection pool from `config`.
    pub fn new(config: Config) -> Result<Self> {
        let config = Arc::new(config);
        let manager = Manager::new(config.clone());
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
        Ok(Client { pool, config })
    }

    // --- reads ---

    /// Fetch a single value.
    pub async fn get(&self, key: &str) -> Result<Option<Value>> {
        self.run("get", key, async {
            self.validate_key(key)?;
            let mut obj = self.pool.get().await?;
            let conn = &mut *obj;
            self.timed(conn.get(key)).await
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
            let conn = &mut *obj;
            self.timed(conn.get_multi(keys)).await
        })
        .await
    }

    /// Fetch a value together with its CAS token.
    pub async fn get_cas(&self, key: &str) -> Result<Option<CasValue>> {
        self.run("getCas", key, async {
            self.validate_key(key)?;
            let mut obj = self.pool.get().await?;
            let conn = &mut *obj;
            self.timed(conn.get_cas(key)).await
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
            let conn = &mut *obj;
            self.timed(conn.cas(key, &value.value, expire, value.cas, false))
                .await
        })
        .await
    }

    /// Delete a key. Returns `true` if the key existed.
    pub async fn delete(&self, key: &str) -> Result<bool> {
        self.run("delete", key, async {
            self.validate_key(key)?;
            let mut obj = self.pool.get().await?;
            let conn = &mut *obj;
            self.timed(conn.delete(key, false)).await
        })
        .await
    }

    /// Delete a key without waiting for a reply.
    pub async fn delete_with_noreply(&self, key: &str) -> Result<()> {
        self.run("deleteWithNoreply", key, async {
            self.validate_key(key)?;
            let mut obj = self.pool.get().await?;
            let conn = &mut *obj;
            self.timed(conn.delete(key, true)).await
        })
        .await
        .map(|_| ())
    }

    /// Atomically increment a counter. Returns `None` if the key is missing.
    pub async fn incr(&self, key: &str, delta: u64) -> Result<Option<u64>> {
        self.run("incr", key, async {
            self.validate_key(key)?;
            let mut obj = self.pool.get().await?;
            let conn = &mut *obj;
            self.timed(conn.incr_decr(true, key, delta, false)).await
        })
        .await
    }

    /// Atomically decrement a counter. Returns `None` if the key is missing.
    pub async fn decr(&self, key: &str, delta: u64) -> Result<Option<u64>> {
        self.run("decr", key, async {
            self.validate_key(key)?;
            let mut obj = self.pool.get().await?;
            let conn = &mut *obj;
            self.timed(conn.incr_decr(false, key, delta, false)).await
        })
        .await
    }

    /// Update a key's expiration without fetching its value.
    pub async fn touch(&self, key: &str, expire: impl Into<Expiration>) -> Result<bool> {
        let expire = expire.into();
        self.run("touch", key, async move {
            self.validate_key(key)?;
            let mut obj = self.pool.get().await?;
            let conn = &mut *obj;
            self.timed(conn.touch(key, expire)).await
        })
        .await
    }

    /// Invalidate all items on the server.
    pub async fn flush_all(&self) -> Result<()> {
        self.run("flushAll", "", async {
            let mut obj = self.pool.get().await?;
            let conn = &mut *obj;
            self.timed(conn.flush_all()).await
        })
        .await
    }

    /// Query the server version string.
    pub async fn version(&self) -> Result<String> {
        self.run("version", "", async {
            let mut obj = self.pool.get().await?;
            let conn = &mut *obj;
            self.timed(conn.version()).await
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
        let conn = &mut *obj;
        self.timed(conn.store(command, key, &value, expire, noreply))
            .await
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
}
