//! The [`Cacheable`] trait — the Rust analogue of the Java `CacheAble<T>`
//! interface, and the dispatch unit the template routes operations through.

use std::collections::HashMap;

use crate::value::{CasValue, Value};
use crate::{Client, Expiration, Result};
use async_trait::async_trait;

/// The cache operation surface the template routes through.
///
/// Implemented by the SDK's [`Client`] (primary cache-service access) and
/// expected to be implemented by whatever backup cache the caller wires in
/// (the Java original routes to a `MemCacheTemplate` over direct mc pools).
///
/// Object-safe so a template can hold `Arc<dyn Cacheable>` backups.
#[async_trait]
pub trait Cacheable: Send + Sync {
    /// Fetch a single value; `Ok(None)` on a miss.
    async fn get(&self, key: &str) -> Result<Option<Value>>;

    /// Fetch multiple values; missing keys are absent from the map.
    async fn get_multi(&self, keys: &[&str]) -> Result<HashMap<String, Value>>;

    /// Store a value unconditionally.
    async fn set(&self, key: &str, value: Value, expire: Expiration) -> Result<bool>;

    /// Store a value without waiting for the server's reply semantics to
    /// matter — the result is discarded (the SDK has no true noreply op).
    async fn set_with_noreply(&self, key: &str, value: Value, expire: Expiration) -> Result<()>;

    /// Store only if the key does not already exist.
    async fn add(&self, key: &str, value: Value, expire: Expiration) -> Result<bool>;

    /// Fetch a value together with its CAS token.
    async fn get_cas(&self, key: &str) -> Result<Option<CasValue>>;

    /// Compare-and-swap.
    async fn cas(&self, key: &str, value: &CasValue, expire: Expiration) -> Result<bool>;

    /// Delete a key.
    async fn delete(&self, key: &str) -> Result<bool>;

    /// Delete a key, discarding the reply.
    async fn delete_with_noreply(&self, key: &str) -> Result<()>;
}

#[async_trait]
impl Cacheable for Client {
    async fn get(&self, key: &str) -> Result<Option<Value>> {
        Client::get(self, key).await
    }

    async fn get_multi(&self, keys: &[&str]) -> Result<HashMap<String, Value>> {
        Client::get_multi(self, keys).await
    }

    async fn set(&self, key: &str, value: Value, expire: Expiration) -> Result<bool> {
        Client::set(self, key, value, expire).await
    }

    async fn set_with_noreply(&self, key: &str, value: Value, expire: Expiration) -> Result<()> {
        Client::set(self, key, value, expire).await?;
        Ok(())
    }

    async fn add(&self, key: &str, value: Value, expire: Expiration) -> Result<bool> {
        Client::add(self, key, value, expire).await
    }

    async fn get_cas(&self, key: &str) -> Result<Option<CasValue>> {
        Client::get_cas(self, key).await
    }

    async fn cas(&self, key: &str, value: &CasValue, expire: Expiration) -> Result<bool> {
        Client::cas(self, key, value, expire).await
    }

    async fn delete(&self, key: &str) -> Result<bool> {
        Client::delete(self, key).await
    }

    async fn delete_with_noreply(&self, key: &str) -> Result<()> {
        Client::delete(self, key).await?;
        Ok(())
    }
}
