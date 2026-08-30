//! The [`Cacheable`] trait — the Rust analogue of the Java `CacheAble<T>`
//! interface, and the dispatch unit the template routes operations through.

#![allow(dead_code)]

use std::collections::HashMap;

use crate::value::{CasValue, Value};
use crate::{Expiration, Result};
use async_trait::async_trait;

/// The cache operation surface the template routes through.
///
/// Kept private as the dispatch contract between the public facade and its
/// concrete topology, plus test-only injected backends.
#[async_trait]
pub(crate) trait Cacheable: Send + Sync {
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
