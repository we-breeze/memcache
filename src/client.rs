//! The mode-agnostic [`Client`] — a unified proxy over the access modes.
//!
//! `Client` is an enum over [`crate::sidecar::SidecarClient`] (mesh access)
//! and [`crate::direct::DirectClient`] (direct backend access), exposing the
//! whole memcached operation surface on either — useful for code paths that
//! serve resources of mixed modes.

use std::collections::HashMap;

use crate::direct::DirectClient;
use crate::error::Result;
use crate::expiration::Expiration;
use crate::sidecar::SidecarClient;
use crate::value::{CasValue, ToMemcacheValue, Value};

/// A client proxy over either access mode.
#[derive(Clone)]
pub enum Client {
    /// Mesh access mode (see [`crate::sidecar`]).
    Sidecar(SidecarClient),
    /// Direct backend access mode (see [`crate::direct`]).
    Direct(DirectClient),
}

impl Client {
    /// The wrapped sidecar client, if in mesh mode.
    pub fn as_sidecar(&self) -> Option<&SidecarClient> {
        match self {
            Client::Sidecar(client) => Some(client),
            Client::Direct(_) => None,
        }
    }

    /// The wrapped direct client, if in direct mode.
    pub fn as_direct(&self) -> Option<&DirectClient> {
        match self {
            Client::Direct(client) => Some(client),
            Client::Sidecar(_) => None,
        }
    }

    /// Fetch a single value.
    pub async fn get(&self, key: &str) -> Result<Option<Value>> {
        match self {
            Client::Sidecar(client) => client.get(key).await,
            Client::Direct(client) => client.get(key).await,
        }
    }

    /// Fetch multiple values in one round-trip per backend.
    pub async fn get_multi(&self, keys: &[&str]) -> Result<HashMap<String, Value>> {
        match self {
            Client::Sidecar(client) => client.get_multi(keys).await,
            Client::Direct(client) => client.get_multi(keys).await,
        }
    }

    /// Fetch a value together with its CAS token.
    pub async fn get_cas(&self, key: &str) -> Result<Option<CasValue>> {
        match self {
            Client::Sidecar(client) => client.get_cas(key).await,
            Client::Direct(client) => client.get_cas(key).await,
        }
    }

    /// Store a value unconditionally.
    pub async fn set(
        &self,
        key: &str,
        value: impl ToMemcacheValue,
        expire: impl Into<Expiration>,
    ) -> Result<bool> {
        match self {
            Client::Sidecar(client) => client.set(key, value, expire).await,
            Client::Direct(client) => client.set(key, value, expire).await,
        }
    }

    /// Store only if the key does not already exist.
    pub async fn add(
        &self,
        key: &str,
        value: impl ToMemcacheValue,
        expire: impl Into<Expiration>,
    ) -> Result<bool> {
        match self {
            Client::Sidecar(client) => client.add(key, value, expire).await,
            Client::Direct(client) => client.add(key, value, expire).await,
        }
    }

    /// Store only if the key already exists.
    pub async fn replace(
        &self,
        key: &str,
        value: impl ToMemcacheValue,
        expire: impl Into<Expiration>,
    ) -> Result<bool> {
        match self {
            Client::Sidecar(client) => client.replace(key, value, expire).await,
            Client::Direct(client) => client.replace(key, value, expire).await,
        }
    }

    /// Append data to an existing value.
    pub async fn append(&self, key: &str, value: impl ToMemcacheValue) -> Result<bool> {
        match self {
            Client::Sidecar(client) => client.append(key, value).await,
            Client::Direct(client) => client.append(key, value).await,
        }
    }

    /// Prepend data to an existing value.
    pub async fn prepend(&self, key: &str, value: impl ToMemcacheValue) -> Result<bool> {
        match self {
            Client::Sidecar(client) => client.prepend(key, value).await,
            Client::Direct(client) => client.prepend(key, value).await,
        }
    }

    /// Compare-and-swap.
    pub async fn cas(
        &self,
        key: &str,
        value: &CasValue,
        expire: impl Into<Expiration>,
    ) -> Result<bool> {
        match self {
            Client::Sidecar(client) => client.cas(key, value, expire).await,
            Client::Direct(client) => client.cas(key, value, expire).await,
        }
    }

    /// Delete a key.
    pub async fn delete(&self, key: &str) -> Result<bool> {
        match self {
            Client::Sidecar(client) => client.delete(key).await,
            Client::Direct(client) => client.delete(key).await,
        }
    }

    /// Atomically increment a counter.
    pub async fn incr(&self, key: &str, delta: u64) -> Result<Option<u64>> {
        match self {
            Client::Sidecar(client) => client.incr(key, delta).await,
            Client::Direct(client) => client.incr(key, delta).await,
        }
    }

    /// Atomically decrement a counter.
    pub async fn decr(&self, key: &str, delta: u64) -> Result<Option<u64>> {
        match self {
            Client::Sidecar(client) => client.decr(key, delta).await,
            Client::Direct(client) => client.decr(key, delta).await,
        }
    }

    /// Update a key's expiration without fetching its value.
    pub async fn touch(&self, key: &str, expire: impl Into<Expiration>) -> Result<bool> {
        match self {
            Client::Sidecar(client) => client.touch(key, expire).await,
            Client::Direct(client) => client.touch(key, expire).await,
        }
    }

    /// Invalidate all items on the server(s) behind this client.
    pub async fn flush_all(&self) -> Result<()> {
        match self {
            Client::Sidecar(client) => client.flush_all().await,
            Client::Direct(client) => client.flush_all().await,
        }
    }

    /// Query the server version string.
    pub async fn version(&self) -> Result<String> {
        match self {
            Client::Sidecar(client) => client.version().await,
            Client::Direct(client) => client.version().await,
        }
    }
}

impl From<SidecarClient> for Client {
    fn from(client: SidecarClient) -> Self {
        Client::Sidecar(client)
    }
}

impl From<DirectClient> for Client {
    fn from(client: DirectClient) -> Self {
        Client::Direct(client)
    }
}
