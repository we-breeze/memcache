//! High-performance asynchronous memcached access through one unified
//! [`CacheService`] facade.
//!
//! - [`CacheService::single`] builds one fixed master with one shard/node.
//! - [`CacheService::new`] builds a fixed replica/shard topology.
//! - [`CacheService::new_live`] accepts application-owned configuration sources.

pub mod cacheservice;
pub mod config;
pub mod error;
pub mod expiration;
pub mod value;

mod api;
mod cache_topology;
#[cfg(feature = "metrics")]
mod profile_metrics;
mod service;
mod session_protocol;
mod sharding;

pub use api::{
    CacheEntry, CacheService, CacheServiceConfigSource, CacheServiceOptions, Memcache, SetOptions,
    SubscriptionHandle,
};
pub use cacheservice::{CacheNamespaceConf, CacheServiceConfig, CacheServiceError};
pub use config::Protocol;
pub use error::{Error, Result};
pub use expiration::Expiration;
pub use value::{CasValue, ToMemcacheValue, Value, flags};
