//! # memcache
//!
//! A high-performance, high-availability async memcached client for the
//! breeze platform, with **two explicitly separated access modes**:
//!
//! ## Mesh mode — [`sidecar`]
//!
//! The SDK talks to the **local breeze mesh agent**, discovered by parsing
//! the sock registry files the mesh publishes (see
//! [`sidecar::discovery`]); the mesh proxies to the real backends and owns
//! sharding and failover. Use [`sidecar::SidecarClient`], configured by
//! [`sidecar::MeshConfig`].
//!
//! ```no_run
//! use memcache::sidecar::SidecarClient;
//!
//! # async fn demo() -> memcache::Result<()> {
//! let client = SidecarClient::connect("my_mc_namespace")?;
//! client.set("greeting", "hello", 60u32).await?;
//! let value = client.get("greeting").await?;
//! assert_eq!(value.unwrap().as_string()?, "hello");
//! # Ok(())
//! # }
//! ```
//!
//! ## Direct backend mode (`direct-mock` feature)
//!
//! Direct-backend types are public only when `direct-mock` is enabled. Their
//! implementation remains available internally to [`CacheService`] and the
//! service templates. The feature exposes `direct::DirectClient` and related
//! sharding/topology types for tests, validation tools, and benchmarks.
//!
//! ```ignore
//! use memcache::direct::{DirectClient, ServerConfig, Shards};
//!
//! # async fn demo() -> memcache::Result<()> {
//! let shards = Shards::new(
//!     "crc32", "modula",
//!     vec!["10.0.0.1:11211".to_string(), "10.0.0.2:11211".to_string()],
//!     vec![
//!         DirectClient::connect(ServerConfig::new("10.0.0.1:11211")?)?,
//!         DirectClient::connect(ServerConfig::new("10.0.0.2:11211")?)?,
//!     ],
//! );
//! // shardingSupport.getClient(key) style:
//! shards.get_client("u:42").set("u:42", "data", 60u32).await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Application API — [`Memcache`], [`CacheService`], and direct access
//!
//! Application code should depend on the small [`Memcache`] contract.
//! [`CacheService`] builds the Java-compatible master/slave/L1 topology from
//! a [`CacheServiceFactory`], while [`SidecarMemcache`] reaches an exact
//! group/namespace through the local breeze sidecar. `DirectMemcache` is
//! available only with the `direct-mock` feature. These facades keep pools and
//! topology implementation types out of the application boundary.
//!
//! ## Unified low-level proxy — [`Client`]
//!
//! [`Client`] wraps the sidecar client by default and additionally exposes its
//! direct variant when `direct-mock` is enabled.
//!
//! ## Shared layers (both pooled modes)
//!
//! - **Protocol core** ([`value`], [`expiration`], [`error`])
//!   — the text and binary wire codecs and the [`Value`]/flags model.
//! - **Connection & pool** (`connection`, `pool`, `maintenance`) — pooled
//!   TCP connections with opaque-based request/response correlation, a
//!   shared minimum-connection maintainer, and TCP keepalive.

pub mod cacheservice;
pub mod client;
pub mod config;
#[cfg(feature = "direct-mock")]
pub mod direct;
#[cfg(not(feature = "direct-mock"))]
#[allow(dead_code, unused_imports)]
#[doc(hidden)]
mod direct;
pub mod error;
pub mod expiration;
#[cfg(not(feature = "service"))]
mod service;
#[cfg(feature = "service")]
pub mod service;
pub mod sidecar;
pub mod value;

mod api;
mod connection;
#[cfg(feature = "direct-mock")]
mod direct_memcache;
mod maintenance;
mod pool;
mod protocol;
mod sidecar_memcache;
#[cfg(feature = "service")]
mod vintage_factory;

pub use api::{
    CacheEntry, CacheService, CacheServiceFactory, CacheServiceOptions, Memcache, SetOptions,
};
pub use cacheservice::{CacheNamespaceConf, CacheServiceConfig, CacheServiceError};
pub use client::Client;
pub use config::{Config, Endpoint, Protocol};
#[cfg(feature = "direct-mock")]
pub use direct_memcache::DirectMemcache;
pub use error::{Error, Result};
pub use expiration::Expiration;
pub use sidecar_memcache::SidecarMemcache;
pub use value::{CasValue, ToMemcacheValue, Value, flags};
#[cfg(feature = "service")]
pub use vintage_factory::{VintageCacheServiceFactory, VintageCacheServiceFactoryError};
