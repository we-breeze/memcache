//! # memcache
//!
//! A high-performance, high-availability async memcached client for the
//! breeze platform, with **three explicitly separated access modes**:
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
//! ## Direct backend mode — [`direct`]
//!
//! The SDK connects to memcached backends directly (no mesh), with
//! client-side shard routing ([`direct::Shards`], the `shardingSupport`
//! pattern) using the same hash/distribution algorithms as the mesh
//! ([`direct::sharding`]).
//!
//! ```no_run
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
//! ## Replay mode — [`replay`] (feature `direct-tcp`)
//!
//! A direct-TCP **text-protocol** client for replay/comparison topologies:
//! single persistent connection ([`replay::ReplayConnection`]) and the
//! crc32/modula node-selection pool ([`replay::MemcachePool`]) that
//! reproduces the source service's `SockIOPool.NEW_COMPAT_HASH` routing, so
//! a replay proxy can lane-match recorded memcached exchanges.
//!
//! ## Unified proxy — [`Client`]
//!
//! [`Client`] is a mode-agnostic enum over the sidecar and direct clients,
//! exposing the whole memcached operation surface regardless of which access
//! mode a resource uses.
//!
//! ## Shared layers (both pooled modes)
//!
//! - **Protocol core** ([`value`], [`expiration`], [`error`])
//!   — the text and binary wire codecs and the [`Value`]/flags model.
//! - **Connection & pool** (`connection`, `pool`, `maintenance`) — pooled
//!   TCP/unix connections with opaque-based request/response correlation, a
//!   shared minimum-connection maintainer, and TCP keepalive.

pub mod cacheservice;
pub mod client;
pub mod config;
pub mod direct;
pub mod error;
pub mod expiration;
#[cfg(feature = "service")]
pub mod service;
pub mod sidecar;
pub mod value;

#[cfg(feature = "direct-tcp")]
pub mod replay;

mod connection;
mod maintenance;
mod pool;
mod protocol;

pub use client::Client;
pub use config::{Config, Endpoint, Protocol};
pub use error::{Error, Result};
pub use expiration::Expiration;
pub use value::{CasValue, ToMemcacheValue, Value, flags};

// Backwards-compatible root re-exports for the replay mode.
#[cfg(feature = "direct-tcp")]
pub use replay::{MemcacheError, MemcacheGet, MemcachePool, new_compat_hash, text_get};
