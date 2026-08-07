//! Async memcached client for the breeze *byMesh* path.
//!
//! This crate implements a high-performance, single-endpoint memcached client
//! that speaks both the **text** and **binary** wire protocols. It targets the
//! deployment shape used by `MeshMemcacheTemplate` in breeze-sdk-core: the
//! client connects to a single mesh sidecar (a `host:port` endpoint or a unix
//! socket) and relies on a connection pool for concurrency and availability.
//! Client-side multi-server sharding / consistent hashing (the *byTcp* path) is
//! intentionally out of scope.
//!
//! # Example
//!
//! ```no_run
//! use memcache::{Client, Config, Protocol};
//!
//! # async fn run() -> memcache::Result<()> {
//! let config = Config::tcp("127.0.0.1", 11211).with_protocol(Protocol::Binary);
//! let client = Client::new(config)?;
//!
//! client.set("greeting", "hello", 60).await?;
//! let value = client.get("greeting").await?;
//! assert_eq!(value.and_then(|v| v.as_string().ok()), Some("hello".to_string()));
//! # Ok(())
//! # }
//! ```

mod client;
mod config;
mod connection;
mod discovery;
mod error;
mod expiration;
mod maintenance;
mod mesh;
mod pool;
mod protocol;
mod value;

pub use client::Client;
pub use config::{Config, Endpoint, MeshDiscovery, Protocol};
pub use error::{Error, Result};
pub use expiration::Expiration;
pub use mesh::DEFAULT_SOCKS_DIR;
pub use value::{CasValue, ToMemcacheValue, Value, flags};
