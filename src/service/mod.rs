//! # Cache-service templates (feature `service`)
//!
//! A Rust port of the Java `commons-memcache` `CacheServiceTemplate` /
//! `MemCacheTemplate`, layered on this crate's clients:
//!
//! - [`Cacheable`] — the async trait analogue of the Java `CacheAble<T>`
//!   interface; implemented by [`crate::Client`], [`ShardedCache`], and
//!   [`MemCacheTemplate`].
//! - [`ShardedCache`] — one sharded pool of direct backends.
//! - [`MemCacheTemplate`] — the multi-tier (master / slave / L1) backup
//!   cache with the Java read cascade, write fan-out, and policy switches.
//! - [`MemcacheServiceTemplate`] — routes operations to a primary
//!   cache-service client with automatic fallback to a backup cache, with
//!   the Java switcher semantics and split `get_multi`.
//! - [`vintage`] — builds and hot-swaps the backup from a Vintage
//!   statics-config group (Java `CacheServiceRecoveryConfigNotifer` flow).
//!
//! # Example
//!
//! ```ignore
//! use memcache::service::MemcacheServiceTemplate;
//! use memcache::sidecar::SidecarClient;
//!
//! # async fn demo() -> memcache::Result<()> {
//! let primary = SidecarClient::connect("my_mc_namespace")?;
//! let template = MemcacheServiceTemplate::builder()
//!     .primary_client(primary)
//!     .use_primary(true)
//!     .expire_minutes(60)
//!     .build();
//!
//! template.set("greeting", "hello").await?;
//! let value = template.get("greeting").await?;
//! # Ok(())
//! # }
//! ```

#![cfg_attr(not(feature = "service"), allow(dead_code, unused_imports))]

mod cacheable;
mod memcache_template;
mod sharded;
mod template;
#[cfg(feature = "service")]
pub mod vintage;

pub use cacheable::Cacheable;
pub use memcache_template::{MemCacheTemplate, MemCacheTemplateBuilder, PoolOptions, WritePolicy};
pub use sharded::ShardedCache;
pub use template::{
    MULTI_GET_MAX_COUNT, MULTI_GET_SPLIT_STEP, MULTI_GET_TIMEOUT, MemcacheServiceTemplate,
    MemcacheServiceTemplateBuilder, global_switch, set_global_switch, set_sync_multi_get_switch,
    sync_multi_get_switch,
};
#[cfg(feature = "service")]
pub use vintage::{
    VintageSourceError, backup_from_vintage, backup_from_vintage_with, spawn_config_watcher,
};
