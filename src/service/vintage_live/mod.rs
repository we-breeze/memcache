//! Vintage-backed live [`crate::CacheService`]: one shared Vintage subscription
//! per group, fanned out to per-namespace hot-swap.
//!
//! This mirrors the Java `CacheServiceRecoveryConfigNotifer` flow but shares a
//! single Vintage poll per group (via [`vintage::Client::live`]) and diffs at
//! the namespace level. Vintage's subscription unit is the *group*; the
//! `CacheService` update unit is the *namespace*.
//!
//! - `VintageCacheServiceConfigSource` implements [`crate::CacheServiceConfigSource`]
//!   for one `(group, namespace)`; it is the push source consumed by
//!   [`crate::CacheService::from_vintage`] (and `CacheService::new_live`).
//! - `GroupDocument` splits the group's YAML into namespace byte ranges
//!   without copying the body (zero-copy [`vintage::ConfigContent`] slices).
//! - `CacheServiceGroup` is the [`vintage::Subscriber`] for one group; on
//!   update it diffs each registered namespace and pushes parsed configs to
//!   the registered source callbacks.
//! - `CacheServiceInner` is the apply core (two-level semantic diff) driven
//!   by `CacheService::new_live`.
//!
//! This module is `pub(crate)`: business code uses
//! [`crate::CacheService::from_vintage`] and never names these types directly.

mod client;
mod document;
mod group;
mod namespace;

pub(crate) use client::VintageCacheServiceConfigSource;
pub(crate) use namespace::CacheServiceInner;
