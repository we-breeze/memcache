//! Vintage-backed live [`crate::CacheService`]: one shared Vintage subscription
//! per group, fanned out to per-namespace hot-swap.
//!
//! This mirrors the Java `CacheServiceRecoveryConfigNotifer` flow but shares a
//! single Vintage poll per group (via [`vintage::Client::live`]) and diffs at
//! the namespace level. Vintage's subscription unit is the *group*; the
//! `CacheService` update unit is the *namespace*.
//!
//! - [`VintageCacheServices`] is the application entry point and the shared
//!   group registry.
//! - [`GroupDocument`] splits the group's YAML into namespace byte ranges
//!   without copying the body (zero-copy [`vintage::ConfigContent`] slices).
//! - [`CacheServiceGroup`] is the [`vintage::Subscriber`] for one group; on
//!   update it diffs each registered namespace and rebuilds only the changed
//!   ones.
//! - [`CacheServiceInner`] holds one namespace's hot-swappable backend with a
//!   two-level diff (text unchanged → skip parse; parsed config equal → skip
//!   rebuild).

mod client;
mod document;
mod group;
mod namespace;

pub use client::{VintageAdapterError, VintageCacheServices};
pub use document::{GroupDocument, GroupDocumentError};
pub use group::CacheServiceGroup;
pub use namespace::CacheServiceInner;
