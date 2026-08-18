//! [`CacheServiceInner`] — one namespace's hot-swappable backend with a
//! two-level semantic diff.
//!
//! This is the apply core driven by [`crate::CacheService::new_live`]: a config
//! source pushes parsed [`CacheNamespaceConf`]s, and the inner rebuilds the
//! backend only when the parsed config actually changes (the second level of
//! the two-level diff). The first level (skip when the raw bytes are
//! unchanged) lives in the Vintage adapter's `CacheServiceGroup`, which slices
//! the group body per namespace before parsing.

use std::sync::Arc;

use arc_swap::{ArcSwap, ArcSwapAny};

use crate::api::{BackendSlot, CacheServiceOptions};
use crate::cacheservice::{CacheNamespaceConf, CacheServiceConfig, CacheServiceError};
use crate::service::{MemCacheTemplate as Topology, PoolOptions};

/// Errors raised while applying a namespace config update.
#[derive(Debug, thiserror::Error)]
pub enum NamespaceApplyError {
    /// The namespace content could not be parsed.
    #[error(transparent)]
    Config(#[from] CacheServiceError),
    /// The backend topology could not be rebuilt.
    #[error("memcache backend build error: {0}")]
    Build(#[from] crate::Error),
}

/// One namespace's live backend. Holds the current topology behind an
/// `ArcSwapAny` shared with its [`crate::CacheService`], plus the last
/// successfully applied config (for the semantic diff).
pub struct CacheServiceInner {
    namespace: Box<str>,
    options: CacheServiceOptions,
    backend: Arc<ArcSwapAny<Arc<BackendSlot>>>,
    applied: ArcSwap<CacheNamespaceConf>,
}

impl std::fmt::Debug for CacheServiceInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CacheServiceInner")
            .field("namespace", &self.namespace)
            .finish_non_exhaustive()
    }
}

impl CacheServiceInner {
    /// Creates a new inner backend. The `backend` `ArcSwapAny` is shared with
    /// the [`crate::CacheService`] so swaps are visible to it. `initial` is the
    /// config the first backend was built from (stored as the semantic-diff
    /// baseline).
    pub(crate) fn new(
        namespace: impl Into<Box<str>>,
        initial: CacheNamespaceConf,
        backend: Arc<ArcSwapAny<Arc<BackendSlot>>>,
        options: CacheServiceOptions,
    ) -> Arc<Self> {
        Arc::new(Self {
            namespace: namespace.into(),
            options,
            backend,
            applied: ArcSwap::from_pointee(initial),
        })
    }

    /// The namespace name.
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Parses one namespace's YAML slice into a `CacheNamespaceConf`. Used by
    /// the Vintage adapter's initial load and per-namespace update dispatch.
    pub fn parse_namespace_yaml(
        namespace: &str,
        yaml: &str,
    ) -> Result<CacheNamespaceConf, CacheServiceError> {
        CacheServiceConfig::from_yaml_str(yaml)?
            .namespace(namespace)
            .cloned()
            .ok_or_else(|| CacheServiceError::MissingNamespace(namespace.to_string()))
    }

    /// Applies an updated namespace config. Semantic diff: if the parsed config
    /// equals the last applied config, skip the rebuild (e.g. only
    /// comments/format changed, or crc32 normalization produced the same
    /// config). Otherwise build a new topology and atomically swap it in. On
    /// any failure the old backend is retained (failure semantics).
    pub fn apply(&self, next: CacheNamespaceConf) -> Result<(), NamespaceApplyError> {
        // Semantic equality → skip rebuild.
        if **self.applied.load() == next {
            return Ok(());
        }

        let pool = PoolOptions::default().with_protocol(self.options.protocol);
        let topology = Topology::from_namespace_conf(&next, pool)?
            .with_default_expiration(self.options.default_expiration);
        // Build succeeded → atomic swap.
        self.backend
            .store(Arc::new(BackendSlot(Arc::new(topology))));
        self.applied.store(Arc::new(next));
        Ok(())
    }
}
