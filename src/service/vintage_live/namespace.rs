//! [`CacheServiceInner`] — one namespace's hot-swappable backend with a
//! two-level diff.

use std::sync::{Arc, Mutex};

use arc_swap::{ArcSwap, ArcSwapAny};

use crate::api::{BackendSlot, CacheServiceOptions};
use crate::cacheservice::{CacheNamespaceConf, CacheServiceConfig, CacheServiceError};
use crate::service::{MemCacheTemplate as Topology, PoolOptions};

/// Errors raised while applying a namespace update.
#[derive(Debug, thiserror::Error)]
pub enum NamespaceApplyError {
    /// The namespace was deleted from the group; the old backend is retained.
    #[error("namespace {0:?} missing from updated group")]
    NamespaceMissing(String),
    /// The namespace content was not valid UTF-8.
    #[error("namespace content is not valid UTF-8: {0}")]
    Utf8(#[from] std::str::Utf8Error),
    /// The namespace YAML could not be parsed.
    #[error(transparent)]
    Config(#[from] CacheServiceError),
    /// The backend topology could not be rebuilt.
    #[error("memcache backend build error: {0}")]
    Build(#[from] crate::Error),
}

/// One namespace's live backend. Holds the current topology behind an
/// `ArcSwapAny` shared with its [`crate::CacheService`], plus the last
/// successfully applied config (for the two-level semantic diff).
pub struct CacheServiceInner {
    namespace: Box<str>,
    options: CacheServiceOptions,
    backend: Arc<ArcSwapAny<Arc<BackendSlot>>>,
    applied: ArcSwap<CacheNamespaceConf>,
    /// Drop hook: lets the group remove this namespace's weak ref promptly when
    /// the last `CacheService` holding the inner drops. Receives the namespace
    /// name and a fresh `Weak` to `self` (already dropping); the group matches
    /// by the strong it stored, so a dead weak simply triggers lazy cleanup.
    unregister: Mutex<Option<Box<dyn FnOnce() + Send + Sync>>>,
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
    /// the [`crate::CacheService`] so swaps are visible to it.
    pub(crate) fn new(
        namespace: Box<str>,
        conf: CacheNamespaceConf,
        backend: Arc<ArcSwapAny<Arc<BackendSlot>>>,
        options: CacheServiceOptions,
    ) -> Arc<Self> {
        Arc::new(Self {
            namespace,
            options,
            backend,
            applied: ArcSwap::from_pointee(conf),
            unregister: Mutex::new(None),
        })
    }

    /// The namespace name.
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Registers a drop hook so the group can prune this namespace's weak ref
    /// when the inner is dropped.
    pub(crate) fn set_unregister<F>(&self, f: F)
    where
        F: FnOnce() + Send + Sync + 'static,
    {
        *self.unregister.lock().expect("unregister mutex poisoned") = Some(Box::new(f));
    }

    /// Applies an updated namespace content. Two-level diff:
    /// 1. (caller-side, at the group level) skip if the namespace bytes are
    ///    unchanged;
    /// 2. here: parse the YAML; if the parsed config equals the last applied
    ///    config, skip the rebuild;
    /// 3. otherwise build a new topology and atomically swap it in.
    ///
    /// On any failure the old backend is retained (failure semantics).
    pub fn apply(
        &self,
        content: Option<vintage::ConfigContent>,
    ) -> Result<(), NamespaceApplyError> {
        let content = content
            .ok_or_else(|| NamespaceApplyError::NamespaceMissing(self.namespace.to_string()))?;
        let yaml = content.as_str()?;
        let next = CacheServiceConfig::from_yaml_str(yaml)?
            .namespace(&self.namespace)
            .cloned()
            .ok_or_else(|| NamespaceApplyError::NamespaceMissing(self.namespace.to_string()))?;

        // Level 2: semantic equality → skip rebuild (e.g. only comments/format
        // changed, or crc32 normalization produced the same config).
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

impl Drop for CacheServiceInner {
    fn drop(&mut self) {
        if let Some(unregister) = self
            .unregister
            .lock()
            .expect("unregister mutex poisoned")
            .take()
        {
            unregister();
        }
    }
}
