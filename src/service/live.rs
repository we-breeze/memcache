//! Apply configuration updates atomically, retaining the last valid topology.
use std::sync::{Arc, Mutex};

use arc_swap::{ArcSwap, ArcSwapAny};

use crate::api::{BackendSlot, CacheServiceOptions};
use crate::cacheservice::CacheNamespaceConf;

/// Errors raised while applying a namespace config update.
#[derive(Debug, thiserror::Error)]
pub(crate) enum NamespaceApplyError {
    /// The backend topology could not be rebuilt.
    #[error("memcache backend build error: {0}")]
    Build(#[from] crate::Error),
}

/// One namespace's live backend. Holds the current topology behind an
/// `ArcSwapAny` shared with its [`crate::CacheService`], plus the last
/// successfully applied config (for the semantic diff).
pub(crate) struct CacheServiceInner {
    update_gate: Mutex<()>,
    options: CacheServiceOptions,
    backend: Arc<ArcSwapAny<Arc<BackendSlot>>>,
    applied: ArcSwap<CacheNamespaceConf>,
}

impl std::fmt::Debug for CacheServiceInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CacheServiceInner").finish_non_exhaustive()
    }
}

impl CacheServiceInner {
    /// Creates a new inner backend. The `backend` `ArcSwapAny` is shared with
    /// the [`crate::CacheService`] so swaps are visible to it. `initial` is the
    /// config the first backend was built from (stored as the semantic-diff
    /// baseline).
    pub(crate) fn new(
        initial: CacheNamespaceConf,
        backend: Arc<ArcSwapAny<Arc<BackendSlot>>>,
        options: CacheServiceOptions,
    ) -> Arc<Self> {
        Arc::new(Self {
            update_gate: Mutex::new(()),
            options,
            backend,
            applied: ArcSwap::from_pointee(initial),
        })
    }

    /// Applies an updated namespace config. Semantic diff: if the parsed config
    /// equals the last applied config, skip the rebuild (e.g. only
    /// comments/format changed). Otherwise build a new topology and atomically swap it in. On
    /// any failure the old backend is retained (failure semantics).
    pub fn apply(&self, next: CacheNamespaceConf) -> Result<(), NamespaceApplyError> {
        // Serialize callbacks so the backend and comparison baseline agree.
        let _update = self.update_gate.lock().expect("update gate poisoned");
        // Semantic equality → skip rebuild.
        if **self.applied.load() == next {
            return Ok(());
        }

        let current = self.backend.load_full();
        let backend = crate::CacheService::build_backend(&next, self.options, current.topology())?;
        // Build succeeded → atomic swap.
        self.backend.store(Arc::new(backend));
        self.applied.store(Arc::new(next));
        Ok(())
    }
}
