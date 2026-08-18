//! [`VintageCacheServices`] — the application entry point and shared group
//! registry for live [`crate::CacheService`]s.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};

use crate::api::{BackendSlot, CacheService, CacheServiceOptions};
use crate::cacheservice::{CacheServiceConfig, CacheServiceError};
use crate::service::{Cacheable, MemCacheTemplate as Topology, PoolOptions};

use super::group::CacheServiceGroup;
use super::namespace::CacheServiceInner;

/// Errors raised while subscribing to a live cache-service namespace.
#[derive(Debug, thiserror::Error)]
pub enum VintageAdapterError {
    /// The Vintage live-value subscription failed.
    #[error("vintage live-value error: {0}")]
    Vintage(#[from] vintage::LiveError),
    /// The cache-service group YAML could not be split or parsed.
    #[error(transparent)]
    Config(#[from] CacheServiceError),
    /// The requested namespace was absent from the group.
    #[error("namespace {0:?} not found in group")]
    MissingNamespace(String),
    /// The initial backend topology could not be built.
    #[error("memcache backend build error: {0}")]
    Build(#[from] crate::Error),
}

/// Application entry point: a shared registry of live cache-service groups.
///
/// One `VintageCacheServices` per process. Clones share the group registry, so
/// every [`CacheService`] for the same group shares one Vintage poll task.
#[derive(Clone)]
pub struct VintageCacheServices {
    inner: Arc<VintageCacheServicesInner>,
}

/// One group's live handle plus a weak reference to its current value. The
/// `Live` keeps the Vintage poll alive; the weak lets the registry detect when
/// all `CacheService`s for the group have dropped (so the group can be evicted
/// and the poll stopped).
type GroupEntry = (vintage::Live<CacheServiceGroup>, Weak<CacheServiceGroup>);

struct VintageCacheServicesInner {
    vintage: vintage::Client,
    /// group name → live handle + weak value. Dropping the `Live` (on eviction)
    /// stops the group's poll.
    groups: Mutex<HashMap<String, GroupEntry>>,
}

impl VintageCacheServices {
    /// Creates a new shared registry backed by `vintage_client`.
    pub fn new(vintage_client: vintage::Client) -> Self {
        Self {
            inner: Arc::new(VintageCacheServicesInner {
                vintage: vintage_client,
                groups: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// Subscribes to `(group, namespace)` with default options and returns a
    /// live [`CacheService`] whose backend is hot-swapped on config change.
    pub async fn subscribe(
        &self,
        group: impl Into<String>,
        namespace: impl Into<String>,
    ) -> Result<CacheService, VintageAdapterError> {
        self.subscribe_with_options(group, namespace, CacheServiceOptions::default())
            .await
    }

    /// Subscribes to `(group, namespace)` with explicit options.
    pub async fn subscribe_with_options(
        &self,
        group: impl Into<String>,
        namespace: impl Into<String>,
        options: CacheServiceOptions,
    ) -> Result<CacheService, VintageAdapterError> {
        let group_name = group.into();
        let ns_name = namespace.into();
        let group = self.get_or_create_group(&group_name).await?;

        // Build the initial backend from the current group document.
        let content = group
            .document()
            .namespace(&ns_name)
            .ok_or_else(|| VintageAdapterError::MissingNamespace(ns_name.clone()))?;
        let yaml = content
            .as_str()
            .map_err(|e| VintageAdapterError::Config(CacheServiceError::Yaml(e.to_string())))?;
        let conf = CacheServiceConfig::from_yaml_str(yaml)?
            .namespace(&ns_name)
            .cloned()
            .ok_or_else(|| VintageAdapterError::MissingNamespace(ns_name.clone()))?;

        let pool = PoolOptions::default().with_protocol(options.protocol);
        let topology = Arc::new(
            Topology::from_namespace_conf(&conf, pool)?
                .with_default_expiration(options.default_expiration),
        ) as Arc<dyn Cacheable>;

        // Shared backend slot between the CacheService and the inner driver.
        let backend = Arc::new(arc_swap::ArcSwapAny::new(Arc::new(BackendSlot(topology))));
        let inner = CacheServiceInner::new(
            ns_name.clone().into_boxed_str(),
            conf,
            backend.clone(),
            options,
        );

        // Register the inner with the group so updates reach it. Wire the drop
        // hook so the group prunes promptly when the inner is dropped.
        group.register(&ns_name, &inner);
        let group_weak = Arc::downgrade(&group);
        let ns_for_drop = ns_name.clone().into_boxed_str();
        inner.set_unregister(move || {
            if let Some(g) = group_weak.upgrade() {
                // Mark for pruning; the actual eviction of the group happens
                // lazily on the next get_or_create_group call.
                if g.prune() {
                    // All subscribers gone; the Live will be dropped on the next
                    // registry access, stopping the poll.
                }
            }
            let _ = ns_for_drop;
        });

        Ok(CacheService::from_live(
            backend,
            options,
            inner as Arc<dyn std::fmt::Debug + Send + Sync>,
        ))
    }

    /// Returns the live [`CacheServiceGroup`] for `group_name`, creating (and
    /// subscribing) it on first use. Evicts dead groups (whose last
    /// `CacheService` has dropped) before creating a fresh one.
    async fn get_or_create_group(
        &self,
        group_name: &str,
    ) -> Result<Arc<CacheServiceGroup>, VintageAdapterError> {
        // Fast path: a live group is registered and its weak upgrades.
        {
            let groups = self.inner.groups.lock().expect("groups mutex poisoned");
            if let Some((_, weak)) = groups.get(group_name)
                && let Some(group) = weak.upgrade()
            {
                return Ok(group);
            }
        }
        // The registered group is dead (or absent): evict and create fresh.
        {
            let mut groups = self.inner.groups.lock().expect("groups mutex poisoned");
            if let Some((_, weak)) = groups.get(group_name)
                && weak.upgrade().is_none()
            {
                groups.remove(group_name);
            }
        }
        // Subscribe to the group via the Vintage live-value API. The Live
        // handle is kept in the registry; dropping it stops the poll.
        let live: vintage::Live<CacheServiceGroup> =
            self.inner.vintage.live(group_name.to_owned()).await?;
        let group = live.load();
        {
            let mut groups = self.inner.groups.lock().expect("groups mutex poisoned");
            // Re-check: another task may have created the same group concurrently.
            if let Some((existing_live, weak)) = groups.get(group_name)
                && let Some(existing) = weak.upgrade()
            {
                let _ = existing_live;
                return Ok(existing);
            }
            groups.insert(group_name.to_owned(), (live, Arc::downgrade(&group)));
        }
        Ok(group)
    }
}
