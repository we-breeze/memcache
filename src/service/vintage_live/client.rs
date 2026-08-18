//! [`VintageCacheServiceConfigSource`] — a [`CacheServiceConfigSource`] backed
//! by a Vintage statics-config group. Vintage's subscription unit is the
//! *group*; this source shares one Vintage poll per group and pushes
//! per-namespace config updates to its subscriber.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};

use crate::Result;
use crate::api::{CacheServiceConfigSource, SubscriptionHandle};
use crate::cacheservice::{CacheNamespaceConf, CacheServiceError};

use super::group::CacheServiceGroup;
use super::namespace::CacheServiceInner;

/// Errors raised while subscribing to a live cache-service namespace.
#[derive(Debug, thiserror::Error)]
pub(crate) enum VintageAdapterError {
    /// The Vintage live-value subscription failed.
    #[error("vintage live-value error: {0}")]
    Vintage(#[from] vintage::LiveError),
    /// The cache-service group YAML could not be split or parsed.
    #[error(transparent)]
    Config(#[from] CacheServiceError),
}

/// One group's live handle plus a weak reference to its current value. The
/// `Live` keeps the Vintage poll alive; the weak lets the registry detect when
/// all sources for the group have dropped (so the group can be evicted and the
/// poll stopped).
type GroupEntry = (vintage::Live<CacheServiceGroup>, Weak<CacheServiceGroup>);

/// A [`CacheServiceConfigSource`] backed by a Vintage statics-config group.
///
/// Multiple sources for the same `(vintage client, group)` share one Vintage
/// poll task via an internal group registry. Each source subscribes to one
/// namespace within the group; the group pushes that namespace's parsed config
/// when its bytes change.
#[derive(Clone)]
pub(crate) struct VintageCacheServiceConfigSource {
    inner: Arc<Inner>,
    group: String,
    namespace: String,
}

struct Inner {
    vintage: vintage::Client,
    /// group name → live handle + weak value. Dropping the `Live` (on
    /// eviction) stops the group's poll.
    groups: Mutex<HashMap<String, GroupEntry>>,
}

impl VintageCacheServiceConfigSource {
    /// Creates a source for `(group, namespace)` backed by `vintage_client`.
    pub fn new(
        vintage_client: vintage::Client,
        group: impl Into<String>,
        namespace: impl Into<String>,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                vintage: vintage_client,
                groups: Mutex::new(HashMap::new()),
            }),
            group: group.into(),
            namespace: namespace.into(),
        }
    }
}

#[async_trait::async_trait]
impl CacheServiceConfigSource for VintageCacheServiceConfigSource {
    async fn load(&self) -> Result<CacheNamespaceConf> {
        let group = self.get_or_create_group().await.map_err(adapter_to_crate)?;
        let content = group.document().namespace(&self.namespace).ok_or_else(|| {
            crate::Error::Protocol(format!(
                "namespace {:?} not found in group {:?}",
                self.namespace, self.group
            ))
        })?;
        let yaml = content
            .as_str()
            .map_err(|e| crate::Error::Protocol(format!("namespace content is not UTF-8: {e}")))?;
        let conf = CacheServiceInner::parse_namespace_yaml(&self.namespace, yaml)
            .map_err(crate::Error::from)?;
        Ok(conf)
    }

    async fn subscribe(
        &self,
        on_update: Arc<dyn Fn(CacheNamespaceConf) + Send + Sync>,
    ) -> Result<SubscriptionHandle> {
        let group = self.get_or_create_group().await.map_err(adapter_to_crate)?;
        let sink = group.register(&self.namespace, on_update);

        // On drop: remove this sink from the group; if the group is now empty,
        // evict it from the registry (which drops the Live and stops the poll).
        let groups = Arc::downgrade(&self.inner);
        let group_name = self.group.clone();
        let namespace = self.namespace.clone();
        let group_weak = Arc::downgrade(&group);
        Ok(SubscriptionHandle::new(move || {
            if let Some(group) = group_weak.upgrade() {
                let empty = group.remove_sink(&namespace, &sink);
                if empty && let Some(inner) = groups.upgrade() {
                    let mut g = inner.groups.lock().expect("groups mutex poisoned");
                    if g.get(&group_name)
                        .is_some_and(|(_, weak)| weak.upgrade().is_none())
                    {
                        g.remove(&group_name);
                    }
                }
            }
        }))
    }
}

impl VintageCacheServiceConfigSource {
    /// Returns the live [`CacheServiceGroup`] for this source's group, creating
    /// (and subscribing) it on first use. Evicts dead groups before creating a
    /// fresh one.
    async fn get_or_create_group(
        &self,
    ) -> std::result::Result<Arc<CacheServiceGroup>, VintageAdapterError> {
        // Fast path: a live group is registered and its weak upgrades.
        {
            let groups = self.inner.groups.lock().expect("groups mutex poisoned");
            if let Some((_, weak)) = groups.get(&self.group)
                && let Some(group) = weak.upgrade()
            {
                return Ok(group);
            }
        }
        // The registered group is dead (or absent): evict and create fresh.
        {
            let mut groups = self.inner.groups.lock().expect("groups mutex poisoned");
            if let Some((_, weak)) = groups.get(&self.group)
                && weak.upgrade().is_none()
            {
                groups.remove(&self.group);
            }
        }
        // Subscribe to the group via the Vintage live-value API. The Live
        // handle is kept in the registry; dropping it stops the poll.
        let live: vintage::Live<CacheServiceGroup> = self
            .inner
            .vintage
            .live(self.group.clone())
            .await
            .map_err(VintageAdapterError::from)?;
        let group = live.load();
        {
            let mut groups = self.inner.groups.lock().expect("groups mutex poisoned");
            // Re-check: another task may have created the same group concurrently.
            if let Some((existing_live, weak)) = groups.get(&self.group)
                && let Some(existing) = weak.upgrade()
            {
                let _ = existing_live;
                return Ok(existing);
            }
            groups.insert(self.group.clone(), (live, Arc::downgrade(&group)));
        }
        Ok(group)
    }
}

fn adapter_to_crate(error: VintageAdapterError) -> crate::Error {
    match error {
        VintageAdapterError::Vintage(e) => crate::Error::Protocol(format!("vintage: {e}")),
        VintageAdapterError::Config(e) => crate::Error::from(e),
    }
}
