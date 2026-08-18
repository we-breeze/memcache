//! [`CacheServiceGroup`] — the [`vintage::Subscriber`] for one statics-config
//! group. It owns the parsed [`GroupDocument`] and dispatches per-namespace
//! updates to registered source-subscription callbacks.
//!
//! Vintage's subscription unit is the group; the update unit pushed to each
//! subscriber is one parsed namespace config. The group does the first level of
//! the two-level diff (skip when a namespace's raw bytes are unchanged) and
//! pushes parsed [`CacheNamespaceConf`]s to the registered callbacks; the
//! second level (skip rebuild when the parsed config is semantically equal)
//! lives in `CacheServiceInner::apply`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arc_swap::ArcSwap;

use super::document::GroupDocument;

/// A per-namespace source subscription callback: invoked with the newly parsed
/// namespace config when that namespace's bytes change.
type NamespaceSink = Arc<dyn Fn(crate::cacheservice::CacheNamespaceConf) + Send + Sync>;

/// One statics-config group's live state. The group's `GroupDocument` is
/// refreshed in place via an `ArcSwap`; registered namespace callbacks are
/// notified only when their namespace's bytes change.
pub(crate) struct CacheServiceGroup {
    document: ArcSwap<GroupDocument>,
    /// Registered namespace callbacks. Each callback is kept alive by the
    /// `SubscriptionHandle` returned to the source; the group evicts a callback
    /// when its handle drops (via `remove_sink`).
    subscribers: Mutex<HashMap<Box<str>, Vec<NamespaceSink>>>,
}

impl std::fmt::Debug for CacheServiceGroup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CacheServiceGroup").finish_non_exhaustive()
    }
}

impl CacheServiceGroup {
    /// The current group document.
    pub(crate) fn document(&self) -> arc_swap::Guard<Arc<GroupDocument>> {
        self.document.load()
    }

    /// Registers a per-namespace callback. Returns the `Sink` so the caller's
    /// `SubscriptionHandle` can hold it and remove it on drop.
    pub(crate) fn register(
        &self,
        namespace: &str,
        sink: NamespaceSink,
    ) -> Arc<dyn Fn(crate::cacheservice::CacheNamespaceConf) + Send + Sync> {
        let mut subs = self.subscribers.lock().expect("subscribers mutex poisoned");
        subs.entry(namespace.into())
            .or_default()
            .push(Arc::clone(&sink));
        sink
    }

    /// Removes the first callback identified by pointer identity from
    /// `namespace`. Returns whether the group now has no live subscribers at
    /// all (so the registry may evict it).
    pub(crate) fn remove_sink(&self, namespace: &str, sink: &NamespaceSink) -> bool {
        let mut subs = self.subscribers.lock().expect("subscribers mutex poisoned");
        if let Some(weaks) = subs.get_mut(namespace) {
            if let Some(pos) = weaks.iter().position(|s| Arc::ptr_eq(s, sink)) {
                weaks.swap_remove(pos);
            }
            if weaks.is_empty() {
                subs.remove(namespace);
            }
        }
        subs.values().all(|v| v.is_empty())
    }
}

impl vintage::Subscriber for CacheServiceGroup {
    fn from_content(content: vintage::ConfigContent) -> Result<Self, vintage::BoxError> {
        let document = GroupDocument::parse(content)?;
        Ok(Self {
            document: ArcSwap::from_pointee(document),
            subscribers: Mutex::new(HashMap::new()),
        })
    }

    fn on_update(
        &self,
        content: vintage::ConfigContent,
    ) -> Result<Option<Self>, vintage::BoxError> {
        // Parse the new document. On failure, retain the old group document
        // (failure semantics: "group 文本无法切分 → 整个 group 保留旧版本").
        let next = Arc::new(GroupDocument::parse(content)?);
        let prev = self.document.load_full();

        // Snapshot live callbacks under the lock, then release before invoking
        // them (a callback may build pools / take locks).
        let snapshot: Vec<(Box<str>, NamespaceSink)> = {
            let subs = self.subscribers.lock().expect("subscribers mutex poisoned");
            subs.iter()
                .flat_map(|(ns, sinks)| sinks.iter().map(|s| (ns.clone(), Arc::clone(s))))
                .collect()
        };

        for (ns, sink) in &snapshot {
            let old = prev.namespace(ns);
            let new = next.namespace(ns);
            // Level 1 diff: skip if the namespace bytes are unchanged.
            if old.as_ref().map(|c| c.as_bytes()) == new.as_ref().map(|c| c.as_bytes()) {
                continue;
            }
            // Parse the changed namespace slice; on failure retain the old
            // backend for this namespace (failure semantics).
            let conf = match new.as_ref().and_then(|c| c.as_str().ok()).and_then(|yaml| {
                super::namespace::CacheServiceInner::parse_namespace_yaml(ns, yaml).ok()
            }) {
                Some(conf) => conf,
                None => {
                    tracing::warn!(
                        namespace = %ns,
                        "vintage live cache-service namespace parse failed; retaining old backend"
                    );
                    continue;
                }
            };
            // Push the parsed config to the subscriber's callback. The
            // callback (CacheService::new_live's on_update) does the level-2
            // semantic diff + atomic swap.
            sink(conf);
        }

        // Publish the new document.
        self.document.store(next);
        // The group identity is stable (the subscriber registry persists); we
        // never replace Self. Return Ok(None) so Live<CacheServiceGroup> does
        // not swap the group itself.
        Ok(None)
    }
}
