//! [`CacheServiceGroup`] — the [`vintage::Subscriber`] for one statics-config
//! group. It owns the parsed [`GroupDocument`] and dispatches per-namespace
//! updates to registered [`CacheServiceInner`]s.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};

use arc_swap::ArcSwap;

use super::document::GroupDocument;
use super::namespace::CacheServiceInner;

/// One statics-config group's live state. The group's `GroupDocument` is
/// refreshed in place via an `ArcSwap`; registered namespace backends are
/// notified only when their namespace's bytes change.
pub struct CacheServiceGroup {
    document: ArcSwap<GroupDocument>,
    /// Registered namespace backends. `Weak` so a dropped `CacheService` is
    /// collected automatically; the group is evicted from
    /// [`super::client::VintageCacheServices`] when all weaks are dead.
    subscribers: Mutex<HashMap<Box<str>, Vec<Weak<CacheServiceInner>>>>,
}

impl std::fmt::Debug for CacheServiceGroup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CacheServiceGroup").finish_non_exhaustive()
    }
}

impl CacheServiceGroup {
    /// The current group document.
    pub fn document(&self) -> arc_swap::Guard<Arc<GroupDocument>> {
        self.document.load()
    }

    /// Registers a namespace backend to receive updates for `namespace`.
    pub(crate) fn register(&self, namespace: &str, inner: &Arc<CacheServiceInner>) {
        let mut subs = self.subscribers.lock().expect("subscribers mutex poisoned");
        subs.entry(namespace.into())
            .or_default()
            .push(Arc::downgrade(inner));
    }

    /// Drops dead weaks across all namespaces; returns whether the group now
    /// has no live subscribers at all (so the registry may evict it).
    pub(crate) fn prune(&self) -> bool {
        let mut subs = self.subscribers.lock().expect("subscribers mutex poisoned");
        subs.retain(|_, weaks| {
            weaks.retain(|w| w.strong_count() > 0);
            !weaks.is_empty()
        });
        subs.is_empty()
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

        // Snapshot live subscribers under the lock, then release before calling
        // apply (apply may take its own locks / build pools).
        let snapshot: Vec<(Box<str>, Vec<Arc<CacheServiceInner>>)> = {
            let subs = self.subscribers.lock().expect("subscribers mutex poisoned");
            subs.iter()
                .map(|(ns, weaks)| {
                    let live: Vec<Arc<CacheServiceInner>> =
                        weaks.iter().filter_map(Weak::upgrade).collect();
                    (ns.clone(), live)
                })
                .collect()
        };

        for (ns, inners) in &snapshot {
            let old = prev.namespace(ns);
            let new = next.namespace(ns);
            // Level 1 diff: skip if the namespace bytes are unchanged.
            if old.as_ref().map(|c| c.as_bytes()) == new.as_ref().map(|c| c.as_bytes()) {
                continue;
            }
            for inner in inners {
                // apply does the level-2 (semantic) diff. On error the old
                // backend is retained; we log but do not abort the whole
                // update (other namespaces still proceed).
                if let Err(error) = inner.apply(new.clone()) {
                    tracing::warn!(
                        namespace = %ns,
                        %error,
                        "vintage live cache-service namespace apply failed; retaining old backend"
                    );
                }
            }
        }

        // Publish the new document.
        self.document.store(next);
        // The group identity is stable (the subscriber registry persists); we
        // never replace Self. Return Ok(None) so Live<CacheServiceGroup> does
        // not swap the group itself.
        Ok(None)
    }
}
