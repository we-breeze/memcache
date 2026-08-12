//! Shared mesh registry scanner.
//!
//! A large deployment may run close to a thousand cache namespaces in one
//! process, most of them served by the same mesh socks directory. Giving each
//! [`crate::Client`] its own rediscovery task would mean a thousand timers
//! and a thousand identical directory scans. Instead, all clients watching
//! the same directory share **one** background task: it scans the directory
//! every [`SCAN_INTERVAL`] and broadcasts the resulting
//! `(group, namespace) → endpoint` snapshot through a [`watch`] channel.
//! Each client compares the snapshot against its own endpoint and switches
//! over independently.
//!
//! The per-directory task is reference-counted: it starts on the first
//! subscription and stops when the last subscriber is dropped.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use arc_swap::ArcSwap;
use tokio::sync::Notify;

use super::discovery::{self, SockKey};
use crate::config::Endpoint;

/// How often each watched socks directory is rescanned.
const SCAN_INTERVAL: Duration = Duration::from_secs(5);

/// A snapshot of all memcached endpoints advertised in one socks directory.
type Snapshot = Arc<ArcSwap<HashMap<SockKey, Endpoint>>>;

/// Handle a client holds to receive snapshots for one directory.
pub(crate) struct DirectoryWatcher {
    /// The latest snapshot (lock-free reads).
    snapshot: Snapshot,
    /// Signalled whenever the snapshot changes.
    notify: Arc<Notify>,
    /// The watched directory; dropping this handle unsubscribes.
    dir: PathBuf,
}

impl DirectoryWatcher {
    /// Rescan the directory synchronously and update the shared snapshot,
    /// notifying all watchers if it changed. Used by manual refreshes; the
    /// background task does the same on its interval.
    pub(crate) fn rescan(&self) {
        let scanned = discovery::scan_all(&self.dir);
        if **self.snapshot.load() != scanned {
            self.snapshot.store(Arc::new(scanned));
            self.notify.notify_waiters();
        }
    }

    /// The current endpoint for `group`/`namespace`, if advertised.
    pub(crate) fn lookup(&self, group: Option<&str>, namespace: &str) -> Option<Endpoint> {
        let map = self.snapshot.load();
        // Prefer an exact group match; fall back to any group serving this
        // namespace (mirrors `discover_matching` with `group = None`).
        if let Some(group) = group {
            let key = SockKey {
                group: Some(group.to_string()),
                namespace: namespace.to_string(),
            };
            if let Some(endpoint) = map.get(&key) {
                return Some(endpoint.clone());
            }
        }
        map.iter()
            .find(|(key, _)| key.namespace == namespace)
            .map(|(_, endpoint)| endpoint.clone())
    }

    /// Wait until the snapshot changes, then return.
    pub(crate) async fn changed(&self) {
        self.notify.notified().await;
    }
}

impl Drop for DirectoryWatcher {
    fn drop(&mut self) {
        let mut registry = registry().lock().unwrap_or_else(|err| err.into_inner());
        if let Some(entry) = registry.get_mut(&self.dir) {
            entry.subscribers -= 1;
            if entry.subscribers == 0 {
                // Stop the scan task and remove the directory entry.
                entry.abort.abort();
                registry.remove(&self.dir);
            }
        }
    }
}

/// Per-directory shared state.
struct ScannerEntry {
    snapshot: Snapshot,
    notify: Arc<Notify>,
    abort: tokio::task::AbortHandle,
    subscribers: usize,
}

fn registry() -> &'static Mutex<HashMap<PathBuf, ScannerEntry>> {
    static REGISTRY: OnceLock<Mutex<HashMap<PathBuf, ScannerEntry>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Subscribe to snapshots of `dir`, starting the shared scan task if this is
/// the first subscriber. The initial snapshot is taken synchronously so the
/// watcher is usable immediately.
pub(crate) fn watch(dir: PathBuf) -> DirectoryWatcher {
    let mut registry = registry().lock().unwrap_or_else(|err| err.into_inner());
    let entry = registry.entry(dir.clone()).or_insert_with(|| {
        let snapshot: Snapshot = Arc::new(ArcSwap::from_pointee(discovery::scan_all(&dir)));
        let notify = Arc::new(Notify::new());
        let task = {
            let snapshot = snapshot.clone();
            let notify = notify.clone();
            let dir = dir.clone();
            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(SCAN_INTERVAL);
                ticker.tick().await; // skip the immediate first tick
                loop {
                    ticker.tick().await;
                    let scanned = discovery::scan_all(&dir);
                    // Only broadcast on an actual change, so idle watchers
                    // are never woken.
                    if **snapshot.load() != scanned {
                        snapshot.store(Arc::new(scanned));
                        notify.notify_waiters();
                    }
                }
            })
        };
        ScannerEntry {
            snapshot,
            notify,
            abort: task.abort_handle(),
            subscribers: 0,
        }
    });
    entry.subscribers += 1;
    DirectoryWatcher {
        snapshot: entry.snapshot.clone(),
        notify: entry.notify.clone(),
        dir,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn watchers_share_one_scanner_entry() {
        let dir = tempfile::tempdir().unwrap();
        let w1 = watch(dir.path().to_path_buf());
        let w2 = watch(dir.path().to_path_buf());
        assert_eq!(
            registry().lock().unwrap_or_else(|e| e.into_inner())[dir.path()].subscribers,
            2,
            "two watchers on one dir share a single scanner entry"
        );
        drop(w1);
        assert_eq!(
            registry().lock().unwrap_or_else(|e| e.into_inner())[dir.path()].subscribers,
            1
        );
        drop(w2);
        assert!(
            !registry()
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .contains_key(dir.path()),
            "last drop removes the scanner entry"
        );
    }

    #[tokio::test]
    async fn rescan_picks_up_new_entries() {
        let dir = tempfile::tempdir().unwrap();
        let watcher = watch(dir.path().to_path_buf());
        assert!(watcher.lookup(Some("g"), "ns").is_none());

        std::fs::write(
            dir.path()
                .join("config.example.com+3+config+v1+g+all:ns@mc:9461@cs"),
            [],
        )
        .unwrap();
        watcher.rescan();
        assert_eq!(
            watcher.lookup(Some("g"), "ns"),
            Some(Endpoint::Tcp {
                host: "127.0.0.1".into(),
                port: 9461
            })
        );
    }

    #[test]
    fn lookup_prefers_exact_group_then_any_group() {
        let snapshot: Snapshot = Arc::new(ArcSwap::from_pointee(HashMap::from([
            (
                SockKey {
                    group: Some("a".into()),
                    namespace: "ns".into(),
                },
                Endpoint::Tcp {
                    host: "127.0.0.1".into(),
                    port: 1,
                },
            ),
            (
                SockKey {
                    group: Some("b".into()),
                    namespace: "ns".into(),
                },
                Endpoint::Tcp {
                    host: "127.0.0.1".into(),
                    port: 2,
                },
            ),
        ])));
        let watcher = DirectoryWatcher {
            snapshot,
            notify: Arc::new(Notify::new()),
            dir: PathBuf::from("/nonexistent"),
        };
        assert_eq!(
            watcher.lookup(Some("b"), "ns"),
            Some(Endpoint::Tcp {
                host: "127.0.0.1".into(),
                port: 2
            })
        );
        // Unknown group falls back to any entry for the namespace.
        assert!(watcher.lookup(Some("zzz"), "ns").is_some());
        assert!(watcher.lookup(None, "ns").is_some());
        assert!(watcher.lookup(None, "missing").is_none());
    }
}
