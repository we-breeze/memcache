//! Global pool maintenance: keep every registered client topped up to its
//! configured minimum connection count.
//!
//! One shared background task serves all clients (there may be close to a
//! thousand cache namespaces in a process): every [`MAINTAIN_INTERVAL`] it
//! iterates the registered clients and, for each whose pool has shrunk below
//! `Config::min_connections` — connections may have died while idle, or been
//! drained by an endpoint change — borrows the missing number and returns
//! them, leaving fresh idle connections in the pool. Clients register on
//! construction and deregister automatically when dropped, so the task
//! stops doing work once nothing is left to maintain.

use std::sync::{Mutex, OnceLock, Weak};
use std::time::Duration;

use crate::pool::Pool;

/// How often the maintainer checks all registered clients.
const MAINTAIN_INTERVAL: Duration = Duration::from_secs(10);

/// What the maintainer needs from a client: its pool (to check and top up)
/// and its configured minimum.
pub(crate) struct MaintenanceEntry {
    pub(crate) pool: Pool,
    pub(crate) min_connections: usize,
    pub(crate) namespace: String,
}

fn registry() -> &'static Mutex<Vec<Weak<MaintenanceEntry>>> {
    static REGISTRY: OnceLock<Mutex<Vec<Weak<MaintenanceEntry>>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(Vec::new()))
}

/// Register a client's pool for minimum-connection maintenance and ensure
/// the shared task is running. Call once per client at construction.
pub(crate) fn register(entry: std::sync::Arc<MaintenanceEntry>) {
    let mut registry = registry().lock().unwrap_or_else(|err| err.into_inner());
    registry.push(std::sync::Arc::downgrade(&entry));
    // Spawn the task lazily on first registration. It runs forever but is
    // cheap: one wakeup per interval, iterating only live clients.
    if registry.len() == 1 {
        tokio::spawn(maintain_loop());
    }
}

async fn maintain_loop() {
    let mut ticker = tokio::time::interval(MAINTAIN_INTERVAL);
    ticker.tick().await; // skip the immediate first tick
    loop {
        ticker.tick().await;
        // Collect live entries, dropping dead weak refs as we go.
        let entries: Vec<std::sync::Arc<MaintenanceEntry>> = {
            let mut registry = registry().lock().unwrap_or_else(|err| err.into_inner());
            registry.retain(|weak| weak.strong_count() > 0);
            registry.iter().filter_map(Weak::upgrade).collect()
        };
        for entry in entries {
            top_up(&entry).await;
        }
    }
}

/// Bring `entry`'s pool up to its configured minimum, creating at most the
/// missing number of connections. Borrowed connections are dropped right
/// back into the pool as idle. Failures are logged and skipped — a briefly
/// unavailable mesh endpoint must not stall maintenance of other clients,
/// and the next interval retries anyway.
async fn top_up(entry: &MaintenanceEntry) {
    let min = entry.min_connections.min(entry.pool.status().max_size);
    let missing = min.saturating_sub(entry.pool.status().size);
    if missing == 0 {
        return;
    }
    let mut conns = Vec::with_capacity(missing);
    for _ in 0..missing {
        match entry.pool.get().await {
            Ok(conn) => conns.push(conn),
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    namespace = %entry.namespace,
                    "mc mesh maintain: failed to top up connection"
                );
                break;
            }
        }
    }
    drop(conns);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use crate::config::Config;
    use crate::pool::Manager;

    /// A pool whose connections all fail to establish (nothing listens on
    /// the port): `top_up` must log-and-skip without panicking or hanging,
    /// leaving the pool empty.
    #[tokio::test]
    async fn top_up_survives_connect_failures() {
        let config =
            Arc::new(Config::tcp("127.0.0.1", 1).with_connect_timeout(Duration::from_millis(50)));
        let endpoint = Arc::new(std::sync::RwLock::new(config.endpoint.clone()));
        let pool = Pool::builder(Manager::new(config, endpoint))
            .max_size(4)
            .build()
            .unwrap();
        let entry = MaintenanceEntry {
            pool: pool.clone(),
            min_connections: 3,
            namespace: "test".into(),
        };
        top_up(&entry).await;
        assert_eq!(pool.status().size, 0);
    }
}
