use std::sync::{Arc, RwLock};

use deadpool::managed::{self, Metrics, RecycleResult};

use crate::config::{Config, Endpoint};
use crate::connection::Connection;
use crate::error::Error;

/// The endpoint currently in effect, shared between the client (which updates
/// it during mesh rediscovery) and the pool manager (which reads it whenever
/// a new connection is established).
pub(crate) type SharedEndpoint = Arc<RwLock<Endpoint>>;

/// deadpool manager that creates and health-checks [`Connection`]s.
pub(crate) struct Manager {
    config: Arc<Config>,
    endpoint: SharedEndpoint,
}

impl Manager {
    pub(crate) fn new(config: Arc<Config>, endpoint: SharedEndpoint) -> Self {
        Manager { config, endpoint }
    }
}

impl managed::Manager for Manager {
    type Type = Connection;
    type Error = Error;

    async fn create(&self) -> Result<Connection, Error> {
        let endpoint = self
            .endpoint
            .read()
            .expect("endpoint lock poisoned")
            .clone();
        Connection::connect(&self.config, &endpoint).await
    }

    async fn recycle(&self, conn: &mut Connection, _: &Metrics) -> RecycleResult<Error> {
        // No VERSION probe: connection health is established by the
        // request/response correlation itself. The binary protocol matches
        // each response to its request by opaque token (a mismatch surfaces as
        // `Error::Desynced`), and `Client` explicitly drops any connection
        // that fails or times out instead of returning it to the pool. A
        // probe here would add a full round-trip to every operation for no
        // additional safety.
        let _ = conn;
        Ok(())
    }
}

/// The connection pool type used by the client.
pub(crate) type Pool = managed::Pool<Manager>;
