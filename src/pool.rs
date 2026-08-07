use std::sync::Arc;

use deadpool::managed::{self, Metrics, RecycleResult};

use crate::config::Config;
use crate::connection::Connection;
use crate::error::Error;

/// deadpool manager that creates and health-checks [`Connection`]s.
pub(crate) struct Manager {
    config: Arc<Config>,
}

impl Manager {
    pub(crate) fn new(config: Arc<Config>) -> Self {
        Manager { config }
    }
}

impl managed::Manager for Manager {
    type Type = Connection;
    type Error = Error;

    async fn create(&self) -> Result<Connection, Error> {
        Connection::connect(&self.config).await
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
