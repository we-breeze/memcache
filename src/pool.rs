use std::sync::Arc;

use deadpool::managed::{self, Metrics, RecycleError, RecycleResult};

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
        // A successful VERSION round-trip proves the connection is still usable.
        conn.version()
            .await
            .map(|_| ())
            .map_err(RecycleError::Backend)
    }
}

/// The connection pool type used by the client.
pub(crate) type Pool = managed::Pool<Manager>;
