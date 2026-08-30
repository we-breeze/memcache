//! Discovery and transport options for [`crate::CacheService::mesh`].
//!
//! The client talks to a single local mesh agent that proxies to the real
//! memcached backends. Connection is by resource **namespace**: the mesh
//! exposes a per-namespace TCP port discovered from the sock-file directory
//! (see [`super::discovery`]). Hosts honor `MESH_CONNECT_HOST` and otherwise
//! use `127.0.0.1`.

use std::path::PathBuf;
use std::time::Duration;

use brz_discovery::Endpoint;

use crate::config::Protocol;
use crate::error::Result;

use super::discovery;

/// Default directory the local mesh agent writes TCP registry files to.
pub use brz_discovery::DEFAULT_SOCKS_DIR;

/// How to discover and connect to a mesh-proxied memcached resource.
#[derive(Clone, Debug)]
pub struct MeshConfig {
    /// Resource namespace — identifies which backend the mesh routes to.
    pub namespace: String,
    /// Deployment group (part of the sock-file name).
    pub group: String,
    /// Directory the mesh publishes sock registry files in.
    pub socket_dir: PathBuf,
    /// Wire protocol to speak (binary by default, matching the mesh
    /// `PingPongMemcachedBinaryClient`).
    pub protocol: Protocol,
    /// Timeout for establishing a new connection.
    pub connect_timeout: Duration,
    /// Timeout applied to each individual operation (request + response).
    pub op_timeout: Duration,
}

impl MeshConfig {
    /// A config for `namespace` with mesh-appropriate defaults.
    pub fn new(namespace: impl Into<String>) -> Self {
        MeshConfig {
            namespace: namespace.into(),
            group: "default".to_string(),
            socket_dir: PathBuf::from(DEFAULT_SOCKS_DIR),
            protocol: Protocol::default(),
            connect_timeout: Duration::from_millis(500),
            op_timeout: Duration::from_millis(400),
        }
    }

    /// Set the deployment group.
    pub fn with_group(mut self, group: impl Into<String>) -> Self {
        self.group = group.into();
        self
    }

    /// Override the sock-file directory (useful for tests).
    pub fn with_socket_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.socket_dir = dir.into();
        self
    }

    /// Select the wire protocol.
    pub fn with_protocol(mut self, protocol: Protocol) -> Self {
        self.protocol = protocol;
        self
    }

    /// Set the per-operation timeout.
    pub fn with_op_timeout(mut self, timeout: Duration) -> Self {
        self.op_timeout = timeout;
        self
    }

    /// Resolve the current local mesh endpoint.
    pub(crate) fn resolve(&self) -> Result<Endpoint> {
        discovery::discover(&self.socket_dir, &self.group, &self.namespace)
    }
}
