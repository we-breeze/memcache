//! Client configuration for connecting to the breeze mesh (byMesh path).
//!
//! The client talks to a single local mesh agent that proxies to the real
//! memcached backends. Connection is by resource **namespace**: the mesh
//! exposes a per-namespace TCP port discovered from the sock-file directory
//! (see [`super::discovery`]). Hosts honor `MESH_CONNECT_HOST` and otherwise
//! use `127.0.0.1`.

use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::config::{Config, Protocol};
use crate::error::{Error, Result};

use super::discovery::{self, MeshDiscovery};

/// Default directory the mesh sidecar writes socks registry files to.
pub use brz_discovery::DEFAULT_SOCKS_DIR;

/// How to reach and pool a mesh-proxied memcached resource.
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
    /// Minimum pooled connections (established at startup, kept topped up
    /// by the shared maintenance task; 0 = fully lazy).
    pub min_connections: usize,
    /// Maximum pooled connections (grows on demand up to this cap).
    pub max_connections: usize,
    /// Timeout for establishing a new connection.
    pub connect_timeout: Duration,
    /// Timeout applied to each individual operation (request + response).
    pub op_timeout: Duration,
    /// Maximum time to wait for a pooled connection to become available.
    pub pool_wait_timeout: Duration,
    /// Enable TCP keepalive on pooled connections.
    pub tcp_keepalive: bool,
    /// Idle time after which TCP keepalive probes start.
    pub keepalive_interval: Duration,
    /// Reject invalid keys client-side (see [`Config::validate_keys`]).
    pub validate_keys: bool,
}

impl MeshConfig {
    /// A config for `namespace` with mesh-appropriate defaults.
    pub fn new(namespace: impl Into<String>) -> Self {
        MeshConfig {
            namespace: namespace.into(),
            group: "default".to_string(),
            socket_dir: PathBuf::from(DEFAULT_SOCKS_DIR),
            protocol: Protocol::default(),
            min_connections: 2,
            max_connections: 128,
            connect_timeout: Duration::from_millis(500),
            op_timeout: Duration::from_millis(400),
            pool_wait_timeout: Duration::from_millis(500),
            tcp_keepalive: true,
            keepalive_interval: Duration::from_secs(10),
            validate_keys: true,
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

    /// Override the minimum pooled connection count (0 = fully lazy).
    pub fn with_min_connections(mut self, n: usize) -> Self {
        self.min_connections = n;
        self
    }

    /// Override the maximum pooled connection count.
    pub fn with_max_connections(mut self, n: usize) -> Self {
        self.max_connections = n.max(1);
        self
    }

    /// Set the per-operation timeout.
    pub fn with_op_timeout(mut self, timeout: Duration) -> Self {
        self.op_timeout = timeout;
        self
    }

    /// Copy the pool/timeout/protocol settings into an engine [`Config`]
    /// built on `endpoint`, and attach the rediscovery coordinates.
    fn apply(&self, mut config: Config, discovery: MeshDiscovery) -> Config {
        config.protocol = self.protocol;
        config.min_connections = self.min_connections;
        config.max_connections = self.max_connections;
        config.connect_timeout = self.connect_timeout;
        config.op_timeout = self.op_timeout;
        config.pool_wait_timeout = self.pool_wait_timeout;
        config.tcp_keepalive = self.tcp_keepalive;
        config.keepalive_interval = self.keepalive_interval;
        config.validate_keys = self.validate_keys;
        config.mesh_discovery = Some(discovery);
        config
    }

    /// Discover the endpoint in the configured socket directory and build
    /// the engine [`Config`].
    pub(crate) fn resolve(&self) -> Result<Config> {
        let endpoint = discovery::discover(&self.socket_dir, &self.group, &self.namespace)?;
        let config = Config::new(endpoint).with_namespace(&self.namespace);
        Ok(self.apply(
            config,
            MeshDiscovery {
                dir: self.socket_dir.clone(),
                group: Some(self.group.clone()),
                namespace: self.namespace.clone(),
            },
        ))
    }

    /// Build a config by **directly parsing** a mesh socks registry file
    /// name, e.g. `…+<group>+all:<namespace>@mc:<port>@cs`; parsed in place
    /// with no remote fetch. `path` may be the full path to the file or just
    /// its name (resolved against [`DEFAULT_SOCKS_DIR`]).
    pub(crate) fn resolve_sock(&self, path: impl AsRef<Path>) -> Result<Config> {
        let path = path.as_ref();
        let (dir, name) = match (path.parent(), path.file_name()) {
            (Some(parent), Some(name)) if !parent.as_os_str().is_empty() => (parent, name),
            (_, Some(name)) => (Path::new(DEFAULT_SOCKS_DIR), name),
            _ => {
                return Err(Error::MeshDiscovery(format!(
                    "sock path has no file name: {}",
                    path.display()
                )));
            }
        };
        let name = name
            .to_str()
            .ok_or_else(|| Error::MeshDiscovery("sock file name is not UTF-8".into()))?;
        let (endpoint, key) = discovery::endpoint_from_name(dir, name)?;
        let (namespace, group) = match key {
            Some(key) => (key.namespace, key.group),
            None => (self.namespace.clone(), None),
        };
        let config = Config::new(endpoint).with_namespace(&namespace);
        Ok(self.apply(
            config,
            MeshDiscovery {
                dir: dir.to_path_buf(),
                group,
                namespace,
            },
        ))
    }
}
