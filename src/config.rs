use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::error::{Error, Result};
use crate::mesh;

/// Wire protocol spoken to the memcached endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Protocol {
    /// Classic ASCII/text protocol (`cn.vika.memcached` compatible).
    Text,
    /// Binary protocol (`com.schooner.MemCached` compatible).
    #[default]
    Binary,
}

/// The single endpoint the client connects to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Endpoint {
    /// A TCP `host:port` endpoint.
    Tcp { host: String, port: u16 },
    /// A unix domain socket path.
    Unix { path: PathBuf },
}

/// Client configuration.
///
/// Mirrors the settings applied by `PingPongMemcachedBinaryClient` /
/// `TextMemcacheClient` in breeze-sdk-core: a single endpoint, a bounded
/// connection pool, TCP_NODELAY, and socket-level timeouts.
#[derive(Debug, Clone)]
pub struct Config {
    /// Endpoint to connect to.
    pub endpoint: Endpoint,
    /// Mesh rediscovery coordinates, set when the endpoint was discovered via
    /// the socks registry ([`Config::mesh`] / [`Config::mesh_in`] /
    /// [`Config::sock`]). When present, the client periodically rescans the
    /// registry and follows endpoint changes (e.g. a mesh port reassignment);
    /// pooled connections to the old endpoint are drained on change.
    pub mesh_discovery: Option<MeshDiscovery>,
    /// Protocol to speak.
    pub protocol: Protocol,
    /// Namespace used to identify this client in logs (matches the mesh
    /// `namespace`). Empty by default; set by [`Config::mesh`] or
    /// [`Config::with_namespace`].
    pub namespace: String,
    /// Maximum number of pooled connections.
    pub max_connections: usize,
    /// Minimum number of pooled connections, established eagerly at startup
    /// and kept topped up afterwards by a shared global maintenance task, so
    /// requests never pay connection-establishment latency — including after
    /// idle connections died or were drained by an endpoint change. `0`
    /// disables prewarming and maintenance. Capped at
    /// [`Config::max_connections`].
    pub min_connections: usize,
    /// Enable TCP keepalive on pooled connections (no-op for unix sockets).
    /// Probes begin after [`Config::keepalive_interval`] of idleness, so
    /// half-open connections to a crashed mesh are reaped by the kernel
    /// instead of failing a request later.
    pub tcp_keepalive: bool,
    /// Idle time after which TCP keepalive probes start (and the interval
    /// between probes).
    pub keepalive_interval: Duration,
    /// Timeout for establishing a new connection.
    pub connect_timeout: Duration,
    /// Timeout applied to each individual operation (request + response).
    pub op_timeout: Duration,
    /// Maximum time to wait for a pooled connection to become available.
    pub pool_wait_timeout: Duration,
    /// Disable Nagle's algorithm on TCP connections.
    pub tcp_nodelay: bool,
    /// Reject keys longer than 250 bytes (memcached's hard limit).
    ///
    /// When `false` the key is passed through unchecked, matching the mesh
    /// deployment where the sidecar performs any needed rewriting.
    pub validate_keys: bool,
}

/// How to rediscover the mesh endpoint: rescan the socks registry in `dir`
/// for the entry matching `group`/`namespace`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MeshDiscovery {
    /// Directory holding the socks registry files.
    pub dir: PathBuf,
    /// Service group to match (the `+<group>+all:` suffix), if known. `None`
    /// (from [`Config::sock`]) matches only by namespace.
    pub group: Option<String>,
    /// Cache namespace to match.
    pub namespace: String,
}

/// memcached's hard key-length limit, in bytes.
pub const MAX_KEY_LEN: usize = 250;

impl Config {
    /// Build a config for a TCP endpoint with sensible defaults.
    pub fn tcp(host: impl Into<String>, port: u16) -> Self {
        Self::new(Endpoint::Tcp {
            host: host.into(),
            port,
        })
    }

    /// Build a config for a unix domain socket with sensible defaults.
    pub fn unix(path: impl Into<PathBuf>) -> Self {
        Self::new(Endpoint::Unix { path: path.into() })
    }

    /// Build a config by **directly parsing** a mesh socks registry file
    /// (byMesh path). The file name encodes the endpoint, e.g.
    /// `…+<group>+all:<namespace>@mc:<port>@cs`; it is parsed in place with no
    /// remote/vintage fetch. A numeric port yields a TCP endpoint to
    /// `127.0.0.1:<port>`; otherwise a sibling `<token>.sock` unix socket.
    ///
    /// `path` may be the full path to the file or just its name (resolved
    /// against [`mesh::DEFAULT_SOCKS_DIR`]). Defaults to the binary protocol,
    /// matching the mesh `PingPong` client.
    pub fn sock(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let (dir, name) = match (path.parent(), path.file_name()) {
            (Some(parent), Some(name)) if !parent.as_os_str().is_empty() => (parent, name),
            (_, Some(name)) => (Path::new(mesh::DEFAULT_SOCKS_DIR), name),
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
        let endpoint = mesh::endpoint_from_name(dir, name)?;
        let namespace = mesh::namespace_from_name(name).map(str::to_string);
        let mut config = Self::new(endpoint);
        config.mesh_discovery = Some(MeshDiscovery {
            dir: dir.to_path_buf(),
            group: mesh::group_from_name(name).map(str::to_string),
            namespace: namespace.clone().unwrap_or_default(),
        });
        if let Some(namespace) = namespace {
            config.namespace = namespace;
        }
        Ok(config)
    }

    /// Discover the mesh memcached endpoint for `group`/`namespace` by scanning
    /// the default socks directory ([`mesh::DEFAULT_SOCKS_DIR`]). Local file
    /// access only — no remote fetch.
    pub fn mesh(group: &str, namespace: &str) -> Result<Self> {
        Self::mesh_in(mesh::DEFAULT_SOCKS_DIR, group, namespace)
    }

    /// Like [`Config::mesh`] but scans `dir` instead of the default directory.
    pub fn mesh_in(dir: impl AsRef<Path>, group: &str, namespace: &str) -> Result<Self> {
        let dir = dir.as_ref();
        let endpoint = mesh::discover(dir, group, namespace)?;
        let mut config = Self::new(endpoint).with_namespace(namespace);
        config.mesh_discovery = Some(MeshDiscovery {
            dir: dir.to_path_buf(),
            group: Some(group.to_string()),
            namespace: namespace.to_string(),
        });
        Ok(config)
    }

    /// Build a config from an [`Endpoint`] with default pool/timeout settings.
    pub fn new(endpoint: Endpoint) -> Self {
        Config {
            endpoint,
            mesh_discovery: None,
            protocol: Protocol::default(),
            namespace: String::new(),
            max_connections: 128,
            min_connections: 2,
            tcp_keepalive: true,
            keepalive_interval: Duration::from_secs(10),
            connect_timeout: Duration::from_millis(500),
            op_timeout: Duration::from_millis(400),
            pool_wait_timeout: Duration::from_millis(500),
            tcp_nodelay: true,
            validate_keys: true,
        }
    }

    /// Select the wire protocol.
    pub fn with_protocol(mut self, protocol: Protocol) -> Self {
        self.protocol = protocol;
        self
    }

    /// Set the namespace used to identify this client in logs.
    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = namespace.into();
        self
    }

    /// Set the maximum number of pooled connections.
    pub fn with_max_connections(mut self, max: usize) -> Self {
        self.max_connections = max;
        self
    }

    /// Set the minimum pool size: established at startup and maintained
    /// afterwards by the shared background task.
    pub fn with_min_connections(mut self, n: usize) -> Self {
        self.min_connections = n;
        self
    }

    /// Enable or disable TCP keepalive on pooled connections.
    pub fn with_tcp_keepalive(mut self, enabled: bool) -> Self {
        self.tcp_keepalive = enabled;
        self
    }

    /// Set the TCP keepalive idle interval.
    pub fn with_keepalive_interval(mut self, interval: Duration) -> Self {
        self.keepalive_interval = interval;
        self
    }

    /// Set the per-operation timeout.
    pub fn with_op_timeout(mut self, timeout: Duration) -> Self {
        self.op_timeout = timeout;
        self
    }

    /// Set the connection-establishment timeout.
    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Enable or disable key validation.
    pub fn with_validate_keys(mut self, validate: bool) -> Self {
        self.validate_keys = validate;
        self
    }
}
