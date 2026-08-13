use std::time::Duration;

pub use brz_discovery::Endpoint;

use crate::sidecar::discovery::MeshDiscovery;

/// Wire protocol spoken to the memcached endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Protocol {
    /// Classic ASCII/text protocol (`cn.vika.memcached` compatible).
    Text,
    /// Binary protocol (`com.schooner.MemCached` compatible).
    #[default]
    Binary,
}

/// Client configuration.
///
/// Mirrors the settings applied by `PingPongMemcachedBinaryClient` /
/// `TextMemcacheClient` in breeze-sdk-core: a single endpoint, a bounded
/// connection pool, TCP_NODELAY, and socket-level timeouts.
///
/// This is the low-level, single-endpoint engine configuration shared by
/// both access modes; applications usually build one via
/// [`crate::sidecar::MeshConfig`] (mesh discovery) or
/// [`crate::direct::ServerConfig`] (direct backend).
#[derive(Debug, Clone)]
pub struct Config {
    /// Endpoint to connect to.
    pub endpoint: Endpoint,
    /// Mesh rediscovery coordinates, set when the endpoint was discovered via
    /// the socks registry (see [`crate::sidecar`]). When present, the client
    /// periodically rescans the registry and follows endpoint changes (e.g.
    /// a mesh port reassignment); pooled connections to the old endpoint are
    /// drained on change.
    pub(crate) mesh_discovery: Option<MeshDiscovery>,
    /// Protocol to speak.
    pub protocol: Protocol,
    /// Namespace used to identify this client in logs (matches the mesh
    /// `namespace`). Empty by default; set by [`Config::with_namespace`] (or
    /// [`crate::sidecar::MeshConfig`], which sets it automatically).
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
    /// Enable TCP keepalive on pooled connections.
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

/// memcached's hard key-length limit, in bytes.
pub const MAX_KEY_LEN: usize = 250;

impl Config {
    /// Build a config for a TCP endpoint with sensible defaults.
    pub fn tcp(host: impl Into<String>, port: u16) -> Self {
        Self::new(Endpoint {
            host: host.into(),
            port,
        })
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
