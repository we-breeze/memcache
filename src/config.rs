use std::path::PathBuf;
use std::time::Duration;

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
    /// Protocol to speak.
    pub protocol: Protocol,
    /// Maximum number of pooled connections.
    pub max_connections: usize,
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
        Self::new(Endpoint::Tcp {
            host: host.into(),
            port,
        })
    }

    /// Build a config for a unix domain socket with sensible defaults.
    pub fn unix(path: impl Into<PathBuf>) -> Self {
        Self::new(Endpoint::Unix { path: path.into() })
    }

    /// Build a config from an [`Endpoint`] with default pool/timeout settings.
    pub fn new(endpoint: Endpoint) -> Self {
        Config {
            endpoint,
            protocol: Protocol::default(),
            max_connections: 64,
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

    /// Set the maximum number of pooled connections.
    pub fn with_max_connections(mut self, max: usize) -> Self {
        self.max_connections = max;
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
