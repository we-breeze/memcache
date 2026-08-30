use std::io;

/// Result alias used throughout the crate.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors returned by the memcached client.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The cache-service YAML or selected namespace was invalid.
    #[error(transparent)]
    CacheServiceConfig(#[from] crate::cacheservice::CacheServiceError),

    /// Transport-level I/O error on an established connection.
    #[error("io error: {0}")]
    Io(#[source] io::Error),

    /// Could not locate a mesh endpoint in the socks registry directory.
    #[error("mesh discovery: {0}")]
    MeshDiscovery(String),

    /// An operation exceeded the configured timeout.
    #[error("operation timed out")]
    Timeout,

    /// The physical node has not connected yet or is reconnecting.
    #[error("backend connection is unavailable")]
    Unavailable,

    /// The physical node's bounded request queue is full.
    #[error("backend request capacity is exhausted")]
    Overloaded,

    /// The server reported a `SERVER_ERROR` (text) or a server-side status.
    #[error("server error: {0}")]
    Server(String),

    /// The server reported a `CLIENT_ERROR` / invalid-argument style failure.
    #[error("client error: {0}")]
    Client(String),

    /// The response could not be parsed / violated the protocol.
    #[error("protocol error: {0}")]
    Protocol(String),

    /// The connection is out of sync: a response frame did not match the
    /// request that was sent (e.g. a leftover frame from a timed-out
    /// operation). The connection must be dropped, not reused.
    #[error("desynced connection: {0}")]
    Desynced(String),

    /// The key was empty, too long, or contained control/space characters.
    #[error("invalid key: {0}")]
    InvalidKey(&'static str),

    /// A stored value used a feature this client does not decode
    /// (QuickLZ compression or Java object serialization).
    #[error("unsupported value encoding: {0}")]
    Unsupported(&'static str),

    /// A typed decode of a [`crate::Value`] did not match the stored flags.
    #[error("value decode error: {0}")]
    Decode(String),
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Error::Io(err)
    }
}
