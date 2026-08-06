use std::io;

/// Result alias used throughout the crate.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors returned by the memcached client.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// Failed to establish the underlying connection.
    #[error("connect failed: {0}")]
    Connect(#[source] io::Error),

    /// Transport-level I/O error on an established connection.
    #[error("io error: {0}")]
    Io(#[source] io::Error),

    /// The pool could not hand out a connection (exhausted, timed out, ...).
    #[error("connection pool error: {0}")]
    Pool(String),

    /// Could not locate a mesh endpoint in the socks registry directory.
    #[error("mesh discovery: {0}")]
    MeshDiscovery(String),

    /// An operation exceeded the configured timeout.
    #[error("operation timed out")]
    Timeout,

    /// The server reported a `SERVER_ERROR` (text) or a server-side status.
    #[error("server error: {0}")]
    Server(String),

    /// The server reported a `CLIENT_ERROR` / invalid-argument style failure.
    #[error("client error: {0}")]
    Client(String),

    /// The response could not be parsed / violated the protocol.
    #[error("protocol error: {0}")]
    Protocol(String),

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

impl<E: std::fmt::Display> From<deadpool::managed::PoolError<E>> for Error {
    fn from(err: deadpool::managed::PoolError<E>) -> Self {
        match err {
            deadpool::managed::PoolError::Backend(err) => Error::Pool(err.to_string()),
            deadpool::managed::PoolError::Timeout(kind) => {
                Error::Pool(format!("timeout ({kind:?})"))
            }
            other => Error::Pool(other.to_string()),
        }
    }
}
