//! Direct-TCP text-protocol client + crc32/modula node-selection pool for the
//! `example-abtest` user-info cache.
//!
//! This is the direct-TCP path used under replay/comparison topologies where
//! the target connects directly to the recorded memcached nodes (e.g.
//! `vintage-node-*.trp.test:15138`) — it is NOT the mesh sidecar path. It
//! reproduces the source service's `SockIOPool.NEW_COMPAT_HASH` (crc32 +
//! modula) node selection so the replay proxy can match recorded memcached
//! exchanges by (target host, key).

use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const READ_BUFFER_BYTES: usize = 8 * 1024;
const MAX_VALUE_BYTES: usize = 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum MemcacheError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("value not found for key {0:?}")]
    NotFound(String),
    #[error("server error: {0}")]
    Server(String),
    #[error("value too large: {0} bytes")]
    TooLarge(usize),
    #[error("malformed response")]
    Malformed,
}

/// A single-entry memcached get result.
#[derive(Debug)]
pub struct MemcacheGet {
    pub key: String,
    pub flags: u32,
    pub value: Vec<u8>,
}

/// Fetch a key from memcached using the text protocol.
///
/// Connects to `host:port`, sends `get <key>\r\n`, and parses the VALUE/END
/// response. The connection is closed after each call (no pooling) to match
/// the simplest replay-compatible topology.
pub async fn text_get(host: &str, port: u16, key: &str) -> Result<MemcacheGet, MemcacheError> {
    let deadline = Duration::from_secs(5);
    let addr = format!("{host}:{port}");
    let mut stream = tokio::time::timeout(deadline, TcpStream::connect(&addr))
        .await
        .map_err(|_| {
            MemcacheError::Io(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("connect timeout to {addr}"),
            ))
        })??;
    let _ = stream.set_nodelay(true);

    let request = format!("get {key}\r\n");
    tokio::time::timeout(deadline, stream.write_all(request.as_bytes()))
        .await
        .map_err(|_| {
            MemcacheError::Io(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "write timeout",
            ))
        })??;

    let mut buf = Vec::with_capacity(READ_BUFFER_BYTES);
    loop {
        let mut chunk = [0u8; READ_BUFFER_BYTES];
        let n = tokio::time::timeout(deadline, stream.read(&mut chunk))
            .await
            .map_err(|_| {
                MemcacheError::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "read timeout",
                ))
            })??;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);
        if buf.len() > MAX_VALUE_BYTES + 256 {
            return Err(MemcacheError::TooLarge(buf.len()));
        }
        // End of response is marked by "END\r\n".
        if buf.windows(5).any(|w| w == b"\r\nEND") || buf.ends_with(b"END\r\n") {
            break;
        }
    }

    parse_get_response(&buf, key)
}

fn parse_get_response(data: &[u8], requested_key: &str) -> Result<MemcacheGet, MemcacheError> {
    // Expected format:
    // VALUE <key> <flags> <bytes>\r\n
    // <data block>\r\n
    // END\r\n
    //
    // Parse the header line as ASCII (it contains only ASCII characters);
    // the value block may contain arbitrary bytes.
    let crlf = memchr::memmem::find(data, b"\r\n").ok_or(MemcacheError::Malformed)?;
    let header = std::str::from_utf8(&data[..crlf]).map_err(|_| MemcacheError::Malformed)?;
    if header == "END" {
        return Err(MemcacheError::NotFound(requested_key.to_string()));
    }
    let parts: Vec<&str> = header.splitn(4, ' ').collect();
    if parts.len() < 4 || parts[0] != "VALUE" {
        return Err(MemcacheError::Malformed);
    }
    let key = parts[1].to_string();
    let flags: u32 = parts[2].parse().map_err(|_| MemcacheError::Malformed)?;
    let bytes: usize = parts[3].parse().map_err(|_| MemcacheError::Malformed)?;
    if bytes > MAX_VALUE_BYTES {
        return Err(MemcacheError::TooLarge(bytes));
    }

    // The value block starts right after the header line + \r\n.
    let header_end = crlf + 2;
    if data.len() < header_end + bytes + 7 {
        // 7 = \r\n + END\r\n
        return Err(MemcacheError::Malformed);
    }
    let value = data[header_end..header_end + bytes].to_vec();
    Ok(MemcacheGet { key, flags, value })
}

/// The memcached pool used by the `example-abtest` namespace.
///
/// The node list is the master list in source-configured order. Each entry is
/// a `host:port` string where `host` is the vintage-node hostname the replay
/// proxy recognizes.
#[derive(Clone, Debug)]
pub struct MemcachePool {
    /// Master server endpoints in configured order (`host:port`).
    masters: Arc<[String]>,
}

/// Default master list for the `example-abtest` namespace.
///
/// This is the source-proven static configuration parsed from the recorded
/// vintage naming response for `cache.service.feedcontent.pool.yf` /
/// namespace `example-abtest`. It is used when no runtime pool config is
/// supplied (e.g. local smoke tests without a replay proxy). Under replay,
/// the same list is recovered from the recorded naming exchange, so the
/// selected hostname is identical.
const DEFAULT_example_ABTEST_MASTERS: &[&str] = &[
    "vintage-node-10-185-32-132.trp.test:15138",
    "vintage-node-10-2-29-229.trp.test:15138",
    "vintage-node-10-2-37-234.trp.test:15138",
    "vintage-node-10-30-214-36.trp.test:15138",
];

impl MemcachePool {
    /// Build the default `example-abtest` pool from the source-proven static
    /// master list.
    pub fn default_example_abtest() -> Self {
        let masters: Arc<[String]> = DEFAULT_example_ABTEST_MASTERS
            .iter()
            .map(|s| (*s).to_string())
            .collect();
        Self { masters }
    }

    /// Build a pool from an explicit master endpoint list (configured order).
    pub fn from_masters<I>(masters: I) -> Self
    where
        I: IntoIterator<Item = String>,
    {
        let masters: Arc<[String]> = masters.into_iter().collect();
        Self { masters }
    }

    /// Number of master nodes in the pool.
    pub fn len(&self) -> usize {
        self.masters.len()
    }

    /// Returns true if the pool has no master nodes.
    pub fn is_empty(&self) -> bool {
        self.masters.is_empty()
    }

    /// Select the master endpoint (`host:port`) for a given cache key using
    /// the source's `crc32` + `modula` algorithm.
    ///
    /// Returns `None` if the pool is empty.
    pub fn select_endpoint(&self, key: &str) -> Option<&str> {
        if self.masters.is_empty() {
            return None;
        }
        let h = new_compat_hash(key);
        let bucket = (h % self.masters.len() as u64) as usize;
        self.masters.get(bucket).map(|s| s.as_str())
    }

    /// Iterate over all master endpoints in configured order.
    pub fn masters(&self) -> &[String] {
        &self.masters
    }
}

/// `SockIOPool.newCompatHashingAlg`: CRC32 of the key bytes, then
/// `(crc >> 16) & 0x7fff`.
///
/// This mirrors `java.util.zip.CRC32` over the key's UTF-8 bytes (Java's
/// `String.getBytes()` uses the platform default charset, which is UTF-8 for
/// this service).
pub fn new_compat_hash(key: &str) -> u64 {
    let crc = crc32(key.as_bytes());
    (crc >> 16) & 0x7fff
}

/// IEEE 802.3 CRC32 (same polynomial as `java.util.zip.CRC32`).
fn crc32(data: &[u8]) -> u64 {
    let mut crc: u32 = 0xffff_ffff;
    for &b in data {
        crc ^= b as u32;
        for _ in 0..8 {
            if (crc & 1) != 0 {
                crc = (crc >> 1) ^ 0xedb8_8320;
            } else {
                crc >>= 1;
            }
        }
    }
    (!crc) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_recorded_value_response() {
        // Recorded response for u6604513523.
        let raw = b"VALUE u6604513523 4096 79\r\n\x08\x11\x10\x01\x18\xb6\xc1\x32\x28\xb6\xc1\x32\x30\x33\x38\x06\x40\x06\x48\x06\x50\x18\x60\x03\x68\x02\x70\xf0\xbd\xc9\xea\xca\x2c\x78\x11\x90\x03\xe0\x86\xcd\xd3\x06\x98\x03\x02\xa0\x03\xad\xf0\xf0\xfd\x02\xb8\x03\x02\xc0\x03\x03\xd0\x03\xd5\x96\xcf\xe7\x0c\xf0\x03\xd4\x01\xf8\x03\xff\xff\xff\xff\x0f\x80\x04\x01\r\nEND\r\n";
        let result = parse_get_response(raw, "u6604513523").expect("parse");
        assert_eq!(result.key, "u6604513523");
        assert_eq!(result.flags, 4096);
        assert_eq!(result.value.len(), 79);
        assert_eq!(result.value[0], 0x08);
    }

    #[test]
    fn returns_not_found_for_end_only() {
        let raw = b"END\r\n";
        let err = parse_get_response(raw, "u123").unwrap_err();
        assert!(matches!(err, MemcacheError::NotFound(_)));
    }

    /// Recorded uid -> selected memcached node hostname, derived from the
    /// source recording's per-case memcached `get u<uid>` exchanges. The
    /// source hashes `u<uid>` (key format `u%d` from `UserInfoServiceImpl
    /// .cacheKeyFormat`) across the 4 master nodes.
    #[test]
    fn selects_source_proven_node_for_recorded_uids() {
        let pool = MemcachePool::default_example_abtest();
        // (uid, expected vintage-node hostname) — taken from the recorded
        // case-scoped memcached exchanges for `get-config`/`get-config.json`.
        let cases: &[(i64, &str)] = &[
            (7458238635, "vintage-node-10-30-214-36.trp.test:15138"),
            (3731815144, "vintage-node-10-2-37-234.trp.test:15138"),
            (4059862692, "vintage-node-10-2-37-234.trp.test:15138"),
            (9013925778, "vintage-node-10-2-29-229.trp.test:15138"),
            (5017514641, "vintage-node-10-30-214-36.trp.test:15138"),
            (5968415126, "vintage-node-10-185-32-132.trp.test:15138"),
            (7924497925, "vintage-node-10-185-32-132.trp.test:15138"),
            (8001472291, "vintage-node-10-30-214-36.trp.test:15138"),
            (7191533806, "vintage-node-10-2-29-229.trp.test:15138"),
            (6616805625, "vintage-node-10-185-32-132.trp.test:15138"),
            (5632498454, "vintage-node-10-30-214-36.trp.test:15138"),
        ];
        for (uid, expected) in cases {
            let key = format!("u{uid}");
            let got = pool
                .select_endpoint(&key)
                .expect("non-empty pool selects a node");
            assert_eq!(
                got, *expected,
                "uid {uid}: key {key} selected {got}, expected {expected}"
            );
        }
    }

    #[test]
    fn empty_pool_selects_none() {
        let pool = MemcachePool::from_masters(std::iter::empty());
        assert!(pool.is_empty());
        assert!(pool.select_endpoint("u123").is_none());
    }

    #[test]
    fn crc32_matches_java_zip_crc32_for_ascii() {
        // java.util.zip.CRC32 of "hello" is 0x3610a686.
        assert_eq!(crc32(b"hello"), 0x3610a686);
        // java.util.zip.CRC32 of "" is 0.
        assert_eq!(crc32(b""), 0);
    }
}
