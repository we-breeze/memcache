#![cfg(feature = "direct-tcp")]
// Integration tests for the direct-TCP `memcache` client against a real
// memcached server.
//
// These tests require Docker with the memcached:1.6 image available. They are
// marked `#[ignore]` so they do NOT run in normal `cargo test`. Run them with:
//
//     cargo test --test direct_integration --features direct-tcp -- --ignored
//
// Each test spins up its own memcached container on a unique port and
// best-effort removes it on completion. The committed image constant points at
// the example registry image; for local iteration the same tests pass against
// `memcached:1.6-alpine` because the tests only depend on host:port reachability.
//
// Container networking note: this host's Docker daemon has no `bridge` network
// (only `host`/`none`/`trp-record-local`), so `-p host:container` port
// publishing does not forward. The tests therefore start memcached with
// `--network host` and pass `-p <port> -l 127.0.0.1` as memcached arguments so
// each container binds a unique loopback port directly on the host.

use memcache::{MemcacheError, MemcachePool, new_compat_hash, text_get};
use std::process::Command;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Committed image reference. Tests only connect to host:port, so the image
/// choice only affects container startup. For local iteration substitute
/// `memcached:1.6-alpine` via the `BRZ_MC_IMAGE` env var (read once at start).
const COMMITTED_IMAGE: &str = "registry.example.com/example_rd_if/memcached:1.6";

/// Image actually used for `docker run`. Defaults to the committed image; can
/// be overridden with `BRZ_MC_IMAGE` for local development against the alpine
/// variant while keeping the committed constant intact.
fn test_image() -> String {
    std::env::var("BRZ_MC_IMAGE").unwrap_or_else(|_| COMMITTED_IMAGE.to_string())
}

/// Monotonic port base so concurrent/sequential tests get unique loopback
/// ports. memcached listens on TCP only; these are well above 1024 and below
/// the ephemeral range to avoid clashing with outbound connections.
static NEXT_PORT: AtomicU16 = AtomicU16::new(21111);

fn unique_port() -> u16 {
    NEXT_PORT.fetch_add(1, Ordering::Relaxed)
}

/// A running memcached container bound to `port` on the loopback interface.
/// `Drop` performs a best-effort `docker rm -f`.
struct MemcachedContainer {
    name: String,
    port: u16,
}

impl MemcachedContainer {
    /// Start a memcached container on `127.0.0.1:<port>` via `--network host`
    /// plus memcached `-p <port> -l 127.0.0.1` args, and poll the port until it
    /// accepts TCP (up to ~10s). Panics on failure; test-infra level errors
    /// are fatal here.
    async fn start(test_id: &str) -> Self {
        let port = unique_port();
        let name = format!("brz-mc-it-{test_id}-{port}");
        let image = test_image();

        // Remove any stale container with the same name, then start fresh.
        // `--network host` is used because this host's Docker daemon has no
        // `bridge` network, so `-p host:container` publishing does not forward.
        // We pass memcached its own `-p <port> -l 127.0.0.1` flags so it binds
        // a unique loopback port directly on the host.
        let _ = Command::new("docker").args(["rm", "-f", &name]).output();

        let out = Command::new("docker")
            .args([
                "run",
                "--rm",
                "-d",
                "--name",
                &name,
                "--network",
                "host",
                &image,
                "-p",
                &port.to_string(),
                "-l",
                "127.0.0.1",
            ])
            .output();
        let out = match out {
            Ok(o) => o,
            Err(e) => panic!("failed to invoke `docker run`: {e}"),
        };
        if !out.status.success() {
            panic!(
                "`docker run` failed (status {:?}): stdout={:?} stderr={:?}",
                out.status.code(),
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr),
            );
        }

        // Poll the port for TCP acceptance (~10s budget).
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            match tokio::time::timeout(Duration::from_millis(250), async {
                TcpStream::connect(("127.0.0.1", port)).await
            })
            .await
            {
                Ok(Ok(_)) => break,
                _ => {
                    if Instant::now() >= deadline {
                        // Best-effort cleanup before failing.
                        let _ = Command::new("docker").args(["rm", "-f", &name]).output();
                        panic!(
                            "memcached container {name} did not become reachable on port {port} within 10s"
                        );
                    }
                }
            }
        }

        MemcachedContainer { name, port }
    }
}

impl Drop for MemcachedContainer {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "-f", &self.name])
            .output();
    }
}

/// Send a raw memcached text-protocol `set` command and await the `STORED`
/// reply. Used to seed data because this crate intentionally has no `set`
/// helper. The value bytes are written verbatim (binary-safe).
async fn raw_set(
    host: &str,
    port: u16,
    key: &str,
    flags: u32,
    exptime: u32,
    value: &[u8],
) -> std::io::Result<()> {
    let addr = format!("{host}:{port}");
    let mut stream = TcpStream::connect(&addr).await?;

    // Header is ASCII; value is raw bytes.
    let header = format!(
        "set {key} {flags} {exptime} {bytes}\r\n",
        bytes = value.len()
    );
    stream.write_all(header.as_bytes()).await?;
    stream.write_all(value).await?;
    stream.write_all(b"\r\n").await?;
    stream.flush().await?;

    // Read until we see the STORED\r\n reply.
    let mut buf = Vec::with_capacity(64);
    let mut chunk = [0u8; 64];
    loop {
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);
        if buf.ends_with(b"STORED\r\n") {
            break;
        }
        if buf.ends_with(b"NOT_STORED\r\n") || buf.ends_with(b"SERVER_ERROR") {
            return Err(std::io::Error::other(format!(
                "memcached rejected set: {:?}",
                String::from_utf8_lossy(&buf)
            )));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Docker-backed integration tests (ignored by default).
// ---------------------------------------------------------------------------

/// `text_get` returns the stored value with correct flags for a key set via raw
/// `set`. Uses a binary-safe value (non-ASCII bytes) and asserts exact bytes +
/// flags round-trip.
#[tokio::test]
#[ignore]
async fn text_get_returns_stored_value_with_flags() {
    let mc = MemcachedContainer::start("get_value").await;

    let key = "u6604513523";
    let flags: u32 = 4096;
    // Mix of ASCII and non-ASCII bytes, including a 0xFF, a 0x00, and bytes
    // that would collide with `\r\n` sequences to exercise binary handling.
    let value: Vec<u8> = {
        let mut v = Vec::new();
        v.extend_from_slice(b"hello ");
        v.extend_from_slice(&[0xff, 0x00, 0x0d, 0x0a, 0x80, 0x7f]);
        v.extend_from_slice(b" world");
        v
    };

    raw_set("127.0.0.1", mc.port, key, flags, 0, &value)
        .await
        .expect("raw set succeeds");

    let got = text_get("127.0.0.1", mc.port, key)
        .await
        .expect("text_get should succeed");
    assert_eq!(got.key, key);
    assert_eq!(got.flags, flags);
    assert_eq!(got.value, value, "value bytes must round-trip exactly");
}

/// `text_get` on a missing key returns `MemcacheError::NotFound`.
#[tokio::test]
#[ignore]
async fn text_get_missing_key_returns_not_found() {
    let mc = MemcachedContainer::start("missing").await;

    let key = "definitely-not-present-8675309";
    let err = text_get("127.0.0.1", mc.port, key)
        .await
        .expect_err("missing key should error");
    match err {
        MemcacheError::NotFound(k) => {
            assert_eq!(k, key, "NotFound should carry the requested key");
        }
        other => panic!("expected NotFound, got {other:?}"),
    }
}

/// An empty value (0 bytes) round-trips correctly.
#[tokio::test]
#[ignore]
async fn text_get_empty_value_round_trips() {
    let mc = MemcachedContainer::start("empty").await;

    let key = "empty-value-key";
    let flags: u32 = 7;
    let value: Vec<u8> = Vec::new();

    raw_set("127.0.0.1", mc.port, key, flags, 0, &value)
        .await
        .expect("raw set of empty value succeeds");

    let got = text_get("127.0.0.1", mc.port, key)
        .await
        .expect("text_get on empty value should succeed");
    assert_eq!(got.key, key);
    assert_eq!(got.flags, flags);
    assert!(got.value.is_empty(), "value must be empty");
}

/// A large-ish value (64KB) round-trips correctly, exercising the read loop
/// (the client's read buffer is 8KB, so this spans multiple reads).
#[tokio::test]
#[ignore]
async fn text_get_large_value_round_trips() {
    let mc = MemcachedContainer::start("large").await;

    let key = "large-value-key";
    let flags: u32 = 0;
    // 64 * 1024 bytes with a deterministic pattern so we can verify integrity.
    let value: Vec<u8> = (0..(64 * 1024)).map(|i| (i & 0xff) as u8).collect();

    raw_set("127.0.0.1", mc.port, key, flags, 0, &value)
        .await
        .expect("raw set of large value succeeds");

    let got = text_get("127.0.0.1", mc.port, key)
        .await
        .expect("text_get on large value should succeed");
    assert_eq!(got.key, key);
    assert_eq!(got.flags, flags);
    assert_eq!(got.value.len(), value.len());
    assert_eq!(got.value, value, "64KB value must round-trip exactly");
}

// ---------------------------------------------------------------------------
// Non-ignored pure-logic tests. The pool already has unit tests in src; these
// cover the re-exported `MemcachePool` and `new_compat_hash` symbols from the
// integration-test perspective and are cheap to keep.
// ---------------------------------------------------------------------------

/// `select_endpoint` is deterministic for a fixed key/pool and matches the
/// documented `new_compat_hash(key) % len` formula.
#[test]
fn select_endpoint_is_deterministic_and_matches_formula() {
    let pool = MemcachePool::from_masters([
        "vintage-node-10-185-32-132.trp.test:15138".to_string(),
        "vintage-node-10-2-29-229.trp.test:15138".to_string(),
        "vintage-node-10-2-37-234.trp.test:15138".to_string(),
        "vintage-node-10-30-214-36.trp.test:15138".to_string(),
    ]);
    assert_eq!(pool.len(), 4);
    assert!(!pool.is_empty());

    for key in ["u123", "u6604513523", "some-other-key", ""] {
        let ep = pool.select_endpoint(key).expect("non-empty pool");
        let again = pool.select_endpoint(key).expect("deterministic");
        assert_eq!(
            ep, again,
            "select_endpoint must be deterministic for {key:?}"
        );

        let h = new_compat_hash(key);
        let expected_idx = (h % pool.len() as u64) as usize;
        let expected = pool.masters()[expected_idx].as_str();
        assert_eq!(ep, expected, "index mismatch for key {key:?}");
    }
}

/// `from_masters` with an explicit single-node list routes every key there.
#[test]
fn from_masters_single_node_routes_all_keys() {
    let pool = MemcachePool::from_masters(["only-node:11211".to_string()]);
    assert_eq!(pool.len(), 1);
    assert_eq!(pool.select_endpoint("any-key").unwrap(), "only-node:11211");
}
