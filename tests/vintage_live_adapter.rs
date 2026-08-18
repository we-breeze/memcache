#![cfg(all(feature = "service", feature = "direct-mock"))]

//! Integration tests for the Vintage live-value cache-service adapter
//! (`VintageCacheServices`).
//!
//! These use an in-process axum fake Vintage server (the same pattern as the
//! `vintage` crate's `config_watch` tests) plus real memcached containers for
//! the cache backends (the same `MemcachedContainer` pattern as
//! `yaml_integration.rs`).
//!
//! `#[ignore]`-guarded because the backend assertions require Docker with a
//! memcached image. Run with:
//!
//!     cargo test --test vintage_live_adapter -- --ignored
//!
//! Requirements:
//!   - Docker daemon running.
//!   - Image `registry.example.com/example_rd_if/memcached:1.6` available
//!     locally (override with `BRZ_MC_IMAGE`).

use std::collections::HashMap;
use std::process::Command;
use std::sync::{
    Arc,
    atomic::{AtomicU16, AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};

use axum::{
    Json, Router,
    extract::{Query, State},
    response::{IntoResponse, Response},
    routing::get,
};
use bytes::Bytes;
use memcache::service::vintage_live::VintageCacheServiceConfigSource;
use memcache::{CacheService, CacheServiceOptions, Memcache};
use serde_json::json;
use tokio::net::{TcpListener, TcpStream};
use vintage::{Client, ClientConfig, SnapshotConfig};

const COMMITTED_IMAGE: &str = "registry.example.com/example_rd_if/memcached:1.6";
static NEXT_PORT: AtomicU16 = AtomicU16::new(24111);

fn test_image() -> String {
    std::env::var("BRZ_MC_IMAGE").unwrap_or_else(|_| COMMITTED_IMAGE.to_string())
}

fn unique_port() -> u16 {
    NEXT_PORT.fetch_add(1, Ordering::Relaxed)
}

struct MemcachedContainer {
    name: String,
    port: u16,
}

impl MemcachedContainer {
    async fn start(test_id: &str) -> Self {
        let port = unique_port();
        let name = format!("brz-mcs-vlive-{test_id}-{port}");
        let image = test_image();
        let _ = Command::new("docker").args(["rm", "-f", &name]).output();
        // Use host networking with memcached's own `-p`/`-l 127.0.0.1` flags so
        // the container listens on the host's loopback port directly. (On this
        // host, `-p 127.0.0.1:<port>:11211` port publishing does not bind on
        // the host — host networking + `-p` is the working pattern, mirroring
        // `service_yaml_local.rs`.)
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
                "-U",
                "0",
                "-m",
                "64",
            ])
            .output()
            .expect("failed to invoke `docker run`");
        assert!(
            out.status.success(),
            "docker run failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            if TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
                break;
            }
            assert!(Instant::now() < deadline, "memcached on {port} not ready");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        MemcachedContainer { name, port }
    }

    fn endpoint(&self) -> String {
        format!("127.0.0.1:{}", self.port)
    }
}

impl Drop for MemcachedContainer {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "-f", &self.name])
            .output();
    }
}

/// In-process fake Vintage statics-config server. Holds a mutable YAML body
/// keyed by group, with an atomic revision whose sign is `sign-{rev}`.
#[derive(Default)]
struct FakeVintage {
    revision: AtomicUsize,
    body: std::sync::Mutex<String>,
}

impl FakeVintage {
    fn new(body: String) -> Self {
        Self {
            revision: AtomicUsize::new(0),
            body: std::sync::Mutex::new(body),
        }
    }

    fn set_body(&self, body: String) {
        self.body.lock().unwrap().clone_from(&body);
        self.revision.fetch_add(1, Ordering::SeqCst);
    }

    fn sign(&self) -> String {
        format!("sign-{}", self.revision.load(Ordering::SeqCst))
    }
}

async fn config_handler(
    State(state): State<Arc<FakeVintage>>,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    let action = query.get("action").map(String::as_str).unwrap_or("");
    let group = query.get("group").map(String::as_str).unwrap_or("");
    match action {
        "lookup" => {
            let body = state.body.lock().unwrap().clone();
            Json(json!({
                "code": "200",
                "body": {
                    "groupId": group,
                    "sign": state.sign(),
                    "nodes": [{"key": "all", "value": body}]
                }
            }))
            .into_response()
        }
        "getsign" => Json(json!({"code": "200", "body": {"sign": state.sign()}})).into_response(),
        _ => Json(json!({"code": "400", "body": {"error": "unknown action"}})).into_response(),
    }
}

async fn spawn_vintage(body: String) -> (String, Arc<FakeVintage>, tokio::task::JoinHandle<()>) {
    let state = Arc::new(FakeVintage::new(body));
    let app = Router::new()
        .route("/1/config/service", get(config_handler))
        .with_state(state.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (format!("http://{address}"), state, server)
}

fn vintage_client(endpoint: String) -> Client {
    let config = ClientConfig::new(endpoint, SnapshotConfig::disabled())
        .unwrap()
        .with_refresh_interval(Duration::from_millis(20))
        .unwrap();
    Client::new(config).unwrap()
}

fn group_yaml(ns_a_master: &str, ns_b_master: &str) -> String {
    format!(
        "ns-a:\n  hash: crc32\n  distribution: modula\n  master:\n  - {a}\nns-b:\n  hash: crc32\n  distribution: modula\n  master:\n  - {b}\n",
        a = ns_a_master,
        b = ns_b_master
    )
}

#[tokio::test]
#[ignore = "requires Docker with a memcached image"]
async fn subscribe_builds_cache_from_namespace() {
    let mc = MemcachedContainer::start("build").await;
    let body = group_yaml(&mc.endpoint(), &mc.endpoint());
    let (endpoint, _state, server) = spawn_vintage(body).await;
    let client = vintage_client(endpoint);
    let source = VintageCacheServiceConfigSource::new(client, "g", "ns-a");

    let cache: CacheService =
        CacheService::new_live(std::sync::Arc::new(source), CacheServiceOptions::default())
            .await
            .unwrap();
    assert!(cache.set("k", Bytes::from_static(b"v")).await.unwrap());
    let got = cache.get("k").await.unwrap().unwrap();
    assert_eq!(got.data, Bytes::from_static(b"v"));

    drop(cache);
    server.abort();
    let _ = server.await;
}

#[tokio::test]
#[ignore = "requires Docker with a memcached image"]
async fn namespace_change_hot_swaps_backend() {
    let mc1 = MemcachedContainer::start("swap1").await;
    let mc2 = MemcachedContainer::start("swap2").await;
    let body = group_yaml(&mc1.endpoint(), &mc1.endpoint());
    let (endpoint, state, server) = spawn_vintage(body).await;
    let client = vintage_client(endpoint);
    let source = VintageCacheServiceConfigSource::new(client, "g", "ns-a");

    let cache: CacheService =
        CacheService::new_live(std::sync::Arc::new(source), CacheServiceOptions::default())
            .await
            .unwrap();
    cache.set("k", Bytes::from_static(b"v")).await.unwrap();
    assert_eq!(
        cache.get("k").await.unwrap().unwrap().data,
        Bytes::from_static(b"v")
    );

    // Mutate only ns-a's master to mc2; ns-b unchanged.
    state.set_body(group_yaml(&mc2.endpoint(), &mc1.endpoint()));
    // Wait for the poll to apply the new config (refresh 20ms).
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        // mc2 should NOT have the key (the old value lived in mc1); a get must
        // miss once the backend swapped to mc2.
        if cache.get("k").await.unwrap().is_none() {
            break;
        }
        assert!(Instant::now() < deadline, "backend did not hot-swap to mc2");
        tokio::time::sleep(Duration::from_millis(30)).await;
    }

    drop(cache);
    server.abort();
    let _ = server.await;
}

#[tokio::test]
#[ignore = "requires Docker with a memcached image"]
async fn namespace_yaml_error_keeps_old_backend() {
    let mc = MemcachedContainer::start("ymlerr").await;
    let body = group_yaml(&mc.endpoint(), &mc.endpoint());
    let (endpoint, state, server) = spawn_vintage(body).await;
    let client = vintage_client(endpoint);
    let source = VintageCacheServiceConfigSource::new(client, "g", "ns-a");

    let cache: CacheService =
        CacheService::new_live(std::sync::Arc::new(source), CacheServiceOptions::default())
            .await
            .unwrap();
    cache.set("k", Bytes::from_static(b"v")).await.unwrap();

    // Push a malformed ns-a block (invalid YAML for that namespace). The group
    // body itself is still splittable, but ns-a's YAML fails to parse → the old
    // backend is retained.
    state.set_body(format!(
        "ns-a:\n  master: [unterminated\nns-b:\n  master:\n  - {b}\n",
        b = mc.endpoint()
    ));
    tokio::time::sleep(Duration::from_millis(200)).await;
    // Old backend still serves the value.
    assert_eq!(
        cache.get("k").await.unwrap().unwrap().data,
        Bytes::from_static(b"v")
    );

    drop(cache);
    server.abort();
    let _ = server.await;
}

#[tokio::test]
#[ignore = "requires Docker with a memcached image"]
async fn drop_cache_service_evicts_group_poll() {
    let mc = MemcachedContainer::start("evict").await;
    let body = group_yaml(&mc.endpoint(), &mc.endpoint());
    let (endpoint, state, server) = spawn_vintage(body).await;
    let client = vintage_client(endpoint.clone());
    let source = VintageCacheServiceConfigSource::new(client, "g", "ns-a");

    let cache: CacheService =
        CacheService::new_live(std::sync::Arc::new(source), CacheServiceOptions::default())
            .await
            .unwrap();
    drop(cache);
    // Dropping the last CacheService should evict the group's Live, stopping
    // the poll. Re-subscribe (new source) works.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let source2 = VintageCacheServiceConfigSource::new(vintage_client(endpoint), "g", "ns-a");
    let cache2: CacheService =
        CacheService::new_live(std::sync::Arc::new(source2), CacheServiceOptions::default())
            .await
            .unwrap();
    cache2.set("k2", Bytes::from_static(b"v2")).await.unwrap();

    drop(cache2);
    server.abort();
    let _ = server.await;
    let _ = state;
}
