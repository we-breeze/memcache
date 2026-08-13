#![cfg(feature = "service")]

//! Docker-backed integration tests: the committed test YAML
//! `fixtures/cache_service_local.yaml` drives the **production service
//! path** — `CacheServiceConfig` parses the document,
//! `MemCacheTemplate::from_namespace_conf` builds the multi-tier template —
//! against four memcached containers on 127.0.0.1:21311-21314.
//!
//! The containers use host networking because the fixture contains fixed
//! loopback ports. They are owned by this test and removed on completion.

use std::process::Command;
use std::time::{Duration, Instant};

use memcache::cacheservice::CacheServiceConfig;
use memcache::direct::{DirectClient, ServerConfig};
use memcache::service::{Cacheable, MemCacheTemplate, PoolOptions};
use memcache::value::{ToMemcacheValue, Value};

const YAML: &str = include_str!("fixtures/cache_service_local.yaml");
const MEMCACHED_IMAGE: &str = "registry.example.com/example_rd_if/memcached:1.6";

/// master / master_l1 / slave / slave_l1 (matches the fixture).
const PORTS: [u16; 4] = [21311, 21312, 21313, 21314];
const MASTER: u16 = 21311;
const MASTER_L1: u16 = 21312;
const SLAVE: u16 = 21313;
const SLAVE_L1: u16 = 21314;

/// Four test-owned memcached containers. Successfully started containers are
/// removed on drop, including while unwinding after a later startup failure.
struct MemcachedContainers {
    names: Vec<String>,
}

impl MemcachedContainers {
    fn start() -> Self {
        let mut containers = Self { names: Vec::new() };
        for port in PORTS {
            assert!(
                std::net::TcpStream::connect(("127.0.0.1", port)).is_err(),
                "service_yaml_local requires exclusive access to 127.0.0.1:{port}"
            );

            let name = format!("brz-mc-service-yaml-{}-{port}", std::process::id());
            let output = Command::new("docker")
                .args([
                    "run",
                    "--rm",
                    "--detach",
                    "--name",
                    &name,
                    "--network",
                    "host",
                    "--label",
                    "breeze.memcache.test=service_yaml_local",
                    MEMCACHED_IMAGE,
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
                .unwrap_or_else(|err| panic!("failed to invoke `docker run`: {err}"));
            assert!(
                output.status.success(),
                "docker failed to start {MEMCACHED_IMAGE} on port {port}: stdout={:?} stderr={:?}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            containers.names.push(name);

            let deadline = Instant::now() + Duration::from_secs(10);
            while std::net::TcpStream::connect(("127.0.0.1", port)).is_err() {
                assert!(
                    Instant::now() < deadline,
                    "memcached container on port {port} was not ready within 10 seconds"
                );
                std::thread::sleep(Duration::from_millis(50));
            }
        }
        containers
    }
}

impl Drop for MemcachedContainers {
    fn drop(&mut self) {
        for name in &self.names {
            let _ = Command::new("docker")
                .args(["rm", "--force", name])
                .output();
        }
    }
}

fn template(namespace: &str) -> MemCacheTemplate {
    let config = CacheServiceConfig::from_yaml_str(YAML).expect("fixture YAML parses");
    let ns = config
        .namespace(namespace)
        .unwrap_or_else(|| panic!("namespace {namespace} in fixture"));
    MemCacheTemplate::from_namespace_conf(ns, PoolOptions::default()).expect("template builds")
}

/// A direct client to one instance, for per-tier assertions.
fn direct(port: u16) -> DirectClient {
    DirectClient::connect(ServerConfig::new(&format!("127.0.0.1:{port}")).unwrap()).unwrap()
}

async fn assert_tier(port: u16, key: &str, expected: Option<&str>) {
    let got = direct(port).get(key).await.unwrap();
    assert_eq!(
        got.map(|v| v.as_string().unwrap()),
        expected.map(str::to_string),
        "tier 127.0.0.1:{port} read of {key}"
    );
}

fn value(s: &str) -> Value {
    s.to_memcache_value()
}

/// The whole suite runs as one test: the four memcached instances are
/// shared, and per-test start/stop would race under parallel test threads.
#[tokio::test]
async fn service_yaml_local_suite() {
    let _memcached = MemcachedContainers::start();
    fans_out_writes().await;
    cascades_reads_and_sets_back().await;
    shards_masters().await;
}

/// The service template builds from the YAML and fans a write out to every
/// tier (master, master_l1, slave, slave_l1).
async fn fans_out_writes() {
    let t = template("test.local");

    assert!(t.set("fanout:k", value("v")).await.unwrap());

    assert_tier(MASTER, "fanout:k", Some("v")).await;
    assert_tier(MASTER_L1, "fanout:k", Some("v")).await;
    assert_tier(SLAVE, "fanout:k", Some("v")).await;
    assert_tier(SLAVE_L1, "fanout:k", Some("v")).await;
}

/// Reads cascade down the tiers and set back the higher ones: a key only on
/// the slave is served from there and written back to master + L1s.
async fn cascades_reads_and_sets_back() {
    let t = template("test.local");

    // Seed the slave tier only (bypassing the template).
    direct(SLAVE)
        .set("cascade:k", value("sv"), 60u32)
        .await
        .unwrap();
    for port in [MASTER, MASTER_L1, SLAVE_L1] {
        direct(port).delete("cascade:k").await.unwrap();
    }

    let got = Cacheable::get(&t, "cascade:k").await.unwrap().unwrap();
    assert_eq!(got.as_string().unwrap(), "sv", "served from the slave tier");

    // The slave hit sets back the master tier (the L1 tiers are only set
    // back when the *master* serves the read, matching the Java template).
    let mut restored = false;
    for _ in 0..100 {
        if direct(MASTER).get("cascade:k").await.unwrap().is_some() {
            restored = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(restored, "slave hit sets back the master tier");

    // A subsequent read goes through the L1/master path and the master hit
    // sets back the consulted L1.
    for _ in 0..6 {
        let got = Cacheable::get(&t, "cascade:k").await.unwrap().unwrap();
        assert_eq!(got.as_string().unwrap(), "sv");
    }
    let mut l1_restored = false;
    for _ in 0..100 {
        if direct(MASTER_L1).get("cascade:k").await.unwrap().is_some() {
            l1_restored = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(l1_restored, "master hit sets back the consulted L1");
}

/// The sharded namespace routes each key to exactly one master shard, and
/// both shards receive keys.
async fn shards_masters() {
    let t = template("test.sharded");

    let keys: Vec<String> = (0..40).map(|i| format!("shard:{i}")).collect();
    for key in &keys {
        assert!(t.set(key, value("x")).await.unwrap());
    }

    let master_a = direct(MASTER);
    let master_b = direct(MASTER_L1); // test.sharded masters: 21311 + 21312
    let mut used = [false, false];
    for key in &keys {
        let on_a = master_a.get(key).await.unwrap().is_some();
        let on_b = master_b.get(key).await.unwrap().is_some();
        assert!(on_a ^ on_b, "{key} must live on exactly one master shard");
        used[on_b as usize] = true;
        // Reads through the template agree with the shard placement.
        assert!(Cacheable::get(&t, key).await.unwrap().is_some());
    }
    assert!(used[0] && used[1], "both master shards should receive keys");
}
