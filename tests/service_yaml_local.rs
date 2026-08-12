#![cfg(feature = "service")]

//! Local (no-Docker) integration tests: the committed test YAML
//! `fixtures/cache_service_local.yaml` drives the **production service
//! path** — `CacheServiceConfig` parses the document,
//! `MemCacheTemplate::from_namespace_conf` builds the multi-tier template —
//! against four plain memcached instances on 127.0.0.1:21311-21314.
//!
//! Instances already listening are reused as-is; otherwise the test starts
//! them and removes them on completion (mirroring
//! `tools/mc-bench/bench_local.sh`'s `service-yaml` layout).

use std::process::Command;
use std::time::{Duration, Instant};

use memcache::cacheservice::CacheServiceConfig;
use memcache::direct::{DirectClient, ServerConfig};
use memcache::service::{Cacheable, MemCacheTemplate, PoolOptions};
use memcache::value::{ToMemcacheValue, Value};

const YAML: &str = include_str!("fixtures/cache_service_local.yaml");

/// master / master_l1 / slave / slave_l1 (matches the fixture).
const PORTS: [u16; 4] = [21311, 21312, 21313, 21314];
const MASTER: u16 = 21311;
const MASTER_L1: u16 = 21312;
const SLAVE: u16 = 21313;
const SLAVE_L1: u16 = 21314;

/// Ensures all four memcached instances are up; instances started here are
/// killed on drop.
struct LocalMemcached {
    started: Vec<u16>,
}

impl LocalMemcached {
    fn ensure() -> Self {
        let mut started = Vec::new();
        for port in PORTS {
            if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
                continue;
            }
            let pid = format!("/tmp/mc-svc-yaml-test-{port}.pid");
            let status = Command::new("memcached")
                .args([
                    "-p",
                    &port.to_string(),
                    "-l",
                    "127.0.0.1",
                    "-U",
                    "0",
                    "-m",
                    "1024",
                    "-d",
                    "-P",
                    &pid,
                ])
                .status()
                .expect("failed to start memcached");
            assert!(status.success(), "memcached -p {port} failed to start");
            started.push(port);
        }
        let deadline = Instant::now() + Duration::from_secs(5);
        for port in PORTS {
            while std::net::TcpStream::connect(("127.0.0.1", port)).is_err() {
                assert!(Instant::now() < deadline, "memcached on {port} not ready");
                std::thread::sleep(Duration::from_millis(50));
            }
        }
        LocalMemcached { started }
    }
}

impl Drop for LocalMemcached {
    fn drop(&mut self) {
        for port in &self.started {
            if let Ok(pid) = std::fs::read_to_string(format!("/tmp/mc-svc-yaml-test-{port}.pid")) {
                let _ = Command::new("kill").arg(pid.trim()).status();
            }
            let _ = std::fs::remove_file(format!("/tmp/mc-svc-yaml-test-{port}.pid"));
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
    let _mc = LocalMemcached::ensure();
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
