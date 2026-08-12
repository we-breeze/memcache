#![cfg(feature = "service")]

//! YAML-driven end-to-end integration tests against real memcached
//! containers, modeled on the `../redis` and `../memcache` harnesses.
//!
//! The test topology is described by a cache-service YAML document generated
//! from the started containers' ports — the same document shape the Vintage
//! statics-config snapshot carries — then parsed and built exactly like the
//! production path: `CacheServiceConfig` → `MemCacheTemplate::from_namespace_conf`.
//!
//! These tests are `#[ignore]`-guarded because they require Docker with a
//! memcached 1.6 image available. Run them with:
//!
//!     cargo test --test yaml_integration -- --ignored
//!
//! Requirements:
//!   - Docker daemon running.
//!   - Image `registry.example.com/example_rd_if/memcached:1.6` available
//!     locally (override with `BRZ_MC_IMAGE`, e.g. `memcached:1.6-alpine`).
//!
//! Container networking note: containers publish memcached's default 11211
//! port to a unique loopback port (`-p 127.0.0.1:<port>:11211`). (An earlier
//! `--network host` approach does not work on Docker Desktop for Mac, where
//! "host" is the VM, not the macOS host.)

use std::process::Command;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::{Duration, Instant};

use memcache::cacheservice::CacheServiceConfig;
use memcache::direct::{DirectClient, ServerConfig};
use memcache::service::{Cacheable, MemCacheTemplate, MemcacheServiceTemplate, PoolOptions};
use memcache::value::{ToMemcacheValue, Value};
use tokio::net::TcpStream;

/// Committed image reference; override with `BRZ_MC_IMAGE` for local dev.
const COMMITTED_IMAGE: &str = "registry.example.com/example_rd_if/memcached:1.6";

fn test_image() -> String {
    std::env::var("BRZ_MC_IMAGE").unwrap_or_else(|_| COMMITTED_IMAGE.to_string())
}

/// Monotonic port base so tests get unique loopback ports.
static NEXT_PORT: AtomicU16 = AtomicU16::new(23111);

fn unique_port() -> u16 {
    NEXT_PORT.fetch_add(1, Ordering::Relaxed)
}

/// A running memcached container bound to `127.0.0.1:<port>`; `Drop` does a
/// best-effort `docker rm -f`.
struct MemcachedContainer {
    name: String,
    port: u16,
}

impl MemcachedContainer {
    async fn start(test_id: &str) -> Self {
        let port = unique_port();
        let name = format!("brz-mcs-it-{test_id}-{port}");
        let image = test_image();

        let _ = Command::new("docker").args(["rm", "-f", &name]).output();
        let out = Command::new("docker")
            .args([
                "run",
                "--rm",
                "-d",
                "--name",
                &name,
                "-p",
                &format!("127.0.0.1:{port}:11211"),
                &image,
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

    /// A direct client to this one container, for per-tier assertions.
    fn client(&self) -> DirectClient {
        DirectClient::connect(ServerConfig::new(&self.endpoint()).unwrap()).unwrap()
    }
}

impl Drop for MemcachedContainer {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "-f", &self.name])
            .output();
    }
}

/// The test topology: two masters, one slave, one master-L1 group, one
/// slave-L1 group — the smallest shape exercising every tier.
struct Topology {
    masters: Vec<MemcachedContainer>,
    slave: MemcachedContainer,
    master_l1: MemcachedContainer,
    slave_l1: MemcachedContainer,
    namespace_yaml: String,
}

impl Topology {
    async fn start(test_id: &str) -> Self {
        let masters = vec![
            MemcachedContainer::start(test_id).await,
            MemcachedContainer::start(test_id).await,
        ];
        let slave = MemcachedContainer::start(test_id).await;
        let master_l1 = MemcachedContainer::start(test_id).await;
        let slave_l1 = MemcachedContainer::start(test_id).await;

        let list = |endpoints: &[String]| {
            endpoints
                .iter()
                .map(|e| format!("   - {e}\n"))
                .collect::<String>()
        };
        // Inner items of a list-of-lists need deeper indent than the outer
        // dash (matching the production snapshot shape).
        let inner_list = |endpoints: &[String]| {
            endpoints
                .iter()
                .map(|e| format!("     - {e}\n"))
                .collect::<String>()
        };
        let master_eps: Vec<String> = masters.iter().map(|m| m.endpoint()).collect();
        let namespace_yaml = format!(
            "test.ns:\n  hash: crc32\n  distribution: modula\n  master:\n{masters}  master_l1:\n   -\n{ml1}  slave:\n{slave}  slave_l1:\n   -\n{sl1}",
            masters = list(&master_eps),
            ml1 = inner_list(&[master_l1.endpoint()]),
            slave = list(&[slave.endpoint()]),
            sl1 = inner_list(&[slave_l1.endpoint()]),
        );

        Topology {
            masters,
            slave,
            master_l1,
            slave_l1,
            namespace_yaml,
        }
    }

    /// Builds the template exactly like the Vintage backup path does.
    fn template(&self) -> MemCacheTemplate {
        let conf = CacheServiceConfig::from_yaml_str(&self.namespace_yaml).unwrap();
        let ns = conf.namespace("test.ns").unwrap();
        // fully lazy: no warmup connections
        let options = PoolOptions {
            min_connections: Some(0),
            ..PoolOptions::default()
        };
        MemCacheTemplate::from_namespace_conf(ns, options).unwrap()
    }
}

fn value(s: &str) -> Value {
    s.to_memcache_value()
}

async fn assert_get(client: &DirectClient, key: &str, expected: Option<&str>) {
    let got = client.get(key).await.unwrap();
    assert_eq!(
        got.map(|v| v.as_string().unwrap()),
        expected.map(str::to_string),
        "direct read of {key}"
    );
}

#[tokio::test]
#[ignore = "requires Docker with a memcached image"]
async fn set_fans_out_to_every_tier() {
    let topo = Topology::start("fanout").await;
    let t = topo.template();

    assert!(t.set("k1", value("v1")).await.unwrap());

    // Master owns the key (on exactly one shard).
    let master_hits: Vec<_> = {
        let mut hits = Vec::new();
        for m in &topo.masters {
            if let Some(v) = m.client().get("k1").await.unwrap() {
                hits.push(v.as_string().unwrap());
            }
        }
        hits
    };
    assert_eq!(master_hits, vec!["v1".to_string()]);
    // Slave, master-L1 and slave-L1 all received the write.
    assert_get(&topo.slave.client(), "k1", Some("v1")).await;
    assert_get(&topo.master_l1.client(), "k1", Some("v1")).await;
    assert_get(&topo.slave_l1.client(), "k1", Some("v1")).await;
}

#[tokio::test]
#[ignore = "requires Docker with a memcached image"]
async fn get_cascades_and_sets_back_across_tiers() {
    let topo = Topology::start("cascade").await;
    let t = topo.template();

    // Seed the slave only; a read must hit the slave and set back master.
    topo.slave
        .client()
        .set("s-key", value("sv"), 60u32)
        .await
        .unwrap();
    let got = Cacheable::get(&t, "s-key").await.unwrap().unwrap();
    assert_eq!(got.as_string().unwrap(), "sv");
    let master_hits: Vec<_> = {
        let mut hits = Vec::new();
        for m in &topo.masters {
            if let Some(v) = m.client().get("s-key").await.unwrap() {
                hits.push(v.as_string().unwrap());
            }
        }
        hits
    };
    assert_eq!(
        master_hits,
        vec!["sv".to_string()],
        "slave hit sets back master"
    );

    // Leave the key on the master tier only: write everywhere, then delete
    // from slave and L1s directly (writing one master shard directly would
    // not respect shard routing). With master_as_one_l1 default (true),
    // repeated reads alternate between the L1 and master paths, and the
    // master hit sets back the consulted L1.
    t.set("m-key", value("mv")).await.unwrap();
    topo.slave.client().delete("m-key").await.unwrap();
    topo.master_l1.client().delete("m-key").await.unwrap();
    topo.slave_l1.client().delete("m-key").await.unwrap();
    for _ in 0..6 {
        let got = Cacheable::get(&t, "m-key").await.unwrap().unwrap();
        assert_eq!(got.as_string().unwrap(), "mv");
    }
    assert!(
        topo.master_l1
            .client()
            .get("m-key")
            .await
            .unwrap()
            .is_some(),
        "master hit sets back the consulted L1"
    );
}

#[tokio::test]
#[ignore = "requires Docker with a memcached image"]
async fn delete_and_get_multi_across_shards() {
    let topo = Topology::start("multi").await;
    let t = topo.template();

    // Write enough keys to land on both master shards.
    let keys: Vec<String> = (0..40).map(|i| format!("mk-{i}")).collect();
    for k in &keys {
        assert!(t.set(k, value(&format!("val-{k}"))).await.unwrap());
    }
    let shard_counts: Vec<usize> = {
        let mut counts = Vec::new();
        for m in &topo.masters {
            let refs: Vec<&str> = keys.iter().map(String::as_str).collect();
            counts.push(m.client().get_multi(&refs).await.unwrap().len());
        }
        counts
    };
    assert!(
        shard_counts.iter().all(|&c| c > 0),
        "keys should land on both master shards: {shard_counts:?}"
    );

    let refs: Vec<&str> = keys.iter().map(String::as_str).collect();
    let all = Cacheable::get_multi(&t, &refs).await.unwrap();
    assert_eq!(all.len(), keys.len());

    // delete removes the key from every tier.
    assert!(Cacheable::delete(&t, "mk-0").await.unwrap());
    assert_get(&topo.slave.client(), "mk-0", None).await;
    assert_get(&topo.master_l1.client(), "mk-0", None).await;
    assert_get(&topo.slave_l1.client(), "mk-0", None).await;
    for m in &topo.masters {
        assert_get(&m.client(), "mk-0", None).await;
    }
}

#[tokio::test]
#[ignore = "requires Docker with a memcached image"]
async fn service_template_routes_to_yaml_built_backup() {
    let topo = Topology::start("svc").await;
    let backup: std::sync::Arc<dyn Cacheable> = std::sync::Arc::new(topo.template());
    let template = MemcacheServiceTemplate::builder()
        .backup(backup)
        .use_primary(false)
        .expire_minutes(5)
        .build();

    template.set("sk", "sv").await.unwrap();
    let got = template.get("sk").await.unwrap().unwrap();
    assert_eq!(got.as_string().unwrap(), "sv");
    assert!(template.delete("sk").await.unwrap());
    assert!(template.get("sk").await.unwrap().is_none());
}
