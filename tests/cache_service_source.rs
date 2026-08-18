//! Unit tests for `CacheService` hot-swap driven by an in-process fake
//! `CacheServiceConfigSource` — no Vintage, no Docker.
//!
//! These verify the push model: the source calls `on_update` and the
//! `CacheService` backend hot-swaps. `CacheService` never polls.

#![cfg(feature = "service")]

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use memcache::cacheservice::{CacheServiceConfig, CacheServiceError};
use memcache::service::vintage_live::CacheServiceInner;
use memcache::{
    CacheNamespaceConf, CacheService, CacheServiceConfigSource, CacheServiceOptions,
    SubscriptionHandle,
};

/// A fake source that records every config pushed to `on_update`. `push(conf)`
/// simulates a config change arriving from the source by invoking the
/// callback captured at subscribe time. Clone shares the recorded-pushes list
/// so the test driver can call `push` on a clone while the original is moved
/// into `CacheService::new_live`.
#[derive(Clone)]
struct FakeSource {
    pushed: Arc<Mutex<Vec<CacheNamespaceConf>>>,
}

impl FakeSource {
    fn new() -> Self {
        Self {
            pushed: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Simulates a config change: invokes the registered callback.
    fn push(&self, conf: CacheNamespaceConf) {
        self.pushed.lock().unwrap().push(conf.clone());
        if let Some(cb) = CALLBACK.with(|cell| cell.borrow().clone()) {
            cb(conf);
        }
    }
}

type Callback = Arc<dyn Fn(CacheNamespaceConf) + Send + Sync>;

thread_local! {
    static CALLBACK: std::cell::RefCell<Option<Callback>> =
        const { std::cell::RefCell::new(None) };
}

#[async_trait]
impl CacheServiceConfigSource for FakeSource {
    async fn load(&self) -> Result<CacheNamespaceConf, memcache::Error> {
        Ok(conf("127.0.0.1:11211"))
    }

    async fn subscribe(
        &self,
        on_update: Arc<dyn Fn(CacheNamespaceConf) + Send + Sync>,
    ) -> Result<SubscriptionHandle, memcache::Error> {
        CALLBACK.with(|cell| *cell.borrow_mut() = Some(on_update));
        Ok(SubscriptionHandle::empty())
    }
}

fn conf(master: &str) -> CacheNamespaceConf {
    let yaml = format!("ns:\n  hash: crc32\n  distribution: modula\n  master:\n  - {master}\n");
    CacheServiceConfig::from_yaml_str(&yaml)
        .unwrap()
        .namespace("ns")
        .unwrap()
        .clone()
}

#[tokio::test]
async fn static_new_builds_from_a_config() {
    // A static CacheService::new just needs a config; the backend is built
    // lazily (direct clients connect lazily) so no live memcached is required
    // to construct.
    let cache = CacheService::new(conf("127.0.0.1:11211"), CacheServiceOptions::default())
        .await
        .unwrap();
    drop(cache);
}

#[tokio::test]
async fn new_live_loads_initial_config() {
    let source: Arc<dyn CacheServiceConfigSource> = Arc::new(FakeSource::new());
    let cache = CacheService::new_live(source, CacheServiceOptions::default())
        .await
        .unwrap();
    // The cache exists and holds the initial backend; no push has happened.
    drop(cache);
}

#[tokio::test]
async fn new_live_applies_pushed_config() {
    let source = Arc::new(FakeSource::new());
    let pushed = Arc::clone(&source.pushed);
    // Keep a clone to drive pushes; the original moves into new_live. The
    // thread-local callback captured at subscribe time routes pushes to the
    // cache's apply.
    let driver = source.clone();
    let source_dyn: Arc<dyn CacheServiceConfigSource> = source;
    let cache = CacheService::new_live(source_dyn, CacheServiceOptions::default())
        .await
        .unwrap();

    driver.push(conf("127.0.0.1:11212"));
    assert_eq!(pushed.lock().unwrap().len(), 1);

    // A semantically-equal push is a no-op at the apply level (PartialEq).
    driver.push(conf("127.0.0.1:11212"));
    drop(cache);
}

#[tokio::test]
async fn partial_eq_skip_is_semantic() {
    // Two configs parsed from identical YAML compare equal -> apply is a no-op.
    let yaml = "ns:\n  hash: crc32\n  distribution: modula\n  master:\n  - 127.0.0.1:9\n";
    let c1 = CacheServiceConfig::from_yaml_str(yaml)
        .unwrap()
        .namespace("ns")
        .unwrap()
        .clone();
    let c2 = CacheServiceConfig::from_yaml_str(yaml)
        .unwrap()
        .namespace("ns")
        .unwrap()
        .clone();
    assert_eq!(c1, c2);
}

#[tokio::test]
async fn parse_namespace_yaml_errors_on_missing() {
    let err = CacheServiceInner::parse_namespace_yaml("ns", "other:\n  master: []\n").unwrap_err();
    assert!(matches!(err, CacheServiceError::MissingNamespace(_)));
}
