//! Validate observable behavior across source-driven configuration changes.
#[path = "support/text_server.rs"]
mod fixture;

use async_trait::async_trait;
use bytes::Bytes;
use memcache::{
    CacheNamespaceConf, CacheService, CacheServiceConfigSource, CacheServiceOptions,
    SubscriptionHandle,
};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

type Callback = Arc<dyn Fn(CacheNamespaceConf) + Send + Sync>;
struct Source {
    initial: CacheNamespaceConf,
    callback: Arc<Mutex<Option<Callback>>>,
    cancelled: Arc<AtomicUsize>,
}
#[async_trait]
impl CacheServiceConfigSource for Source {
    async fn load(&self) -> memcache::Result<CacheNamespaceConf> {
        Ok(self.initial.clone())
    }
    async fn subscribe(&self, callback: Callback) -> memcache::Result<SubscriptionHandle> {
        *self.callback.lock().unwrap() = Some(callback);
        let slot = self.callback.clone();
        let cancelled = self.cancelled.clone();
        Ok(SubscriptionHandle::new(move || {
            slot.lock().unwrap().take();
            cancelled.fetch_add(1, Ordering::SeqCst);
        }))
    }
}

#[tokio::test]
async fn updates_replace_the_backend_and_invalid_updates_keep_the_last_valid_one() {
    let (first_port, first_store) = fixture::spawn_text_server().await;
    let (second_port, second_store) = fixture::spawn_text_server().await;
    let callback = Arc::new(Mutex::new(None));
    let cancelled = Arc::new(AtomicUsize::new(0));
    let source = Arc::new(Source {
        initial: CacheNamespaceConf::single_master(format!("127.0.0.1:{first_port}")),
        callback: callback.clone(),
        cancelled: cancelled.clone(),
    });
    let weak = Arc::downgrade(&source);
    let cache = CacheService::new_live(source, CacheServiceOptions::default())
        .await
        .unwrap();
    assert!(weak.upgrade().is_some());
    assert!(fixture::set_when_connected(&cache, "first", Bytes::from_static(b"one")).await);
    assert!(first_store.lock().await.contains_key("first"));
    let next = CacheNamespaceConf::single_master(format!("127.0.0.1:{second_port}"));
    callback.lock().unwrap().as_ref().unwrap()(next.clone());
    assert!(fixture::set_when_connected(&cache, "second", Bytes::from_static(b"two")).await);
    assert!(second_store.lock().await.contains_key("second"));
    assert!(!first_store.lock().await.contains_key("second"));
    callback.lock().unwrap().as_ref().unwrap()(next.clone());
    let mut invalid = next;
    invalid.master.clear();
    callback.lock().unwrap().as_ref().unwrap()(invalid);
    assert!(fixture::set_when_connected(&cache, "retained", Bytes::from_static(b"three")).await);
    assert!(second_store.lock().await.contains_key("retained"));
    let clone = cache.clone();
    drop(cache);
    assert_eq!(cancelled.load(Ordering::SeqCst), 0);
    drop(clone);
    assert_eq!(cancelled.load(Ordering::SeqCst), 1);
    assert!(weak.upgrade().is_none());
}
