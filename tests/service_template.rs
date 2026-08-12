#![cfg(feature = "service")]

//! Integration tests for `MemcacheServiceTemplate` routing, using in-memory
//! `Cacheable` mocks (mirroring the Java `CacheServiceTemplateTest` mock
//! client approach).
// The process-global switch lock is held across awaits deliberately: tests are
// the only contenders and tokio runs them on separate threads safely.
#![allow(clippy::await_holding_lock)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use memcache::error::Error;
use memcache::service::{
    Cacheable, MULTI_GET_SPLIT_STEP, MemcacheServiceTemplate, set_global_switch,
    set_sync_multi_get_switch,
};
use memcache::value::{CasValue, Value};
use memcache::{Expiration, Result};

/// In-memory cache mock; can be told to fail so fallback paths are exercised.
#[derive(Default)]
struct MockCache {
    data: Mutex<HashMap<String, (Value, u64)>>,
    fail: Mutex<bool>,
    calls: AtomicUsize,
    next_cas: AtomicU64,
}

impl MockCache {
    fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn set_fail(&self, fail: bool) {
        *self.fail.lock().unwrap() = fail;
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::Relaxed)
    }

    fn check(&self) -> Result<()> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        if *self.fail.lock().unwrap() {
            Err(Error::Server("mock failure".to_string()))
        } else {
            Ok(())
        }
    }

    fn put(&self, key: &str, value: &str) {
        let cas = self.next_cas.fetch_add(1, Ordering::Relaxed) + 1;
        self.data.lock().unwrap().insert(
            key.to_string(),
            (Value::new(value.as_bytes().to_vec(), 0), cas),
        );
    }
}

#[async_trait]
impl Cacheable for MockCache {
    async fn get(&self, key: &str) -> Result<Option<Value>> {
        self.check()?;
        Ok(self.data.lock().unwrap().get(key).map(|(v, _)| v.clone()))
    }

    async fn get_multi(&self, keys: &[&str]) -> Result<HashMap<String, Value>> {
        self.check()?;
        let data = self.data.lock().unwrap();
        Ok(keys
            .iter()
            .filter_map(|k| data.get(*k).map(|(v, _)| (k.to_string(), v.clone())))
            .collect())
    }

    async fn set(&self, key: &str, value: Value, _expire: Expiration) -> Result<bool> {
        self.check()?;
        let cas = self.next_cas.fetch_add(1, Ordering::Relaxed) + 1;
        self.data
            .lock()
            .unwrap()
            .insert(key.to_string(), (value, cas));
        Ok(true)
    }

    async fn set_with_noreply(&self, key: &str, value: Value, expire: Expiration) -> Result<()> {
        Cacheable::set(self, key, value, expire).await?;
        Ok(())
    }

    async fn add(&self, key: &str, value: Value, _expire: Expiration) -> Result<bool> {
        self.check()?;
        let mut data = self.data.lock().unwrap();
        if data.contains_key(key) {
            return Ok(false);
        }
        let cas = self.next_cas.fetch_add(1, Ordering::Relaxed) + 1;
        data.insert(key.to_string(), (value, cas));
        Ok(true)
    }

    async fn get_cas(&self, key: &str) -> Result<Option<CasValue>> {
        self.check()?;
        Ok(self
            .data
            .lock()
            .unwrap()
            .get(key)
            .map(|(v, cas)| CasValue::new(v.clone(), *cas)))
    }

    async fn cas(&self, key: &str, value: &CasValue, _expire: Expiration) -> Result<bool> {
        self.check()?;
        let mut data = self.data.lock().unwrap();
        match data.get(key) {
            Some((_, cas)) if *cas == value.cas => {
                let new_cas = self.next_cas.fetch_add(1, Ordering::Relaxed) + 1;
                data.insert(key.to_string(), (value.value.clone(), new_cas));
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    async fn delete(&self, key: &str) -> Result<bool> {
        self.check()?;
        Ok(self.data.lock().unwrap().remove(key).is_some())
    }

    async fn delete_with_noreply(&self, key: &str) -> Result<()> {
        Cacheable::delete(self, key).await?;
        Ok(())
    }
}

/// The switches are process-global, so tests touching them serialize on this.
static SWITCH_LOCK: Mutex<()> = Mutex::new(());

fn backup_only_template(backup: Arc<MockCache>) -> MemcacheServiceTemplate {
    MemcacheServiceTemplate::builder()
        .backup(backup)
        .use_primary(false)
        .expire_minutes(60)
        .build()
}

async fn exercise_all_methods(template: &MemcacheServiceTemplate) -> Result<()> {
    assert!(template.set("k1", "v1").await?);
    assert_eq!(template.get("k1").await?.unwrap().as_string()?, "v1");
    assert!(template.get("missing").await?.is_none());

    assert!(!template.add("k1", "other").await?);
    assert!(template.add("k2", "v2").await?);

    let map = template.get_multi(&["k1", "k2", "missing"]).await?;
    assert_eq!(map.len(), 2);
    assert_eq!(map["k1"].as_string()?, "v1");

    template.set_with_noreply("k3", "v3").await?;
    assert_eq!(template.get("k3").await?.unwrap().as_string()?, "v3");

    assert!(template.delete("k3").await?);
    assert!(!template.delete("k3").await?);
    template.delete_with_noreply("k2").await?;
    assert!(template.get("k2").await?.is_none());

    let cas = template.get_cas("k1").await?.expect("k1 exists");
    let updated = CasValue::new(Value::new("v1-cas".as_bytes().to_vec(), 0), cas.cas);
    assert!(template.cas("k1", &updated).await?);
    assert_eq!(template.get("k1").await?.unwrap().as_string()?, "v1-cas");
    // The stale token must now fail.
    assert!(!template.cas("k1", &updated).await?);
    Ok(())
}

#[tokio::test]
async fn backup_only_routes_everything_to_backup() {
    let _guard = SWITCH_LOCK.lock().unwrap();
    set_global_switch(true);

    let backup = MockCache::new();
    let template = backup_only_template(backup.clone());
    exercise_all_methods(&template).await.unwrap();
    assert!(backup.calls() > 0);
}

#[tokio::test]
async fn missing_backup_and_no_primary_errors() {
    let _guard = SWITCH_LOCK.lock().unwrap();
    set_global_switch(true);

    let template = MemcacheServiceTemplate::builder().build();
    assert!(template.get("k").await.is_err());
}

#[tokio::test]
async fn primary_failure_falls_back_to_backup() {
    let _guard = SWITCH_LOCK.lock().unwrap();
    set_global_switch(true);

    let primary = MockCache::new();
    let backup = MockCache::new();
    let template = MemcacheServiceTemplate::builder()
        .primary(primary.clone())
        .backup(backup.clone())
        .use_primary(true)
        .build();

    // Healthy primary serves traffic.
    template.set("k", "from-primary").await.unwrap();
    assert!(primary.calls() > 0);
    assert_eq!(backup.calls(), 0);

    // Failing primary: reads fall back to the backup.
    primary.set_fail(true);
    backup.put("k", "from-backup");
    let value = template.get("k").await.unwrap().unwrap();
    assert_eq!(value.as_string().unwrap(), "from-backup");
}

#[tokio::test]
async fn primary_circuit_opens_after_consecutive_failures() {
    let _guard = SWITCH_LOCK.lock().unwrap();
    set_global_switch(true);

    let primary = MockCache::new();
    let backup = MockCache::new();
    let template = MemcacheServiceTemplate::builder()
        .primary(primary.clone())
        .backup(backup.clone())
        .use_primary(true)
        .build();

    primary.set_fail(true);
    backup.put("k", "v");

    // The first few failures still try the primary each time; after the
    // threshold the circuit opens and requests go straight to the backup.
    for _ in 0..5 {
        template.get("k").await.unwrap();
    }
    let calls_at_open = primary.calls();
    for _ in 0..10 {
        template.get("k").await.unwrap();
    }
    assert_eq!(
        primary.calls(),
        calls_at_open,
        "open circuit must short-circuit the primary"
    );
    assert!(backup.calls() >= 15, "backup serves all fallback traffic");
}

#[tokio::test]
async fn global_switch_off_forces_backup() {
    let _guard = SWITCH_LOCK.lock().unwrap();

    let primary = MockCache::new();
    let backup = MockCache::new();
    let template = MemcacheServiceTemplate::builder()
        .primary(primary.clone())
        .backup(backup.clone())
        .use_primary(true)
        .build();

    set_global_switch(false);
    template.set("k", "v").await.unwrap();
    set_global_switch(true);

    assert_eq!(primary.calls(), 0);
    assert!(backup.calls() > 0);
}

#[tokio::test]
async fn get_multi_splits_large_key_sets() {
    let _guard = SWITCH_LOCK.lock().unwrap();
    set_global_switch(true);
    set_sync_multi_get_switch(false);

    let backup = MockCache::new();
    let template = backup_only_template(backup.clone());

    let count = MULTI_GET_SPLIT_STEP * 3 + 17;
    let keys: Vec<String> = (0..count).map(|i| format!("key_{i}")).collect();
    for k in &keys {
        backup.put(k, &format!("val-{k}"));
    }

    let refs: Vec<&str> = keys.iter().map(String::as_str).collect();
    let result = template.get_multi(&refs).await.unwrap();
    set_sync_multi_get_switch(true);

    assert_eq!(result.len(), count);
    assert_eq!(result["key_0"].as_string().unwrap(), "val-key_0");
    let last = keys.last().unwrap();
    assert_eq!(result[last].as_string().unwrap(), format!("val-{last}"));
}

#[tokio::test]
async fn get_multi_empty_keys_short_circuits() {
    let template = MemcacheServiceTemplate::builder().build();
    assert!(template.get_multi(&[]).await.unwrap().is_empty());
}

#[test]
fn expire_minutes_is_clamped() {
    let template = MemcacheServiceTemplate::builder()
        .expire_minutes(u64::MAX)
        .build();
    assert_eq!(template.expire(), Expiration::Seconds(1440 * 30 * 60));
}
