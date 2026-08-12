#![cfg(feature = "service")]

//! Behavior tests for the Java-aligned `MemCacheTemplate`, using in-memory
//! mock pools to verify the read cascade, write fan-out, and policy
//! switches against the Java `MemCacheTemplate` semantics.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use memcache::service::{Cacheable, MemCacheTemplate, WritePolicy};
use memcache::value::{CasValue, Value};
use memcache::{Expiration, Result};

#[derive(Default)]
struct Counts {
    get: AtomicUsize,
    set: AtomicUsize,
    add: AtomicUsize,
    delete: AtomicUsize,
    cas: AtomicUsize,
}

/// In-memory pool mock with per-op counters and an optional failure mode.
#[derive(Default)]
struct MockPool {
    data: Mutex<HashMap<String, (Value, u64)>>,
    fail_reads: Mutex<bool>,
    fail_writes: Mutex<bool>,
    counts: Counts,
    next_cas: AtomicU64,
}

impl MockPool {
    fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn seed(&self, key: &str, value: &str) {
        let cas = self.next_cas.fetch_add(1, Ordering::Relaxed) + 1;
        self.data.lock().unwrap().insert(
            key.to_string(),
            (Value::new(value.as_bytes().to_vec(), 0), cas),
        );
    }

    fn has(&self, key: &str, value: &str) -> bool {
        self.data
            .lock()
            .unwrap()
            .get(key)
            .is_some_and(|(v, _)| v.as_bytes() == value.as_bytes())
    }

    fn contains(&self, key: &str) -> bool {
        self.data.lock().unwrap().contains_key(key)
    }

    fn count(&self, pick: impl Fn(&Counts) -> &AtomicUsize) -> usize {
        pick(&self.counts).load(Ordering::Relaxed)
    }
}

#[async_trait]
impl Cacheable for MockPool {
    async fn get(&self, key: &str) -> Result<Option<Value>> {
        self.counts.get.fetch_add(1, Ordering::Relaxed);
        if *self.fail_reads.lock().unwrap() {
            return Err(memcache::Error::Server("mock read failure".into()));
        }
        Ok(self.data.lock().unwrap().get(key).map(|(v, _)| v.clone()))
    }

    async fn get_multi(&self, keys: &[&str]) -> Result<HashMap<String, Value>> {
        self.counts.get.fetch_add(1, Ordering::Relaxed);
        if *self.fail_reads.lock().unwrap() {
            return Err(memcache::Error::Server("mock read failure".into()));
        }
        let data = self.data.lock().unwrap();
        Ok(keys
            .iter()
            .filter_map(|k| data.get(*k).map(|(v, _)| (k.to_string(), v.clone())))
            .collect())
    }

    async fn set(&self, key: &str, value: Value, _expire: Expiration) -> Result<bool> {
        self.counts.set.fetch_add(1, Ordering::Relaxed);
        if *self.fail_writes.lock().unwrap() {
            return Ok(false);
        }
        let cas = self.next_cas.fetch_add(1, Ordering::Relaxed) + 1;
        self.data
            .lock()
            .unwrap()
            .insert(key.to_string(), (value, cas));
        Ok(true)
    }

    async fn set_with_noreply(&self, key: &str, value: Value, expire: Expiration) -> Result<()> {
        Cacheable::set(self, key, value, expire).await.map(|_| ())
    }

    async fn add(&self, key: &str, value: Value, _expire: Expiration) -> Result<bool> {
        self.counts.add.fetch_add(1, Ordering::Relaxed);
        if *self.fail_writes.lock().unwrap() {
            return Ok(false);
        }
        let mut data = self.data.lock().unwrap();
        if data.contains_key(key) {
            return Ok(false);
        }
        let cas = self.next_cas.fetch_add(1, Ordering::Relaxed) + 1;
        data.insert(key.to_string(), (value, cas));
        Ok(true)
    }

    async fn get_cas(&self, key: &str) -> Result<Option<CasValue>> {
        Ok(self
            .data
            .lock()
            .unwrap()
            .get(key)
            .map(|(v, cas)| CasValue::new(v.clone(), *cas)))
    }

    async fn cas(&self, key: &str, value: &CasValue, _expire: Expiration) -> Result<bool> {
        self.counts.cas.fetch_add(1, Ordering::Relaxed);
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
        self.counts.delete.fetch_add(1, Ordering::Relaxed);
        Ok(self.data.lock().unwrap().remove(key).is_some())
    }

    async fn delete_with_noreply(&self, key: &str) -> Result<()> {
        Cacheable::delete(self, key).await.map(|_| ())
    }
}

fn template(
    master: &Arc<MockPool>,
    slave: Option<&Arc<MockPool>>,
    master_l1: &[&Arc<MockPool>],
    slave_l1: &[&Arc<MockPool>],
) -> MemCacheTemplate {
    let mut builder = MemCacheTemplate::builder()
        .master(master.clone())
        .master_as_one_l1(false);
    if let Some(s) = slave {
        builder = builder.slave(s.clone());
    }
    for pool in master_l1 {
        builder = builder.master_l1_pool((*pool).clone());
    }
    for pool in slave_l1 {
        builder = builder.slave_l1_pool((*pool).clone());
    }
    builder.build()
}

#[tokio::test]
async fn get_cascades_l1_master_slave_with_setbacks() {
    let (master, slave, l1) = (MockPool::new(), MockPool::new(), MockPool::new());
    let t = template(&master, Some(&slave), &[&l1], &[]);

    // L1 hit short-circuits.
    l1.seed("k", "from-l1");
    let got = Cacheable::get(&t, "k").await.unwrap().unwrap();
    assert_eq!(got.as_string().unwrap(), "from-l1");
    assert_eq!(master.count(|c| &c.get), 0);

    // Master hit sets back into the consulted L1.
    master.seed("m", "from-master");
    let got = Cacheable::get(&t, "m").await.unwrap().unwrap();
    assert_eq!(got.as_string().unwrap(), "from-master");
    assert!(l1.has("m", "from-master"));
    assert_eq!(slave.count(|c| &c.get), 0);

    // Slave hit sets back into both master and L1.
    slave.seed("s", "from-slave");
    let got = Cacheable::get(&t, "s").await.unwrap().unwrap();
    assert_eq!(got.as_string().unwrap(), "from-slave");
    assert!(master.has("s", "from-slave"));
    assert!(l1.has("s", "from-slave"));

    // Total miss.
    assert!(Cacheable::get(&t, "nope").await.unwrap().is_none());
}

#[tokio::test]
async fn master_as_one_l1_penetrates_to_master() {
    let (master, l1) = (MockPool::new(), MockPool::new());
    master.seed("k", "from-master");
    l1.seed("k", "from-l1");
    let t = MemCacheTemplate::builder()
        .master(master.clone())
        .master_l1_pool(l1.clone())
        .master_as_one_l1(true)
        .build();

    // One L1 pool + master-as-one-L1: reads alternate between consulting the
    // L1 (hit, master untouched) and penetrating straight to master.
    Cacheable::get(&t, "k").await.unwrap();
    Cacheable::get(&t, "k").await.unwrap();
    assert_eq!(master.count(|c| &c.get), 1);
    assert_eq!(l1.count(|c| &c.get), 1);
}

#[tokio::test]
async fn get_multi_cascades_and_sets_back() {
    let (master, slave, l1) = (MockPool::new(), MockPool::new(), MockPool::new());
    l1.seed("a", "la");
    master.seed("b", "mb");
    slave.seed("c", "sc");
    let t = template(&master, Some(&slave), &[&l1], &[]);

    let values = Cacheable::get_multi(&t, &["a", "b", "c", "d"])
        .await
        .unwrap();
    assert_eq!(values.len(), 3);
    assert_eq!(values["a"].as_string().unwrap(), "la");
    assert_eq!(values["b"].as_string().unwrap(), "mb");
    assert_eq!(values["c"].as_string().unwrap(), "sc");

    // master/slave hits are set back into the consulted L1; the slave hit is
    // also set back into master.
    assert!(l1.has("b", "mb"));
    assert!(l1.has("c", "sc"));
    assert!(master.has("c", "sc"));
}

#[tokio::test]
async fn set_aborts_on_master_failure_unless_force_write_all() {
    let (master, slave) = (MockPool::new(), MockPool::new());
    *master.fail_writes.lock().unwrap() = true;

    let t = template(&master, Some(&slave), &[], &[]);
    let rs = Cacheable::set(
        &t,
        "k",
        Value::new("v".as_bytes().to_vec(), 0),
        Expiration::Never,
    )
    .await
    .unwrap();
    assert!(!rs);
    assert_eq!(slave.count(|c| &c.set), 0);

    let t = MemCacheTemplate::builder()
        .master(master.clone())
        .slave(slave.clone())
        .force_write_all(true)
        .build();
    let rs = Cacheable::set(
        &t,
        "k",
        Value::new("v".as_bytes().to_vec(), 0),
        Expiration::Never,
    )
    .await
    .unwrap();
    assert!(!rs);
    assert!(slave.has("k", "v"));
}

#[tokio::test]
async fn add_only_fans_out_on_master_success() {
    let (master, slave) = (MockPool::new(), MockPool::new());
    master.seed("k", "existing");
    let t = template(&master, Some(&slave), &[], &[]);

    let rs = Cacheable::add(
        &t,
        "k",
        Value::new("v".as_bytes().to_vec(), 0),
        Expiration::Never,
    )
    .await
    .unwrap();
    assert!(!rs);
    assert_eq!(slave.count(|c| &c.set), 0);

    let rs = Cacheable::add(
        &t,
        "fresh",
        Value::new("v".as_bytes().to_vec(), 0),
        Expiration::Never,
    )
    .await
    .unwrap();
    assert!(rs);
    assert!(slave.has("fresh", "v"));
}

#[tokio::test]
async fn cas_uses_plain_set_beyond_master() {
    let (master, slave, l1) = (MockPool::new(), MockPool::new(), MockPool::new());
    master.seed("k", "v0");
    let t = template(&master, Some(&slave), &[&l1], &[]);

    let cas = Cacheable::get_cas(&t, "k").await.unwrap().unwrap();
    let updated = CasValue::new(Value::new("v1".as_bytes().to_vec(), 0), cas.cas);
    assert!(
        Cacheable::cas(&t, "k", &updated, Expiration::Never)
            .await
            .unwrap()
    );
    assert!(master.has("k", "v1"));
    // Slave / L1 receive a set of the value, not a cas with the master token.
    assert_eq!(slave.count(|c| &c.cas), 0);
    assert!(slave.has("k", "v1"));
    assert!(l1.has("k", "v1"));

    // A stale token fails on master and aborts the fan-out.
    assert!(
        !Cacheable::cas(&t, "k", &updated, Expiration::Never)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn delete_fans_out_with_slave_l1_gate() {
    let (master, slave, ml1, sl1) = (
        MockPool::new(),
        MockPool::new(),
        MockPool::new(),
        MockPool::new(),
    );
    for pool in [&master, &slave, &ml1, &sl1] {
        pool.seed("k", "v");
    }
    let t = template(&master, Some(&slave), &[&ml1], &[&sl1]);

    assert!(Cacheable::delete(&t, "k").await.unwrap());
    assert!(!master.contains("k"));
    assert!(!slave.contains("k"));
    assert!(!ml1.contains("k"));
    assert!(!sl1.contains("k"));

    // update_slave_l1 off: slave L1 is left alone (master L1 always deleted).
    for pool in [&master, &slave, &ml1, &sl1] {
        pool.seed("k", "v");
    }
    t.set_update_slave_l1(false);
    Cacheable::delete(&t, "k").await.unwrap();
    assert!(!ml1.contains("k"));
    assert!(sl1.contains("k"));
}

#[tokio::test]
async fn write_policies_drive_l1_behavior() {
    let value = || Value::new("v".as_bytes().to_vec(), 0);

    // writeAll: L1 gets the set.
    let (master, l1) = (MockPool::new(), MockPool::new());
    let t = template(&master, None, &[&l1], &[]);
    Cacheable::set(&t, "k", value(), Expiration::Never)
        .await
        .unwrap();
    assert!(l1.has("k", "v"));

    // writeAndDeleteL1: the L1 entry is deleted instead of set.
    let (master, l1) = (MockPool::new(), MockPool::new());
    l1.seed("k", "old");
    let t = MemCacheTemplate::builder()
        .master(master.clone())
        .master_l1_pool(l1.clone())
        .write_policy(WritePolicy::WriteAndDeleteL1)
        .build();
    Cacheable::set(&t, "k", value(), Expiration::Never)
        .await
        .unwrap();
    assert!(!l1.contains("k"));
    assert_eq!(l1.count(|c| &c.set), 0);

    // writeAndIfExistL1: only existing L1 entries are refreshed.
    let (master, l1) = (MockPool::new(), MockPool::new());
    l1.seed("hot", "old");
    let t = MemCacheTemplate::builder()
        .master(master.clone())
        .master_l1_pool(l1.clone())
        .write_policy(WritePolicy::WriteAndIfExistL1)
        .build();
    Cacheable::set(&t, "hot", value(), Expiration::Never)
        .await
        .unwrap();
    Cacheable::set(&t, "cold", value(), Expiration::Never)
        .await
        .unwrap();
    assert!(l1.has("hot", "v"));
    assert!(!l1.contains("cold"));
}

#[tokio::test]
async fn read_only_short_circuits_writes() {
    let master = MockPool::new();
    let t = template(&master, None, &[], &[]);
    t.set_read_only(true);

    assert!(
        Cacheable::set(
            &t,
            "k",
            Value::new("v".as_bytes().to_vec(), 0),
            Expiration::Never
        )
        .await
        .unwrap()
    );
    assert!(
        Cacheable::add(
            &t,
            "k",
            Value::new("v".as_bytes().to_vec(), 0),
            Expiration::Never
        )
        .await
        .unwrap()
    );
    assert_eq!(master.count(|c| &c.set), 0);
    assert_eq!(master.count(|c| &c.add), 0);
}

#[tokio::test]
async fn update_master_l1_gate_skips_l1_writes() {
    let (master, l1) = (MockPool::new(), MockPool::new());
    let t = template(&master, None, &[&l1], &[]);
    t.set_update_master_l1(false);
    Cacheable::set(
        &t,
        "k",
        Value::new("v".as_bytes().to_vec(), 0),
        Expiration::Never,
    )
    .await
    .unwrap();
    assert_eq!(l1.count(|c| &c.set), 0);
}
