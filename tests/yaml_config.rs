#![cfg(feature = "service")]

//! YAML/snapshot-driven configuration tests (no Docker required).
//!
//! The committed fixture `fixtures/cache_service_snapshot.txt` reproduces
//! the on-disk Vintage statics-config snapshot format (first line: content
//! sign, second line: the `all` key, remainder: the YAML document), trimmed
//! from a real `cache.service.*.pool.*` snapshot. Set `BRZ_MCS_SNAPSHOT` to
//! point at a live snapshot file (e.g. under `/tmp/breeze/snapshot/`) to run
//! the same assertions against production configuration.

use memcache::cacheservice::{CacheNamespaceConf, CacheServiceConfig};
use memcache::service::{MemCacheTemplate, PoolOptions};

const FIXTURE: &str = include_str!("fixtures/cache_service_snapshot.txt");

/// Extracts the YAML body from a snapshot file: skips the leading sign line
/// and the `all` key line, like the Vintage client's `all_entry_value`.
fn yaml_from_snapshot(snapshot: &str) -> &str {
    let mut lines = snapshot.lines();
    let sign = lines.next().expect("snapshot sign line");
    assert!(
        sign.trim().chars().all(|c| c.is_ascii_digit()),
        "first snapshot line should be the numeric content sign, got {sign:?}"
    );
    assert_eq!(lines.next(), Some("all"), "second snapshot line");
    let offset = snapshot.find("\nall\n").expect("all key line") + 5;
    &snapshot[offset..]
}

fn snapshot() -> String {
    match std::env::var("BRZ_MCS_SNAPSHOT") {
        Ok(path) => std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("failed to read BRZ_MCS_SNAPSHOT={path}: {e}")),
        Err(_) => FIXTURE.to_string(),
    }
}

#[test]
fn snapshot_yaml_parses_namespaces() {
    let snap = snapshot();
    let yaml = yaml_from_snapshot(&snap);
    let conf = CacheServiceConfig::from_yaml_str(yaml).expect("snapshot yaml parses");

    // Every namespace exposes its pools; masters must be non-empty for any
    // namespace that carries L1 groups.
    let namespaces = namespace_names(yaml);
    assert!(!namespaces.is_empty(), "snapshot should contain namespaces");
    for name in &namespaces {
        let ns = conf.namespace(name).expect("namespace present");
        assert!(
            !ns.masters().is_empty() || ns.master_l1().is_empty(),
            "namespace {name} has L1 groups but no masters"
        );
    }
}

/// Enumerates the namespace keys of a snapshot YAML document (skipping the
/// `global` block) by re-reading the top-level mapping; `CacheServiceConfig`
/// itself only exposes lookup-by-name.
fn namespace_names(yaml: &str) -> Vec<String> {
    let value: serde_yaml::Value = serde_yaml::from_str(yaml).expect("yaml re-parse");
    value
        .as_mapping()
        .into_iter()
        .flat_map(|m| m.keys())
        .filter_map(|k| k.as_str().map(str::to_string))
        .filter(|k| k != "global")
        .collect()
}

#[test]
fn fixture_pools_match_expected_shape() {
    let conf = CacheServiceConfig::from_yaml_str(yaml_from_snapshot(FIXTURE)).unwrap();

    let full = conf.namespace("attitude.specVector").unwrap();
    assert_eq!(full.hash(), Some("crc32"));
    assert_eq!(full.distribution(), Some("modula"));
    assert_eq!(full.masters().len(), 2);
    assert_eq!(full.slaves().len(), 2);
    assert_eq!(full.master_l1().len(), 1);
    assert_eq!(full.master_l1()[0].len(), 2);
    assert_eq!(full.slave_l1().len(), 1);

    let l1_only = conf.namespace("status.darwinTag").unwrap();
    assert_eq!(l1_only.hash(), Some("bkdr"));
    assert_eq!(l1_only.distribution(), Some("ketama"));
    assert_eq!(l1_only.masters().len(), 3);
    assert_eq!(l1_only.master_l1().len(), 2);
    assert!(l1_only.slaves().is_empty());
}

#[tokio::test]
async fn template_builds_from_snapshot_namespaces() {
    let snap = snapshot();
    let yaml = yaml_from_snapshot(&snap).to_string();
    let conf = CacheServiceConfig::from_yaml_str(&yaml).unwrap();

    // Construction is lazy enough to succeed without reachable backends; the
    // point is that every real-shape namespace maps onto a template.
    for name in namespace_names(&yaml) {
        let ns: &CacheNamespaceConf = conf.namespace(&name).unwrap();
        if ns.masters().is_empty() {
            continue;
        }
        MemCacheTemplate::from_namespace_conf(ns, PoolOptions::default())
            .unwrap_or_else(|e| panic!("namespace {name} should build a template: {e}"));
    }
}
