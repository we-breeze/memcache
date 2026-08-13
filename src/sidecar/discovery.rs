//! Memcache adapter over the shared Breeze socks-registry discovery crate.
//!
//! Cache-service advertisements encode coordinates as
//! `...+<group>+all:<namespace>@mc:<slot>@<backend>`. The shared crate owns
//! parsing, exact matching, `/data1/breeze/socks`, and `MESH_CONNECT_HOST`;
//! this module maps its results into the memcache client's refresh model.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use brz_discovery::{CoordinateLayout, Registry};

pub use brz_discovery::{DEFAULT_SOCKS_DIR, MESH_CONNECT_HOST_ENV};

use crate::config::Endpoint;
use crate::error::{Error, Result};

const MC_PROTOCOL: &str = "mc";
const MC_LAYOUT: CoordinateLayout = CoordinateLayout::GroupAllNamespace;

/// How to rediscover one memcache endpoint from the Breeze registry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MeshDiscovery {
    pub(crate) dir: PathBuf,
    pub(crate) group: Option<String>,
    pub(crate) namespace: String,
}

/// Coordinates identifying one memcache service in a registry snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct SockKey {
    pub(crate) group: Option<String>,
    pub(crate) namespace: String,
}

/// Parse a concrete registry name. Coordinates are optional for compatibility
/// with callers that pass a bare `service@mc:<slot>@backend` name directly.
pub(crate) fn endpoint_from_name(dir: &Path, name: &str) -> Result<(Endpoint, Option<SockKey>)> {
    let registry = Registry::new(dir);
    let endpoint = registry
        .endpoint_from_name(MC_PROTOCOL, name)
        .ok_or_else(|| Error::MeshDiscovery(format!("invalid memcache sock name: {name}")))?;
    let key = registry
        .parse_endpoint(MC_PROTOCOL, MC_LAYOUT, name)
        .map(|parsed| SockKey {
            group: Some(parsed.group),
            namespace: parsed.namespace,
        });
    Ok((endpoint, key))
}

/// Locate the preferred endpoint for an exact `(group, namespace)` pair.
pub fn discover(dir: &Path, group: &str, namespace: &str) -> Result<Endpoint> {
    Registry::new(dir)
        .discover(MC_PROTOCOL, MC_LAYOUT, group, namespace)
        .map_err(|err| {
            Error::MeshDiscovery(format!("cannot read socks dir {}: {err}", dir.display()))
        })?
        .into_iter()
        .next()
        .ok_or_else(|| {
            Error::MeshDiscovery(format!(
                "no memcache sock for group={group} namespace={namespace} in {}",
                dir.display()
            ))
        })
}

/// Scan every memcache advertisement into the refresh snapshot.
///
/// A transient directory error produces an empty snapshot. The scanner keeps
/// the client's current endpoint when its key is absent, matching the existing
/// endpoint-refresh failure semantics.
pub(crate) fn scan_all(dir: &Path) -> HashMap<SockKey, Endpoint> {
    let Ok(entries) = Registry::new(dir).scan(MC_PROTOCOL, MC_LAYOUT) else {
        return HashMap::new();
    };
    let mut snapshot = HashMap::new();
    for entry in entries {
        snapshot
            .entry(SockKey {
                group: Some(entry.group),
                namespace: entry.namespace,
            })
            .or_insert(entry.endpoint);
    }
    snapshot
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn discovers_only_the_exact_cache_service_coordinate() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path()
                .join("config.example.com+3+config+v1+grp+all:ns@mc:9461@cs"),
            [],
        )
        .unwrap();

        assert_eq!(
            discover(dir.path(), "grp", "ns").unwrap(),
            Endpoint {
                host: "127.0.0.1".into(),
                port: 9461
            }
        );
        assert!(discover(dir.path(), "other", "ns").is_err());
    }
}
