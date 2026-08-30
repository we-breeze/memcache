//! Memcache TCP endpoint discovery over the shared Breeze registry crate.

use std::path::Path;

use brz_discovery::{CoordinateLayout, Endpoint, Registry};

pub use brz_discovery::MESH_CONNECT_HOST_ENV;

use crate::error::{Error, Result};

const MC_PROTOCOL: &str = "mc";
const MC_LAYOUT: CoordinateLayout = CoordinateLayout::GroupAllNamespace;

/// Locate the preferred TCP endpoint for an exact `(group, namespace)` pair.
pub(crate) fn discover(dir: &Path, group: &str, namespace: &str) -> Result<Endpoint> {
    Registry::new(dir)
        .discover(MC_PROTOCOL, MC_LAYOUT, group, namespace)
        .map_err(|error| {
            Error::MeshDiscovery(format!("cannot read socks dir {}: {error}", dir.display()))
        })?
        .into_iter()
        .next()
        .ok_or_else(|| {
            Error::MeshDiscovery(format!(
                "no memcache TCP endpoint for group={group} namespace={namespace} in {}",
                dir.display()
            ))
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn discovers_only_the_exact_tcp_coordinate() {
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
                port: 9461,
            }
        );
        assert!(discover(dir.path(), "other", "ns").is_err());
    }
}
