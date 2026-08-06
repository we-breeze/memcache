//! Mesh (byMesh) endpoint discovery via the breeze socks registry.
//!
//! The mesh sidecar advertises each backend as a registry file in a shared
//! directory (default [`DEFAULT_SOCKS_DIR`]). The file name encodes the service
//! coordinates and how to reach it, e.g. for memcached:
//!
//! ```text
//! config.example.com+3+config+v1+<group>+all:<namespace>@mc:<port>@cs
//! ```
//!
//! Parsing follows breeze's `context::Quadruple::parse`: the name is split by
//! `@` into `service@protocol@backend`; the protocol field is split by `:` and
//! if its second token is a numeric port the endpoint is TCP `127.0.0.1:<port>`,
//! otherwise it is a unix socket `<dir>/<token-or-service>.sock`.

use std::fs;
use std::path::Path;

use crate::config::Endpoint;
use crate::error::{Error, Result};

/// Default directory the mesh sidecar writes socks registry files to.
pub const DEFAULT_SOCKS_DIR: &str = "/tmp/breeze/socks";

/// Protocol token used by memcached registry files.
const MC_PROTOCOL: &str = "mc";
const LOCALHOST: &str = "127.0.0.1";

/// A parsed socks registry file name.
struct ParsedSock {
    service: String,
    protocol: String,
    endpoint: Endpoint,
}

/// Parse a socks registry file name into a service + endpoint.
///
/// Returns `None` if the name does not have the expected `a@b@c` shape.
fn parse_sock(dir: &Path, file_name: &str) -> Option<ParsedSock> {
    let fields: Vec<&str> = file_name.split('@').collect();
    if fields.len() != 3 {
        return None;
    }
    let service = fields[0];
    let protocol_item = fields[1];
    let protocol_fields: Vec<&str> = protocol_item.split(':').collect();
    let is_tcp = protocol_fields
        .get(1)
        .is_some_and(|port| port.parse::<u16>().is_ok());
    let endpoint = if is_tcp {
        Endpoint::Tcp {
            host: LOCALHOST.to_string(),
            port: protocol_fields[1].parse().expect("checked numeric port"),
        }
    } else {
        let id = protocol_fields.get(1).copied().unwrap_or(service);
        Endpoint::Unix {
            path: dir.join(format!("{id}.sock")),
        }
    };
    Some(ParsedSock {
        service: service.to_string(),
        protocol: protocol_fields[0].to_string(),
        endpoint,
    })
}

/// Parse a single sock registry file name into an [`Endpoint`], connecting
/// directly (no scan, no remote fetch). `dir` resolves unix-socket file names.
pub(crate) fn endpoint_from_name(dir: &Path, name: &str) -> Result<Endpoint> {
    parse_sock(dir, name)
        .map(|parsed| parsed.endpoint)
        .ok_or_else(|| Error::MeshDiscovery(format!("invalid sock name: {name}")))
}

/// Locate the memcached endpoint for `group`/`namespace` under `dir`.
///
/// Matches the registry file whose protocol is `mc` and whose service ends with
/// `+<group>+all:<namespace>`, ignoring the (deployment-specific) domain prefix.
/// A unix-socket match is preferred over a TCP match when both are present.
pub(crate) fn discover(dir: &Path, group: &str, namespace: &str) -> Result<Endpoint> {
    let suffix = format!("+{group}+all:{namespace}");
    let entries = fs::read_dir(dir).map_err(|err| {
        Error::MeshDiscovery(format!("cannot read socks dir {}: {err}", dir.display()))
    })?;

    let mut tcp_match: Option<Endpoint> = None;
    for entry in entries {
        let entry = entry.map_err(|err| Error::MeshDiscovery(err.to_string()))?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let Some(parsed) = parse_sock(dir, name) else {
            continue;
        };
        if parsed.protocol != MC_PROTOCOL || !parsed.service.ends_with(&suffix) {
            continue;
        }
        match parsed.endpoint {
            endpoint @ Endpoint::Unix { .. } => return Ok(endpoint),
            endpoint => tcp_match.get_or_insert(endpoint),
        };
    }

    tcp_match.ok_or_else(|| {
        Error::MeshDiscovery(format!(
            "no memcached sock for group={group} namespace={namespace} in {}",
            dir.display()
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn parse(name: &str) -> ParsedSock {
        parse_sock(Path::new("/tmp/breeze/socks"), name).unwrap()
    }

    #[test]
    fn parses_tcp_registry_name() {
        let sock = parse(
            "config.example.com+3+config+v1+cache.service.friendship.pool.yf+all:relation_cluster_exposure@mc:9461@cs",
        );
        assert_eq!(sock.protocol, "mc");
        assert_eq!(
            sock.endpoint,
            Endpoint::Tcp {
                host: "127.0.0.1".to_string(),
                port: 9461,
            }
        );
    }

    #[test]
    fn parses_unix_registry_name() {
        let sock = parse("config.example.com+3+config+v1+g+all:ns@mc:U_ns@cs");
        assert_eq!(
            sock.endpoint,
            Endpoint::Unix {
                path: PathBuf::from("/tmp/breeze/socks/U_ns.sock"),
            }
        );
    }

    #[test]
    fn rejects_names_without_three_fields() {
        assert!(parse_sock(Path::new("/tmp"), "not-a-sock-file").is_none());
    }

    #[test]
    fn discovers_matching_memcached_sock() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path()
                .join("config.example.com+3+config+v1+grp+all:nsX@mc:9461@cs"),
            [],
        )
        .unwrap();
        // A redis sock and a mismatched namespace must be ignored.
        std::fs::write(
            dir.path()
                .join("d+3+config+cloud+redis+grp+other@redis:6379@rs"),
            [],
        )
        .unwrap();

        let endpoint = discover(dir.path(), "grp", "nsX").unwrap();
        assert_eq!(
            endpoint,
            Endpoint::Tcp {
                host: "127.0.0.1".to_string(),
                port: 9461,
            }
        );

        assert!(discover(dir.path(), "grp", "missing").is_err());
    }
}
