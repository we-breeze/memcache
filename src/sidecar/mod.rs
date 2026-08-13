//! Mesh access mode: reach memcached **through the local breeze mesh agent**
//! (the byMesh path).
//!
//! The mesh publishes one sock registry file per resource in a local
//! directory; [`discovery`] parses the file name to resolve the namespace's
//! local endpoint (no fetch/register step), and the client speaks the
//! memcached binary/text protocol over it — sharding and backend failover
//! are the mesh's job. A shared scanner watches the directory and lets the
//! client follow endpoint changes (e.g. a port reassignment).
//!
//! Entry points:
//!
//! - [`SidecarClient`] — the pooled client
//!   ([`SidecarClient::connect("namespace")`](SidecarClient::connect));
//! - [`MeshConfig`] — namespace/group/socket-dir/protocol/pool/timeout
//!   settings.
//!
//! For direct backend access (no mesh), see [`crate::direct`]; for
//! replay/comparison topologies, see [`crate::replay`].

pub mod client;
pub mod config;
pub mod discovery;
pub(crate) mod scanner;

pub use client::SidecarClient;
pub use config::{DEFAULT_SOCKS_DIR, MeshConfig};
pub use discovery::MESH_CONNECT_HOST_ENV;
