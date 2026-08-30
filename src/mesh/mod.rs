//! Mesh discovery: reach memcached **through the local breeze mesh agent**
//! (the byMesh path).
//!
//! The mesh publishes one sock registry file per resource in a local
//! directory; [`discovery`] parses the file name to resolve the namespace's
//! local endpoint (no fetch/register step), and the client speaks the
//! memcached binary/text protocol over it — sharding and backend failover
//! are the mesh's job. [`crate::CacheService::mesh`] uses this discovery
//! layer once at construction and then uses the same
//! `brz-net` session implementation as every other CacheService topology.

pub mod config;
pub mod discovery;

pub use config::{DEFAULT_SOCKS_DIR, MeshConfig};
pub use discovery::MESH_CONNECT_HOST_ENV;
