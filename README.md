# brz-memcache

An asynchronous memcached client with text and binary protocols, sharding,
replica failover, L1 caches, and application-supplied live configuration.
Connections and protocol sessions use `brz-net`.

## Fixed configuration

The Rust library name is `memcache`:

```rust
use bytes::Bytes;
use memcache::{CacheNamespaceConf, CacheService, CacheServiceOptions, Memcache};

# async fn example() -> memcache::Result<()> {
let mut config = CacheNamespaceConf::single_master("127.0.0.1:11211".into());
config.slave = vec!["127.0.0.1:11212".into()];
let cache = CacheService::new(config, CacheServiceOptions::default()).await?;
cache.set("example", Bytes::from_static(b"value")).await?;
let value = cache.get("example").await?;
# Ok(())
# }
```

`CacheService::single(endpoint)` is the single-master convenience constructor.
`CacheServiceConfig::from_yaml_str` accepts a map of namespace names to native
configuration blocks. The configuration exposes master/slave endpoints, L1
groups, hash/distribution, timeouts, writeback expiration, and explicit
`update_slave_l1`, `local_affinity`, and `backend_no_storage` booleans.

## Live configuration

Implement `CacheServiceConfigSource` in the application or an adapter:

- `load()` returns the initial `CacheNamespaceConf`.
- `subscribe(callback)` registers updates and returns a `SubscriptionHandle`.
  Replay the current configuration when subscribing to avoid losing changes
  between `load` and `subscribe`; deliver later updates in source order.
- The handle's cancellation closure unregisters the callback.

Pass the source to `CacheService::new_live(Arc::new(source), options)`. Live
configuration is available with default features; `service` remains an empty
compatibility feature. The client retains the source and subscription until
the final client clone is dropped. Equal configurations skip rebuilding;
invalid updates retain the last working topology. Concurrent callbacks are
serialized when applying the topology. The client does not poll configuration.

## Infrastructure adapters

The client has no dependency on `discovery`, Vintage, or a mesh registry.
The local `discovery` crate's optional `memcache` feature owns these adapters:

- `vintage_memcache::VintageCacheServiceConfigSource`: shared group polling,
  per-namespace updates, and subscription cleanup.
- `memcache_config::CacheServiceConfig`: legacy YAML conversion, including
  `crc32` normalization and legacy flag bits.
- `memcache_mesh::MeshConfig`: registry-file endpoint resolution and `connect`.

Replace `CacheService::from_vintage` with the adapter plus `new_live`, and
`CacheService::mesh_with_config` with `MeshConfig::connect(options)`.
For legacy YAML, use the discovery parser: the native parser ignores unknown
fields and does **not** interpret the old `flag` field. Existing sharding
algorithms, including hash aliases and `fishermen`, remain unchanged.

## Validation

```sh
cargo fmt --all -- --check
cargo test --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

Tests use local protocol servers. The `metrics` feature enables physical-access
metrics through `brz-metrics`.

## Releases

CI runs formatting, Clippy, and tests on pushes and pull requests. To publish,
open **Actions → Publish → Run workflow** on `main`. Leave `retry_tag` empty
to create the next `v0.0.x` release. The workflow validates the package, commits
the version, pushes the commit and tag atomically, and publishes to crates.io
using the organization secret `CARGO_REGISTRY_TOKEN`.

If uploading fails after the tag was pushed, retry with that existing tag.
Normal pushes do not publish. Historical tags retain their original package
metadata; use new release tags for registry packages.

Licensed under MIT OR Apache-2.0.
