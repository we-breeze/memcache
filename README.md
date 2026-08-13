# memcache

A high-performance, high-availability **async memcached client** (Rust /
tokio) for the breeze platform, with **two explicitly separated access
modes**:

- **Mesh mode** ([`sidecar`]) — talk to the local breeze mesh agent,
  discovered from the sock registry files it publishes; the mesh proxies to
  the real backends and owns sharding and failover. (The *byMesh* path.)
- **Direct backend mode** ([`direct`]) — connect to memcached backends
  directly, with client-side shard routing using the same hash/distribution
  algorithms as the mesh. (The *byTcp* path.)
- **Cache-service templates** ([`service`], feature `service`) — a Rust port
  of the Java `commons-memcache` templates: `MemcacheServiceTemplate`
  (primary cache-service client with backup fallback) and `MemCacheTemplate`
  (multi-tier master / slave / L1 backup), with Vintage statics-config
  integration for building and hot-swapping the backup.

Both wire protocols (text and binary) are implemented; binary is the
default, matching the mesh `PingPongMemcachedBinaryClient`. The low-level
clients retain the broader breeze-sdk-core operation surface, while the
application API intentionally starts with only `get` and `set`.

## Application API

Application code can depend only on the root `Memcache` trait. With the
`service` feature, the SDK-provided `VintageCacheServiceFactory` owns the
Vintage lookup, YAML parsing, namespace selection, and topology construction:

```rust
use bytes::Bytes;
use memcache::{CacheService, Memcache, VintageCacheServiceFactory};

# async fn demo(vintage_client: vintage::Client) -> memcache::Result<()> {
let factory = VintageCacheServiceFactory::new(
    vintage_client,
    "cache.service.feedcontent.pool.yf",
    "example-abtest",
);
let cache = CacheService::new(factory).await?;

cache.set("user:42", Bytes::from_static(b"value")).await?;
let entry = cache.get("user:42").await?;
# Ok(())
# }
```

The factory is loaded once during construction. This version does not update
an existing `CacheService` when the Vintage configuration changes.

For a resource exposed directly by the local breeze sidecar, use the same
application contract without a factory:

```rust
use bytes::Bytes;
use memcache::{Memcache, SidecarMemcache};

# async fn demo() -> memcache::Result<()> {
let cache = SidecarMemcache::new(
    "cache.service.friendship.pool.yf",
    "relation_cluster_exposure",
)?;
cache.set("user:42", Bytes::from_static(b"value")).await?;
# Ok(())
# }
```

## Mesh mode (sidecar)

```rust
use memcache::sidecar::SidecarClient;

# async fn demo() -> memcache::Result<()> {
// Connect to the mesh for a resource namespace.
let client = SidecarClient::connect("my_mc_namespace")?;

client.set("greeting", "hello", 60u32).await?;      // expire in 60s
let value = client.get("greeting").await?;
assert_eq!(value.unwrap().as_string()?, "hello");

// Conditional stores and CAS.
client.add("greeting", "ignored", 60u32).await?;    // false: already exists
if let Some(cas) = client.get_cas("greeting").await? {
    let next = memcache::CasValue::new(memcache::Value::new("hiya", 0), cas.cas);
    client.cas("greeting", &next, 60u32).await?;
}

// Counters and multi-get.
client.set("n", "10", 0u32).await?;
assert_eq!(client.incr("n", 5).await?, Some(15));
let many = client.get_multi(&["greeting", "n"]).await?;
# Ok(())
# }
```

Custom configuration (group, socket dir, protocol, pool size):

```rust
use memcache::sidecar::{MeshConfig, SidecarClient};
use memcache::Protocol;

# fn demo() -> memcache::Result<()> {
let cfg = MeshConfig::new("my_ns")
    .with_group("cache.service.friendship.pool.yf")
    .with_protocol(Protocol::Binary)
    .with_min_connections(2)     // default; established at startup, kept topped up
    .with_max_connections(128);  // default; pool grows on demand up to this
let client = SidecarClient::from_config(cfg)?;
# Ok(())
# }
```

### Discovery and endpoint rediscovery

The mesh advertises each resource as a registry file under
`/data1/breeze/socks/` (`sidecar::DEFAULT_SOCKS_DIR`):

```text
config.example.com+3+config+v1+<group>+all:<namespace>@mc:<port>@cs
```

The SDK parses this name **directly** — no remote/vintage fetch. A numeric
port means TCP `<host>:<port>`; `host` comes from a non-blank
`MESH_CONNECT_HOST`, falling back to `127.0.0.1`. This matches breeze's
traffic-e2e interception convention and accepts hostnames such as
`breeze.recording`. Non-numeric slots and Unix sockets are unsupported.
`SidecarClient::from_sock(path)` parses one specific registry file.

Mesh ports are normally fixed per service, but occasionally reassigned. The
client follows registry changes: new connections dial the new endpoint and
idle connections to the old one are drained. Scanning is **shared per
directory** — all clients watching the same socks directory share one
background task and one scan per 5 seconds, so a process with hundreds of
namespaces costs one timer and one directory read, not one per namespace.
`SidecarClient::refresh_endpoint()` forces a rescan on demand and
`SidecarClient::current_endpoint()` reports the endpoint in use.

## Direct backend mode

No mesh: the SDK connects to the backends directly and routes keys itself.
`Shards` uses the same hash/distribution algorithms as the breeze mesh
(`direct::sharding`, ported from `breeze/sharding`), so a key maps to the
same backend whether routed by this client or by the mesh:

```rust
use memcache::direct::{DirectClient, ServerConfig, Shards};

# fn demo() -> memcache::Result<()> {
let shards = Shards::new(
    "crc32", "modula",
    vec!["10.0.0.1:11211".to_string(), "10.0.0.2:11211".to_string()],
    vec![
        DirectClient::connect(ServerConfig::new("10.0.0.1:11211")?)?,
        DirectClient::connect(ServerConfig::new("10.0.0.2:11211")?)?,
    ],
);
// shardingSupport.getClient(key) style:
# async fn run(shards: Shards) -> memcache::Result<()> {
shards.get_client("u:42").set("u:42", "data", 60u32).await?;
# Ok(())
# }
# Ok(())
# }
```

## Unified proxy

`memcache::Client` is an enum over the sidecar and direct clients, exposing
the whole operation surface regardless of access mode:

```rust
use memcache::Client;
use memcache::sidecar::SidecarClient;

# async fn demo(direct: memcache::direct::DirectClient) -> memcache::Result<()> {
let sidecar = SidecarClient::connect("my_ns")?;
let clients: Vec<Client> = vec![sidecar.into(), direct.into()];
for client in &clients {
    let _ = client.get("key").await?;
}
# Ok(())
# }
```

## API

| Method | Description |
| --- | --- |
| `get` / `get_multi` / `get_cas` | fetch value(s), optionally with CAS token |
| `set` | store unconditionally |
| `add` / `replace` | store if absent / if present |
| `append` / `prepend` | extend an existing value |
| `cas` | store only if the CAS token still matches |
| `delete` | remove a key |
| `incr` / `decr` | atomic counter update |
| `touch` | update expiration only |
| `flush_all` / `version` | server admin |

## Availability and pooling (pooled modes)

- Connection pooling via [`deadpool`](https://docs.rs/deadpool) with
  request/response correlation: the binary protocol matches each response to
  its request by opaque token, and connections that fail, time out, or show
  a desynced frame are dropped instead of being recycled.
- Pool floor: a single shared background task keeps every client topped up
  to `min_connections` (not one task per client), so long-idle clients do
  not pay reconnect latency when traffic resumes.
- TCP keepalive reaps half-open connections to a crashed mesh before they
  can stall a request.
- Per-operation timeouts and key validation.
- Java-compatible value flag markers (int/long/bool/string), so values are
  interoperable with the `cn.vika.memcached` / `com.schooner.MemCached`
  clients.

## Expiration

`Expiration` follows memcached semantics: `0` = never, values up to 30 days
are relative seconds, larger values are absolute unix timestamps. Any
`impl Into<Expiration>` is accepted, including `u32` seconds and
`std::time::Duration`.

## Values and flags

Stored values are bytes plus a `u32` flags field. `ToMemcacheValue` is
implemented for `&[u8]` / `Vec<u8>` / `Bytes` / `&str` / `String` (stored
untagged) and for `i32` / `i64` / `u64` / `bool` (tagged with the Java marker
bits). `Value` offers `as_bytes` / `as_string` / `as_i64` / `as_u64` /
`as_bool` decoders.

## Logging

Request exceptions are logged at `error` level via [`tracing`], matching the
mesh `MeshMemcacheTemplate` format:

```text
mc mesh <method> error ,namespace:<namespace> ,key: <key>
```

Set the namespace with `Config::with_namespace` (or `MeshConfig`, which sets
it automatically) so the logs identify the client. Install any `tracing`
subscriber to route these logs to your sink.

## Not implemented (out of scope)

- QuickLZ compression (`F_COMPRESSED`) and Java object serialization
  (`F_SERIALIZED`): such values are surfaced as `Error::Unsupported` on
  decode.
- SASL authentication and UDP transport.

## Development

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
```

The test suite includes an in-process fake memcached server that exercises
the full request/response path for both protocols; no external server is
required.

## Cache-service templates (feature `service`)

`memcache::service` ports the Java `commons-memcache` templates:

- `Cacheable` — the async trait analogue of the Java `CacheAble<T>`;
  implemented by `memcache::Client`, `ShardedCache`, and `MemCacheTemplate`.
- `MemcacheServiceTemplate` — routes operations to a primary cache-service
  client with automatic backup fallback, process-wide and per-template
  switches, and split `get_multi` (300-key shards, 200ms assist window).
- `MemCacheTemplate` — the multi-tier (master / slave / master-L1 /
  slave-L1) backup cache: L1→master→slave read cascade with set-back,
  policy-driven write fan-out (`writeAll` / `writeAndDeleteL1` /
  `writeAndIfExistL1`), and the Java runtime switches (`read_only`,
  `update_master_l1`, `update_slave_l1`, `force_write_all`, ...).
- `service::vintage` — builds the backup from a Vintage statics-config
  group (`backup_from_vintage`) and hot-swaps it on config sign changes
  (`spawn_config_watcher`).

```bash
cargo test --features service
# YAML/snapshot config tests against a live Vintage snapshot:
BRZ_MCS_SNAPSHOT=/tmp/breeze/snapshot/<snapshot-file> cargo test --features service --test yaml_config
# Docker-based end-to-end tests (memcached image, BRZ_MC_IMAGE to override):
cargo test --features service --test yaml_integration -- --ignored
```

## Load testing

`tools/mc-bench` is a load-test harness mirroring the redis SDK's
`redis-bench`: fixed-shape workloads (get / getmulti / set / incr) over
`--concurrency` async workers, with throughput, p50/p95/p99 latency,
per-request allocation accounting (via `brz-mem`), reply verification
(`--verify` detects request/response mixups), and TCP fault injection
(`--slow-rate`, `--timeout-rate`, `--reset-rate`, `--outage-ms`,
client-side `--cpu-stall-rate`). It drives the sidecar, direct, sharded, and
cache-service access modes:

```bash
cargo run -p mc-bench --release -- --namespace my_ns -c 64 -n 1000000 get
cargo run -p mc-bench --release -- --direct 127.0.0.1:11211 -c 64 set
cargo run -p mc-bench --release -- --shards 10.0.0.1:11211,10.0.0.2:11211 get
```
