# memcache

A high-performance, async **memcached client for Rust**, implementing both the
**text (ASCII)** and **binary** wire protocols. It targets the breeze *byMesh*
deployment shape: the client connects to a single memcached endpoint (a mesh
sidecar over TCP or a unix socket) and uses a bounded connection pool for
concurrency and availability.

The public API mirrors breeze-sdk-core's `CacheAble` interface
(`MeshMemcacheTemplate` / `TextMemcacheClient` / `PingPongMemcachedBinaryClient`).

## Features

- `async` / [Tokio](https://tokio.rs) throughout, built on [`bytes`](https://docs.rs/bytes).
- Text **and** binary protocols, selectable per client.
- Connection pooling via [`deadpool`](https://docs.rs/deadpool) with
  request/response correlation: the binary protocol matches each response to
  its request by opaque token, and connections that fail, time out, or show a
  desynced frame are dropped instead of being recycled.
- TCP (`host:port`) and unix-socket endpoints.
- Per-operation timeouts and key validation.
- Java-compatible value flag markers (int/long/bool/string), so values are
  interoperable with the `cn.vika.memcached` / `com.schooner.MemCached` clients.

## Usage

```rust
use memcache::{Client, Config, Protocol};
use std::time::Duration;

#[tokio::main]
async fn main() -> memcache::Result<()> {
    // Binary protocol over TCP (the default protocol is Binary).
    let config = Config::tcp("127.0.0.1", 11211)
        .with_protocol(Protocol::Binary)
        .with_max_connections(64)
        .with_op_timeout(Duration::from_millis(400));
    let client = Client::new(config)?;

    // Store and fetch.
    client.set("greeting", "hello", 60u32).await?;      // expire in 60s
    let value = client.get("greeting").await?;
    assert_eq!(value.unwrap().as_string()?, "hello");

    // Conditional stores.
    client.add("greeting", "ignored", 60u32).await?;    // false: already exists
    client.replace("greeting", "hi", 60u32).await?;     // true

    // Compare-and-swap.
    if let Some(cas) = client.get_cas("greeting").await? {
        let next = memcache::CasValue::new(memcache::Value::new("hiya", 0), cas.cas);
        client.cas("greeting", &next, 60u32).await?;
    }

    // Counters and multi-get.
    client.set("n", "10", 0u32).await?;
    assert_eq!(client.incr("n", 5).await?, Some(15));
    let many = client.get_multi(&["greeting", "n"]).await?;

    client.delete("greeting").await?;
    Ok(())
}
```

Use the text protocol or a unix socket by adjusting the config:

```rust
use memcache::{Config, Protocol};

let text = Config::tcp("127.0.0.1", 11211).with_protocol(Protocol::Text);
let uds  = Config::unix("/var/run/mc.sock").with_protocol(Protocol::Binary);
```

## byMesh: connecting via the socks registry

In the mesh deployment the sidecar advertises each backend as a registry file
under `/tmp/breeze/socks/` (`DEFAULT_SOCKS_DIR`). For memcached the file name is:

```text
config.example.com+3+config+v1+<group>+all:<namespace>@mc:<port>@cs
```

The SDK parses this name **directly** — no remote/vintage fetch. Parsing follows
breeze's `context::Quadruple`: split the name by `@` into `service@protocol@backend`,
then split the protocol by `:`; a numeric port means TCP `127.0.0.1:<port>`,
otherwise it is a sibling `<token>.sock` unix socket.

```rust
use memcache::Config;

// Parse a specific sock file directly (full path or bare name):
let a = Config::sock(
    "/tmp/breeze/socks/config.example.com+3+config+v1+\
     cache.service.friendship.pool.yf+all:relation_cluster_exposure@mc:9461@cs",
)?;

// Or discover it by group + namespace under the default socks dir:
let b = Config::mesh("cache.service.friendship.pool.yf", "relation_cluster_exposure")?;

// Custom socks directory:
let c = Config::mesh_in("/data1/breeze/socks", "grp", "ns")?;
# Ok::<(), memcache::Error>(())
```

These constructors default to the binary protocol, matching the mesh
`PingPongMemcachedBinaryClient`.

### Endpoint rediscovery

Mesh ports are normally fixed per service, but occasionally reassigned. When
the config was built from the socks registry (`Config::mesh` / `mesh_in` /
`sock`), the client keeps the discovery coordinates and follows registry
changes: new connections dial the new endpoint and idle connections to the
old one are drained.

Scanning is **shared per directory**: all clients watching the same socks
directory share a single background task and a single scan every 5 seconds,
so a process with hundreds of namespaces costs one timer and one directory
read, not one per namespace. The scan task starts on first use and stops when
the last client watching the directory is dropped. Clients are only woken
when the registry snapshot actually changes.

`Client::refresh_endpoint()` forces a rescan on demand (e.g. after a burst of
connect failures) and `Client::current_endpoint()` reports the endpoint
currently in use.

## API

The client exposes the `CacheAble`-equivalent surface plus a few extras:

| Method | Description |
| --- | --- |
| `get` / `get_multi` / `get_cas` | fetch value(s), optionally with CAS token |
| `set` / `set_with_noreply` | store unconditionally |
| `add` / `replace` | store if absent / if present |
| `append` / `prepend` | extend an existing value |
| `cas` | store only if the CAS token still matches |
| `delete` / `delete_with_noreply` | remove a key |
| `incr` / `decr` | atomic counter update |
| `touch` | update expiration only |
| `flush_all` / `version` | server admin |

## Expiration

`Expiration` follows memcached semantics: `0` = never, values up to 30 days are
relative seconds, larger values are absolute unix timestamps. Any
`impl Into<Expiration>` is accepted, including `u32` seconds and
`std::time::Duration`.

## Values and flags

Stored values are bytes plus a `u32` flags field. `ToMemcacheValue` is
implemented for `&[u8]` / `Vec<u8>` / `Bytes` / `&str` / `String` (stored
untagged) and for `i32` / `i64` / `u64` / `bool` (tagged with the Java marker
bits). `Value` offers `as_bytes` / `as_string` / `as_i64` / `as_u64` / `as_bool`
decoders.

## Logging

Request exceptions are logged at `error` level via [`tracing`], matching the
mesh `MeshMemcacheTemplate` format:

```text
mc mesh <method> error ,namespace:<namespace> ,key: <key>
```

Set the namespace with `Config::with_namespace` (or `Config::mesh`, which sets
it automatically) so the logs identify the client. Install any `tracing`
subscriber to route these logs to your sink.

## Not implemented (out of scope)

- Client-side multi-server sharding / consistent hashing / failover (the *byTcp*
  path). High availability is provided by the pool and the mesh sidecar.
- QuickLZ compression (`F_COMPRESSED`) and Java object serialization
  (`F_SERIALIZED`): such values are surfaced as `Error::Unsupported` on decode.
- SASL authentication and UDP transport.

## Development

```bash
cargo fmt --check
cargo test
cargo clippy --all-targets -- -D warnings
```

The test suite includes an in-process fake memcached server that exercises the
full request/response path for both protocols; no external server is required.
