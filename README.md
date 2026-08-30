# memcache

统一通过 `CacheService` 访问 memcached。所有模式共用 `brz-net` 的单连接
session、协议解析、超时和快速失败实现。

## 单节点

`single` 是一个固定的 CacheService：只有一个 master replica，并且 master
只有一个 shard/node。

```rust
use bytes::Bytes;
use memcache::{CacheService, Memcache};

# async fn demo() -> memcache::Result<()> {
let cache = CacheService::single("127.0.0.1:11211").await?;
cache.set("key", Bytes::from_static(b"value")).await?;
let value = cache.get("key").await?;
# Ok(())
# }
```

## Mesh

`mesh` 构造时从 Breeze socks 注册目录发现一次本地 TCP 端口，随后按
`single(localhost:port)` 的固定拓扑运行。不支持 Unix socket，也不监听注册
文件的后续变化。

```rust
use memcache::{CacheService, Memcache};

# async fn demo() -> memcache::Result<()> {
let cache = CacheService::mesh("cache.service.group", "namespace").await?;
let value = cache.get("key").await?;
# Ok(())
# }
```

测试或基础设施代码可通过 `MeshConfig` 指定注册目录、协议和超时：

```rust
use memcache::{CacheService, CacheServiceOptions, MeshConfig, Protocol};

# async fn demo() -> memcache::Result<()> {
let config = MeshConfig::new("namespace")
    .with_group("cache.service.group")
    .with_socket_dir("/data1/breeze/socks")
    .with_protocol(Protocol::Binary);
let cache = CacheService::mesh_with_config(config, CacheServiceOptions::default()).await?;
# Ok(())
# }
```

## CacheService 配置

- `CacheService::new(conf, options)`：固定的 replica/shard 拓扑。
- `CacheService::new_live(source, options)`：由配置源推送运行时更新。
- `CacheService::from_vintage(...)`：使用 Vintage live config；需要 `service`
  feature。

`DirectClient`、`SidecarClient`、旧连接池和旧 service template 已移除。
