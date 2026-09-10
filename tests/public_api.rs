use std::sync::Arc;

use brz_memcache::{
    CacheNamespaceConf, CacheService, CacheServiceConfig, CacheServiceOptions, Memcache,
};

#[tokio::test]
async fn cache_service_is_constructed_from_a_namespace_conf() {
    let yaml = "users:\n  hash: crc32\n  distribution: modula\n  master:\n  - 127.0.0.1:11211\n";
    let conf: CacheNamespaceConf = CacheServiceConfig::from_yaml_str(yaml)
        .unwrap()
        .namespace("users")
        .unwrap()
        .clone();

    let cache: Arc<dyn Memcache> = Arc::new(
        CacheService::new(conf, CacheServiceOptions::default())
            .await
            .unwrap(),
    );

    drop(cache);
}
