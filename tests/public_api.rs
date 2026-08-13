use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use memcache::{
    CacheNamespaceConf, CacheService, CacheServiceConfig, CacheServiceFactory, Memcache, Result,
};

struct ParsedConfigFactory {
    yaml: &'static str,
    namespace: &'static str,
    loads: Arc<AtomicUsize>,
}

#[async_trait]
impl CacheServiceFactory for ParsedConfigFactory {
    async fn load(&self) -> Result<CacheNamespaceConf> {
        self.loads.fetch_add(1, Ordering::Relaxed);
        let config = CacheServiceConfig::from_yaml_str(self.yaml)?;
        config.namespace(self.namespace).cloned().ok_or_else(|| {
            memcache::CacheServiceError::MissingNamespace(self.namespace.to_owned()).into()
        })
    }
}

#[tokio::test]
async fn cache_service_is_constructed_from_the_public_factory_contract() {
    let loads = Arc::new(AtomicUsize::new(0));
    let factory = ParsedConfigFactory {
        yaml: "users:\n  hash: crc32\n  distribution: modula\n  master:\n  - 127.0.0.1:11211\n",
        namespace: "users",
        loads: loads.clone(),
    };

    let cache: Arc<dyn Memcache> = Arc::new(CacheService::new(factory).await.unwrap());

    assert_eq!(loads.load(Ordering::Relaxed), 1);
    drop(cache);
}
