//! Vintage integration: build the template's backup cache from a Vintage
//! statics-config group, and watch for config changes to hot-swap it.
//!
//! This mirrors the Java `CacheServiceRecoveryConfigNotifer` flow: the Java
//! template subscribes to `(group, namespace)` and rebuilds its backup cache
//! when the cache-service config changes. Here the watcher polls
//! [`vintage::Client::lookup_config`] and, when the content sign changes,
//! rebuilds the sharded backup and atomically swaps it in via
//! [`MemcacheServiceTemplate::set_backup`]. The old backup is dropped once
//! in-flight operations release it — the Java version needed a scheduled
//! delayed destroy for this; `Arc` handles it naturally.

use std::sync::Arc;
use std::time::Duration;

use crate::cacheservice::{CacheServiceConfig, CacheServiceError};
use tokio::task::JoinHandle;
use tracing::{info, warn};

use super::{Cacheable, MemCacheTemplate, MemcacheServiceTemplate, PoolOptions};

/// Errors from building a backup cache from Vintage statics config.
#[derive(Debug, thiserror::Error)]
pub enum VintageSourceError {
    /// The Vintage lookup itself failed.
    #[error("vintage lookup failed: {0}")]
    Vintage(#[from] vintage::Error),

    /// The lookup response carried no `key="all"` YAML entry.
    #[error("vintage statics config for group {group:?} has no \"all\" entry")]
    MissingAllEntry {
        /// The statics-config group that was looked up.
        group: String,
    },

    /// The cache-service YAML could not be parsed or the namespace is absent.
    #[error("cache-service config error: {0}")]
    CacheService(#[from] CacheServiceError),

    /// The backup clients could not be built.
    #[error("memcache client build error: {0}")]
    Memcache(#[from] crate::Error),
}

/// Builds a multi-tier backup cache from a Vintage statics-config group.
///
/// Looks up `group`, parses the `key="all"` YAML with
/// [`crate::cacheservice`], and builds a [`MemCacheTemplate`] over the
/// namespace's master / slave / L1 pools (routing with the namespace's
/// hash/distribution), like the Java `createBackupCache`.
pub async fn backup_from_vintage(
    client: &vintage::Client,
    group: &str,
    namespace: &str,
) -> Result<Arc<dyn Cacheable>, VintageSourceError> {
    backup_from_vintage_with(client, group, namespace, PoolOptions::default()).await
}

/// [`backup_from_vintage`] with explicit per-pool connection tuning (the
/// Java `minConnections` / `maxConnections`).
pub async fn backup_from_vintage_with(
    client: &vintage::Client,
    group: &str,
    namespace: &str,
    options: PoolOptions,
) -> Result<Arc<dyn Cacheable>, VintageSourceError> {
    let snapshot = client.lookup_config(group).await?;
    let yaml = snapshot
        .all_entry_value()
        .ok_or_else(|| VintageSourceError::MissingAllEntry {
            group: group.to_string(),
        })?;
    backup_from_yaml(yaml, namespace, options)
}

fn backup_from_yaml(
    yaml: &str,
    namespace: &str,
    options: PoolOptions,
) -> Result<Arc<dyn Cacheable>, VintageSourceError> {
    let conf = CacheServiceConfig::from_yaml_str(yaml)?
        .namespace(namespace)
        .ok_or_else(|| CacheServiceError::MissingNamespace(namespace.to_string()))?
        .clone();
    Ok(Arc::new(MemCacheTemplate::from_namespace_conf(
        &conf, options,
    )?))
}

/// Spawns a background task that watches the Vintage statics-config group
/// and hot-swaps the template's backup cache when the config changes.
///
/// Polls every `interval`; when the config content sign changes, the backup
/// is rebuilt from the new YAML and swapped in. Lookup or rebuild failures
/// are logged and the last-known-good backup is retained.
///
/// Dropping the returned handle detaches the task; abort it to stop watching.
pub fn spawn_config_watcher(
    client: vintage::Client,
    group: impl Into<String>,
    namespace: impl Into<String>,
    template: MemcacheServiceTemplate,
    interval: Duration,
) -> JoinHandle<()> {
    let group = group.into();
    let namespace = namespace.into();
    tokio::spawn(async move {
        let mut last_sign: Option<String> = None;
        loop {
            tokio::time::sleep(interval).await;
            let snapshot = match client.lookup_config(&group).await {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    warn!(group, %error, "vintage config watcher lookup failed; retaining backup");
                    continue;
                }
            };
            let sign = snapshot.sign().to_string();
            if last_sign.as_deref() == Some(sign.as_str()) {
                continue;
            }
            let rebuild = snapshot
                .all_entry_value()
                .ok_or_else(|| VintageSourceError::MissingAllEntry {
                    group: group.clone(),
                })
                .and_then(|yaml| backup_from_yaml(yaml, &namespace, PoolOptions::default()));
            match rebuild {
                Ok(backup) => {
                    template.set_backup(backup);
                    last_sign = Some(sign);
                    info!(
                        group,
                        namespace, "backup cache rebuilt from vintage config change"
                    );
                }
                Err(error) => {
                    warn!(group, namespace, %error, "backup rebuild failed; retaining old backup");
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE_YAML: &str = "example-abtest:\n  hash: crc32\n  distribution: modula\n  master:\n  - 127.0.0.1:21211\n  - 127.0.0.1:21212\n";

    #[tokio::test]
    async fn builds_backup_from_yaml() {
        // Direct clients connect lazily enough that construction succeeds
        // without a live backend (but need a tokio runtime for the pool
        // maintenance task).
        let backup = backup_from_yaml(SAMPLE_YAML, "example-abtest", PoolOptions::default());
        assert!(backup.is_ok());
    }

    #[test]
    fn missing_namespace_is_an_error() {
        assert!(matches!(
            backup_from_yaml(SAMPLE_YAML, "nope", PoolOptions::default()),
            Err(VintageSourceError::CacheService(
                CacheServiceError::MissingNamespace(n)
            )) if n == "nope"
        ));
    }

    #[test]
    fn empty_master_list_is_an_error() {
        let yaml = "ns:\n  hash: crc32\n";
        assert!(matches!(
            backup_from_yaml(yaml, "ns", PoolOptions::default()),
            Err(VintageSourceError::Memcache(_))
        ));
    }

    #[test]
    fn malformed_yaml_is_an_error() {
        assert!(matches!(
            backup_from_yaml("not: [valid: yaml", "ns", PoolOptions::default()),
            Err(VintageSourceError::CacheService(CacheServiceError::Yaml(_)))
        ));
    }
}
