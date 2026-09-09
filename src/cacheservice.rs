//! Application-supplied cache topology and native configuration parsing.

use std::collections::HashMap;

use serde::Deserialize;

/// Parsed cache-service configuration: a map from namespace name to its
/// configuration block. Unknown YAML fields are ignored.
#[derive(Clone, Debug, Deserialize)]
pub struct CacheServiceConfig(HashMap<String, CacheNamespaceConf>);

impl CacheServiceConfig {
    /// Parse native cache configuration without legacy format conversions.
    pub fn from_yaml_str(yaml: &str) -> Result<Self, CacheServiceError> {
        serde_yaml::from_str(yaml).map_err(|e| CacheServiceError::Yaml(e.to_string()))
    }

    /// Returns the master list for the given namespace, if present.
    pub fn masters_of(&self, namespace: &str) -> Option<&[String]> {
        self.0.get(namespace).map(|c| c.master.as_slice())
    }

    /// Returns the configuration block for the given namespace, if present.
    pub fn namespace(&self, namespace: &str) -> Option<&CacheNamespaceConf> {
        self.0.get(namespace)
    }
}

/// One namespace block within a [`CacheServiceConfig`].
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct CacheNamespaceConf {
    #[serde(default)]
    pub hash: Option<String>,
    #[serde(default)]
    pub distribution: Option<String>,
    #[serde(default)]
    pub master: Vec<String>,
    #[serde(default)]
    pub slave: Vec<String>,
    #[serde(default, rename = "master_l1")]
    pub master_l1: Vec<Vec<String>>,
    #[serde(default, rename = "slave_l1")]
    pub slave_l1: Vec<Vec<String>>,
    #[serde(default)]
    pub exptime: i64,
    #[serde(default)]
    pub timeout_ms_master: u32,
    #[serde(default)]
    pub timeout_ms_slave: u32,
    #[serde(default = "default_true")]
    pub update_slave_l1: bool,
    #[serde(default)]
    pub local_affinity: bool,
    #[serde(default)]
    pub backend_no_storage: bool,
}

impl CacheNamespaceConf {
    pub fn single_master(endpoint: String) -> Self {
        Self {
            hash: None,
            distribution: None,
            master: vec![endpoint],
            slave: Vec::new(),
            master_l1: Vec::new(),
            slave_l1: Vec::new(),
            exptime: 0,
            timeout_ms_master: 0,
            timeout_ms_slave: 0,
            update_slave_l1: true,
            local_affinity: false,
            backend_no_storage: false,
        }
    }

    /// Hashing algorithm (e.g. `crc32`).
    pub fn hash(&self) -> Option<&str> {
        self.hash.as_deref()
    }

    /// Distribution strategy (e.g. `modula`, `ketama`).
    pub fn distribution(&self) -> Option<&str> {
        self.distribution.as_deref()
    }

    /// Master endpoints in configured order (`host:port`).
    pub fn masters(&self) -> &[String] {
        &self.master
    }

    /// Slave endpoints in configured order (`host:port`); empty when the
    /// namespace has no slave pool.
    pub fn slaves(&self) -> &[String] {
        &self.slave
    }

    /// L1 master endpoint groups; each inner list is one group of `host:port`.
    pub fn master_l1(&self) -> &[Vec<String>] {
        &self.master_l1
    }

    /// L1 slave endpoint groups; each inner list is one group of `host:port`.
    pub fn slave_l1(&self) -> &[Vec<String>] {
        &self.slave_l1
    }

    /// Expiration in milliseconds used when a read hit is written back into
    /// the first missed replica group.
    pub fn writeback_expiration_ms(&self) -> i64 {
        self.exptime
    }

    pub fn timeout_ms_master(&self) -> u32 {
        self.timeout_ms_master
    }

    pub fn timeout_ms_slave(&self) -> u32 {
        self.timeout_ms_slave
    }

    pub fn update_slave_l1(&self) -> bool {
        self.update_slave_l1
    }

    pub fn local_affinity(&self) -> bool {
        self.local_affinity
    }

    pub fn backend_no_storage(&self) -> bool {
        self.backend_no_storage
    }
}

fn default_true() -> bool {
    true
}

/// Errors returned by cache-service YAML parsing.
#[derive(Debug, thiserror::Error)]
pub enum CacheServiceError {
    /// The YAML document could not be parsed.
    #[error("cache-service yaml parse error: {0}")]
    Yaml(String),

    /// The namespace identifier was empty or contained control characters.
    #[error("invalid cache-service namespace: {0:?}")]
    InvalidNamespace(String),

    /// The namespace was not present in the parsed YAML document.
    #[error("cache-service namespace not found: {0}")]
    MissingNamespace(String),
}

/// Looks up the master list for `namespace` in a cache-service YAML document.
///
/// Convenience wrapper around [`CacheServiceConfig::from_yaml_str`] +
/// [`CacheServiceConfig::masters_of`]: parses `yaml` and returns the master
/// endpoints for `namespace`, or an error if the namespace is absent.
///
/// `namespace` must be non-empty (after trimming) and contain no control
/// characters; otherwise an [`CacheServiceError::InvalidNamespace`] is returned
/// before parsing.
pub fn masters_from_yaml(yaml: &str, namespace: &str) -> Result<Vec<String>, CacheServiceError> {
    if namespace.trim().is_empty() || namespace.chars().any(char::is_control) {
        return Err(CacheServiceError::InvalidNamespace(namespace.to_string()));
    }
    let cfg = CacheServiceConfig::from_yaml_str(yaml)?;
    cfg.masters_of(namespace)
        .map(|m| m.to_vec())
        .ok_or_else(|| CacheServiceError::MissingNamespace(namespace.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE_YAML: &str = "example-abtest:\n  hash: crc32\n  distribution: modula\n  hash_tag: example-abtest\n  master:\n  - 192.0.4.103:15138\n  - 192.0.0.95:15138\n  - 192.0.0.131:15138\n  - 192.0.0.209:15138\n  slave_l1:\n  - - 192.0.2.203:15138\n    - 192.0.2.204:15138\n";

    #[test]
    fn parses_cache_service_yaml_namespace() {
        let cfg = CacheServiceConfig::from_yaml_str(SAMPLE_YAML).unwrap();
        let ns = cfg.namespace("example-abtest").unwrap();
        // Native configuration preserves the explicitly supplied hash name.
        assert_eq!(ns.hash(), Some("crc32"));
        assert_eq!(ns.distribution(), Some(crate::sharding::DIST_MODULA));
        assert_eq!(
            ns.masters(),
            &[
                "192.0.4.103:15138",
                "192.0.0.95:15138",
                "192.0.0.131:15138",
                "192.0.0.209:15138",
            ]
        );
        assert_eq!(ns.slave_l1().len(), 1);
        assert_eq!(
            ns.slave_l1()[0],
            &["192.0.2.203:15138", "192.0.2.204:15138"]
        );
    }

    #[test]
    fn masters_of_returns_none_for_missing_namespace() {
        let cfg = CacheServiceConfig::from_yaml_str(SAMPLE_YAML).unwrap();
        assert!(cfg.masters_of("nope").is_none());
    }

    #[test]
    fn masters_from_yaml_returns_master_list() {
        let masters = masters_from_yaml(SAMPLE_YAML, "example-abtest").unwrap();
        assert_eq!(
            masters,
            vec![
                "192.0.4.103:15138",
                "192.0.0.95:15138",
                "192.0.0.131:15138",
                "192.0.0.209:15138",
            ]
        );
    }

    #[test]
    fn masters_from_yaml_errors_on_missing_namespace() {
        assert!(matches!(
            masters_from_yaml(SAMPLE_YAML, "missing-namespace"),
            Err(CacheServiceError::MissingNamespace(n)) if n == "missing-namespace"
        ));
    }

    #[test]
    fn masters_from_yaml_rejects_empty_namespace() {
        assert!(matches!(
            masters_from_yaml(SAMPLE_YAML, " \t"),
            Err(CacheServiceError::InvalidNamespace(_))
        ));
    }

    #[test]
    fn ignores_unknown_yaml_fields() {
        // Extra top-level keys and extra fields within a namespace must be
        // silently ignored, not cause a parse failure.
        let yaml = "other-namespace:\n  hash: bkdr\nexample-abtest:\n  master:\n  - 1.2.3.4:15138\n  custom_field: whatever\n  nested:\n    a: 1\n";
        let masters = masters_from_yaml(yaml, "example-abtest").unwrap();
        assert_eq!(masters, vec!["1.2.3.4:15138"]);
    }

    #[test]
    fn masters_defaults_to_empty_when_absent() {
        // A namespace with no `master:` key parses to an empty master list
        // (serde default), not an error.
        let yaml = "example-abtest:\n  hash: crc32\n";
        let cfg = CacheServiceConfig::from_yaml_str(yaml).unwrap();
        assert_eq!(cfg.masters_of("example-abtest").unwrap(), &[] as &[String]);
    }

    #[test]
    fn malformed_yaml_is_an_error() {
        assert!(matches!(
            CacheServiceConfig::from_yaml_str("not: [valid: yaml"),
            Err(CacheServiceError::Yaml(_))
        ));
    }
}
