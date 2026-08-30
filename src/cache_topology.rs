//! reference_client-compatible CacheService topology over `brz-net` sessions.

use std::{
    collections::{HashMap, HashSet},
    net::{SocketAddr, SocketAddrV4},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use brz_net::{Node, NodeOptions, QuotaBalancerOptions, QuotaSelector, QuotaTicket};
use futures_util::future::join_all;
use rand::{Rng, seq::SliceRandom};

use crate::{
    CacheNamespaceConf, CacheServiceOptions, Error, Expiration, Protocol, Result,
    service::Cacheable,
    session_protocol::{
        MemcacheProtocol, MemcacheRequest, MemcacheResponse, WriteResponse, map_session_error,
        validate_key,
    },
    sharding::{DIST_MODULA, HASH_CRC32, Sharding},
    value::{CasValue, Value},
};

const MIN_TIMEOUT_MS: u64 = 20;
const MAX_TIMEOUT_MS: u64 = 6_000;

#[derive(Clone, Eq, Hash, PartialEq)]
struct NodeKey {
    group: Box<[String]>,
    shard_index: usize,
    endpoint: SocketAddrV4,
    binary: bool,
    request_timeout: Duration,
}

struct ShardGroup {
    sharding: Sharding,
    nodes: Arc<[Node<MemcacheProtocol>]>,
}

impl ShardGroup {
    #[inline]
    fn node(&self, key: &str) -> &Node<MemcacheProtocol> {
        let index = if self.nodes.len() == 1 {
            0
        } else {
            self.sharding.shard_idx(key.as_bytes())
        };
        debug_assert!(index < self.nodes.len());
        &self.nodes[index]
    }
}

enum ReadSelector {
    Quota(QuotaSelector),
    Batched {
        cursor: AtomicUsize,
        replicas: usize,
    },
}

struct ReadSelection {
    index: usize,
    quota: Option<QuotaTicket>,
}

impl ReadSelector {
    #[inline]
    fn select(&self) -> ReadSelection {
        match self {
            Self::Quota(selector) => {
                let ticket = selector.select();
                ReadSelection {
                    index: ticket.index(),
                    quota: Some(ticket),
                }
            }
            Self::Batched { cursor, replicas } => ReadSelection {
                // This intentionally matches reference_client: one group is retained
                // for 1024 requests when local-affinity quota mode is off.
                index: (cursor.fetch_add(1, Ordering::Relaxed) >> 10) % replicas,
                quota: None,
            },
        }
    }
}

/// Concrete production backend used by [`crate::CacheService`].
pub(crate) struct CacheTopology {
    groups: Arc<[ShardGroup]>,
    local_len: usize,
    selector: ReadSelector,
    writer_indices: Arc<[usize]>,
    writeback_expiration: Expiration,
    backend_no_storage: bool,
    nodes: HashMap<NodeKey, Node<MemcacheProtocol>>,
}

impl std::fmt::Debug for CacheTopology {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CacheTopology")
            .field("groups", &self.groups.len())
            .field("local_len", &self.local_len)
            .field("writer_indices", &self.writer_indices)
            .finish_non_exhaustive()
    }
}

impl CacheTopology {
    pub(crate) fn from_namespace_conf(
        conf: &CacheNamespaceConf,
        options: CacheServiceOptions,
        previous: Option<&Self>,
    ) -> Result<Self> {
        let (specs, local_len, writer_indices) = group_specs(conf, options.update_master_l1)?;
        let hash = conf.hash().unwrap_or(HASH_CRC32);
        let distribution = conf.distribution().unwrap_or(DIST_MODULA);
        let master_timeout = configured_timeout(conf.timeout_ms_master(), options.master_timeout);
        let slave_timeout = configured_timeout(conf.timeout_ms_slave(), options.slave_timeout);
        let mut nodes = HashMap::new();
        let mut groups = Vec::with_capacity(specs.len());

        for (group_index, endpoints) in specs.iter().enumerate() {
            let timeout = if group_index == 0 {
                master_timeout
            } else {
                slave_timeout
            };
            let group_key: Box<[String]> = endpoints.clone().into_boxed_slice();
            let mut group_nodes = Vec::with_capacity(endpoints.len());
            for (shard_index, endpoint) in endpoints.iter().enumerate() {
                let endpoint = endpoint.parse::<SocketAddrV4>().map_err(|_| {
                    Error::Client(format!(
                        "CacheService backend must be an IPv4 socket address: {endpoint}"
                    ))
                })?;
                let key = NodeKey {
                    group: group_key.clone(),
                    shard_index,
                    endpoint,
                    binary: options.protocol == Protocol::Binary,
                    request_timeout: timeout,
                };
                let node = previous
                    .and_then(|topology| topology.nodes.get(&key))
                    .cloned()
                    .map(Ok)
                    .unwrap_or_else(|| {
                        Node::new(
                            SocketAddr::V4(endpoint),
                            MemcacheProtocol::new(options.protocol),
                            NodeOptions {
                                request_timeout: timeout,
                                connect_timeout: options.connect_timeout,
                                ..NodeOptions::default()
                            },
                        )
                    })
                    .map_err(|error| Error::Client(error.to_string()))?;
                group_nodes.push(node.clone());
                nodes.insert(key, node);
            }
            groups.push(ShardGroup {
                sharding: Sharding::new(hash, distribution, endpoints),
                nodes: group_nodes.into(),
            });
        }

        let initial = rand::thread_rng().gen_range(0..local_len);
        let selector = if conf.local_affinity() {
            ReadSelector::Quota(
                QuotaSelector::with_initial(
                    local_len,
                    QuotaBalancerOptions {
                        quota: options.replica_quota,
                        failure_penalty: options.failure_penalty,
                    },
                    initial,
                )
                .map_err(|error| Error::Client(error.to_string()))?,
            )
        } else {
            // reference_client stores the random group index directly, then applies
            // `fetch_add >> 10`; preserve that established behavior.
            ReadSelector::Batched {
                cursor: AtomicUsize::new(initial),
                replicas: local_len,
            }
        };

        let writeback_ms = conf.writeback_expiration_ms().max(0) as u64;
        let writeback_expiration =
            Expiration::from((writeback_ms / 1_000).min(u64::from(u32::MAX)) as u32);
        Ok(Self {
            groups: groups.into(),
            local_len,
            selector,
            writer_indices: writer_indices.into(),
            writeback_expiration,
            backend_no_storage: conf.backend_no_storage(),
            nodes,
        })
    }

    async fn request_at(
        &self,
        group_index: usize,
        key: &str,
        request: MemcacheRequest,
        quota: Option<QuotaTicket>,
    ) -> Result<MemcacheResponse> {
        let future = match self.groups[group_index].node(key).request(request) {
            Ok(future) => future,
            Err(error) => {
                if let Some(quota) = quota {
                    quota.failure();
                }
                return Err(map_session_error(error));
            }
        };
        match future.await {
            Ok(response) => {
                if let Some(quota) = quota {
                    // A miss or server status is still a valid response.
                    quota.success();
                }
                Ok(response)
            }
            Err(error) => {
                if let Some(quota) = quota {
                    quota.failure();
                }
                Err(map_session_error(error))
            }
        }
    }

    fn second_read_group(&self, first: usize) -> Option<usize> {
        if first != 0 {
            return Some(0);
        }
        if self.local_len > 1 {
            return Some(1);
        }
        (self.backend_no_storage && self.groups.len() > 1).then_some(1)
    }

    async fn retrieve(&self, key: &str, master_first: bool) -> Result<Option<CasValue>> {
        validate_key(key.as_bytes())?;
        let selected = self.selector.select();
        let first_index = if master_first { 0 } else { selected.index };
        let first = self
            .request_at(
                first_index,
                key,
                if master_first {
                    MemcacheRequest::gets(key)
                } else {
                    MemcacheRequest::get(key)
                },
                selected.quota,
            )
            .await
            .and_then(MemcacheResponse::into_get);
        if matches!(first, Ok(Some(_))) {
            return first;
        }

        let Some(second_index) = self.second_read_group(first_index) else {
            return first;
        };
        let second = self
            .request_at(
                second_index,
                key,
                if master_first {
                    MemcacheRequest::gets(key)
                } else {
                    MemcacheRequest::get(key)
                },
                None,
            )
            .await?
            .into_get()?;
        if let Some(value) = second.as_ref() {
            self.submit_best_effort(
                first_index,
                key,
                MemcacheRequest::set(key, value.value.clone(), self.writeback_expiration),
            );
        }
        Ok(second)
    }

    fn submit_best_effort(&self, group_index: usize, key: &str, request: MemcacheRequest) {
        // Dropping the future does not cancel the admitted request. The node
        // continues decoding its response and releases the completion slot,
        // avoiding one spawned task per propagated write.
        let _ = self.groups[group_index].node(key).request(request);
    }

    fn fanout_after(&self, position: usize, key: &str, request: &MemcacheRequest) {
        let request = request.fanout();
        for &group_index in &self.writer_indices[position + 1..] {
            self.submit_best_effort(group_index, key, request.clone());
        }
    }

    async fn write(&self, key: &str, request: MemcacheRequest) -> Result<bool> {
        validate_key(key.as_bytes())?;
        let attempts = self.writer_indices.len().min(2);
        let mut last_transport_error = None;
        for position in 0..attempts {
            let group_index = self.writer_indices[position];
            match self
                .request_at(group_index, key, request.clone(), None)
                .await
            {
                Ok(response) => {
                    let WriteResponse {
                        result,
                        fanout,
                        retryable,
                    } = response.into_write()?;
                    if fanout {
                        self.fanout_after(position, key, &request);
                        return result;
                    }
                    if retryable && position + 1 < attempts {
                        continue;
                    }
                    return result;
                }
                Err(error) => {
                    last_transport_error = Some(error);
                    if position + 1 == attempts {
                        break;
                    }
                }
            }
        }
        Err(last_transport_error.unwrap_or(Error::Unavailable))
    }
}

#[async_trait]
impl Cacheable for CacheTopology {
    async fn get(&self, key: &str) -> Result<Option<Value>> {
        Ok(self.retrieve(key, false).await?.map(|value| value.value))
    }

    async fn get_multi(&self, keys: &[&str]) -> Result<HashMap<String, Value>> {
        let responses = join_all(keys.iter().map(|key| self.get(key))).await;
        let mut values = HashMap::with_capacity(keys.len());
        for (key, response) in keys.iter().zip(responses) {
            if let Some(value) = response? {
                values.insert((*key).to_owned(), value);
            }
        }
        Ok(values)
    }

    async fn set(&self, key: &str, value: Value, expire: Expiration) -> Result<bool> {
        self.write(key, MemcacheRequest::set(key, value, expire))
            .await
    }

    async fn set_with_noreply(&self, key: &str, value: Value, expire: Expiration) -> Result<()> {
        self.set(key, value, expire).await.map(|_| ())
    }

    async fn add(&self, key: &str, value: Value, expire: Expiration) -> Result<bool> {
        self.write(key, MemcacheRequest::add(key, value, expire))
            .await
    }

    async fn get_cas(&self, key: &str) -> Result<Option<CasValue>> {
        self.retrieve(key, true).await
    }

    async fn cas(&self, key: &str, value: &CasValue, expire: Expiration) -> Result<bool> {
        self.write(
            key,
            MemcacheRequest::cas(key, value.value.clone(), expire, value.cas),
        )
        .await
    }

    async fn delete(&self, key: &str) -> Result<bool> {
        self.write(key, MemcacheRequest::delete(key)).await
    }

    async fn delete_with_noreply(&self, key: &str) -> Result<()> {
        self.delete(key).await.map(|_| ())
    }
}

fn configured_timeout(configured_ms: u32, fallback: Duration) -> Duration {
    if configured_ms == 0 {
        return fallback;
    }
    Duration::from_millis(u64::from(configured_ms).clamp(MIN_TIMEOUT_MS, MAX_TIMEOUT_MS))
}

fn group_specs(
    conf: &CacheNamespaceConf,
    update_master_l1: bool,
) -> Result<(Vec<Vec<String>>, usize, Vec<usize>)> {
    if conf.masters().is_empty() {
        return Err(Error::Client(
            "cache-service namespace has an empty master list".into(),
        ));
    }
    let mut groups = Vec::with_capacity(2 + conf.master_l1().len() + conf.slave_l1().len());
    let mut writers = Vec::with_capacity(groups.capacity());
    let mut seen = HashSet::with_capacity(groups.capacity());

    fn add_group(
        groups: &mut Vec<Vec<String>>,
        writers: &mut Vec<usize>,
        seen: &mut HashSet<Vec<String>>,
        group: Vec<String>,
        writer: bool,
    ) {
        if group.is_empty() || !seen.insert(group.clone()) {
            return;
        }
        let index = groups.len();
        groups.push(group);
        if writer {
            writers.push(index);
        }
    }

    add_group(
        &mut groups,
        &mut writers,
        &mut seen,
        conf.masters().to_vec(),
        true,
    );
    let mut master_l1 = conf.master_l1().to_vec();
    master_l1.shuffle(&mut rand::thread_rng());
    for group in master_l1 {
        let also_slave = group.as_slice() == conf.slaves();
        add_group(
            &mut groups,
            &mut writers,
            &mut seen,
            group,
            update_master_l1 || also_slave,
        );
    }
    let local_len = groups.len();
    add_group(
        &mut groups,
        &mut writers,
        &mut seen,
        conf.slaves().to_vec(),
        true,
    );
    if conf.update_slave_l1() {
        for group in conf.slave_l1() {
            add_group(&mut groups, &mut writers, &mut seen, group.clone(), true);
        }
    }
    Ok((groups, local_len, writers))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(yaml: &str) -> CacheNamespaceConf {
        crate::CacheServiceConfig::from_yaml_str(yaml)
            .unwrap()
            .namespace("cache")
            .unwrap()
            .clone()
    }

    #[test]
    fn groups_are_replica_first_and_exact_duplicates_are_removed() {
        let conf = parse(
            "cache:\n  master: [127.0.0.1:1, 127.0.0.1:2]\n  master_l1:\n    - [127.0.0.2:1]\n    - [127.0.0.1:1, 127.0.0.1:2]\n  slave: [127.0.0.3:1]\n",
        );
        let (groups, local, writers) = group_specs(&conf, true).unwrap();
        assert_eq!(groups[0], conf.masters());
        assert_eq!(groups.len(), 3);
        assert_eq!(local, 2);
        assert_eq!(writers, vec![0, 1, 2]);
    }

    #[test]
    fn slave_equal_to_master_l1_remains_a_writer_when_l1_updates_are_off() {
        let conf = parse(
            "cache:\n  master: [127.0.0.1:1]\n  master_l1:\n    - [127.0.0.2:1]\n  slave: [127.0.0.2:1]\n",
        );
        let (groups, local, writers) = group_specs(&conf, false).unwrap();
        assert_eq!(groups.len(), 2);
        assert_eq!(local, 2);
        assert_eq!(writers, vec![0, 1]);
    }

    #[test]
    fn namespace_timeouts_are_clamped_like_reference_client() {
        assert_eq!(
            configured_timeout(1, Duration::from_secs(1)),
            Duration::from_millis(20)
        );
        assert_eq!(
            configured_timeout(9_000, Duration::from_secs(1)),
            Duration::from_secs(6)
        );
        assert_eq!(
            configured_timeout(0, Duration::from_millis(100)),
            Duration::from_millis(100)
        );
    }
}
