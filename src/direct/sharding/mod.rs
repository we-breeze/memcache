//! Hash and distribution algorithms for client-side backend sharding.
//!
//! Vendored 1:1 from the breeze mesh's `sharding` crate
//! (`rust/breeze/sharding/src/{hash,distribution}`), so an SDK accessing
//! backends directly computes **the same shard index as the mesh** for a
//! given `(hash, distribution)` configuration. The only deviations from the
//! upstream sources: `log::` calls are mapped to `tracing::`, and the
//! `ds::RingSlice` `HashKey` impl (mesh-internal) is omitted.
//!
//! Names come from the resource configuration, e.g. hash `crc32-underscore`,
//! distribution `modula`, `absmodula`, `ketama`, `range-256`, `modrange`,
//! `slotmod-1024`, `splitmod-32`, `secmod`.

// Vendored upstream sources keep their original style: they are excluded
// from this repository's clippy/rustfmt gates so future re-syncs stay
// diff-minimal.
#[allow(clippy::all)]
#[rustfmt::skip]
pub mod distribution;
#[allow(clippy::all)]
#[rustfmt::skip]
pub mod hash;

pub use distribution::Distribute;
pub use hash::Hasher;

use hash::Hash;

/// Hash algorithm names as used in resource configuration. For memcached,
/// plain [`HASH_CRC32`] is always rewritten to [`HASH_CRC32_SHORT`] (see
/// [`Sharding::new`]).
pub const HASH_CRC32: &str = "crc32";
/// The short crc32 variant (`(crc32 >> 16) & 0x7fff`) — what memcached
/// namespaces actually route with.
pub const HASH_CRC32_SHORT: &str = "crc32-short";
/// The default distribution (modulo over the backend list).
pub const DIST_MODULA: &str = "modula";

/// Normalize a configured hash name: for memcached, plain [`HASH_CRC32`]
/// means [`HASH_CRC32_SHORT`] — the mesh routes with the short variant
/// (matching the breeze endpoint's cacheservice conversion). Any other
/// name passes through unchanged.
pub fn normalize_hash_name(name: &str) -> &str {
    if name.eq_ignore_ascii_case(HASH_CRC32) {
        HASH_CRC32_SHORT
    } else {
        name
    }
}

/// A resolved client-side sharding plan: hash algorithm + slot distribution.
#[derive(Clone, Debug)]
pub struct Sharding {
    hasher: Hasher,
    distribute: Distribute,
}

impl Sharding {
    /// Build from configuration names and the backend name list (ketama uses
    /// the names as ring nodes; other distributions only use the count).
    ///
    /// Like the breeze endpoint's cacheservice config, a plain `crc32` hash
    /// name is rewritten to `crc32-short`: for memcached the two are the
    /// same algorithm and the mesh routes with the short variant.
    pub fn new(hash_alg: &str, distribution: &str, backends: &[String]) -> Self {
        Sharding {
            hasher: Hasher::from(normalize_hash_name(hash_alg)),
            distribute: Distribute::from(distribution, backends),
        }
    }

    /// The shard index for `key`.
    #[inline]
    pub fn shard_idx(&self, key: &[u8]) -> usize {
        self.distribute.index(self.hasher.hash(&key))
    }

    /// The raw hash of `key`.
    #[inline]
    pub fn hash(&self, key: &[u8]) -> i64 {
        self.hasher.hash(&key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc32_is_rewritten_to_crc32_short() {
        let names: Vec<String> = (0..4).map(|i| format!("127.0.0.1:{}", 11000 + i)).collect();
        let plain = Sharding::new(HASH_CRC32, DIST_MODULA, &names);
        let short = Sharding::new(HASH_CRC32_SHORT, DIST_MODULA, &names);
        for key in [&b"u7559723634"[..], b"abc", b"0000000000000001kkkk"] {
            assert_eq!(
                plain.shard_idx(key),
                short.shard_idx(key),
                "crc32 must equal crc32-short for {key:?}"
            );
            // crc32-short = (crc32 >> 16) & 0x7fff: for these keys the
            // truncated hash differs from the raw crc32 modulo.
            assert_eq!(plain.hash(key), short.hash(key));
        }
        // The known vector: crc32("u7559723634") = 798256386 → short hash
        // (798256386 >> 16) & 0x7fff = 12180 → 12180 % 4 = 0.
        assert_eq!(plain.hash(b"u7559723634"), 12180);
        assert_eq!(plain.shard_idx(b"u7559723634"), 0);
    }
}
