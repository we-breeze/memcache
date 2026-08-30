//! Public protocol selection for [`crate::CacheService`].

/// Wire protocol spoken to the memcached endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Protocol {
    /// Classic ASCII/text protocol (`cn.vika.memcached` compatible).
    Text,
    /// Binary protocol (`com.schooner.MemCached` compatible).
    #[default]
    Binary,
}
