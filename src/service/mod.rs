//! Internal operation contract and optional Vintage live-config adapter.

mod cacheable;
#[cfg(feature = "service")]
pub(crate) mod vintage_live;

pub(crate) use cacheable::Cacheable;
