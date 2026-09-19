use std::{
    collections::BTreeMap,
    fmt::Debug,
};

/// Version of the funrun wire protocol: `funrun.proto` and the `pb::common`
/// / `pb::storage` messages it embeds. Bump it on any change to them. prost
/// drops unknown fields, so mixed builds would otherwise lose data silently
/// (e.g. read-set intervals) instead of failing. Workers report it in
/// `LoadReport` and send it as `x-funrun-protocol` on `FunctionHost` calls.
pub const FUNRUN_PROTOCOL_VERSION: u32 = 2;

pub mod auth;
pub mod callbacks;
pub mod deploy;
pub mod http;
pub mod ids;
pub mod index_page;
pub mod request;
pub mod search;
pub mod transaction;

#[cfg(test)]
pub mod test_samples;

/// Collects decoded `(key, value)` pairs into a map, rejecting duplicate keys.
/// A plain `collect` keeps the last duplicate, so a malformed message could
/// silently drop entries (e.g. read-set intervals needed for OCC).
pub(crate) fn collect_unique<K: Ord + Debug, V>(
    what: &str,
    entries: impl IntoIterator<Item = anyhow::Result<(K, V)>>,
) -> anyhow::Result<BTreeMap<K, V>> {
    let mut map = BTreeMap::new();
    for entry in entries {
        let (k, v) = entry?;
        anyhow::ensure!(!map.contains_key(&k), "duplicate {what} entry for {k:?}");
        map.insert(k, v);
    }
    Ok(map)
}
