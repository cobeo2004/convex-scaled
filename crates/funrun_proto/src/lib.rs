use std::{
    collections::BTreeMap,
    fmt::Debug,
};

pub mod auth;
pub mod ids;
pub mod request;
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
