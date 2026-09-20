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

/// Size limit for `ExecuteUp`, which carries both `Run` and `Deploy` frames
/// over one channel. Deploy is the larger of the two because analyze embeds
/// the push's whole module source, so both ends size the channel for it.
/// Conductor and worker must agree, hence one function rather than two knob
/// lookups.
pub fn max_up_message_size() -> usize {
    (*common::knobs::MAX_FUNRUN_RUN_FUNCTION_REQUEST_MESSAGE_SIZE)
        .max(*common::knobs::MAX_FUNRUN_DEPLOY_MESSAGE_SIZE)
}

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

#[cfg(test)]
mod tests {
    use common::knobs::MAX_PUSH_BYTES;

    use super::max_up_message_size;

    /// Analyze ships the push's whole module source inside one `ExecuteUp`, so
    /// anything the push API accepts must also fit on the funrun channel.
    /// Otherwise a push succeeds with `FUNCTION_RUNNER=local` and fails with
    /// `remote`, which breaks the local/remote equivalence guarantee.
    #[test]
    fn up_messages_fit_the_largest_accepted_push() {
        assert!(
            max_up_message_size() >= *MAX_PUSH_BYTES,
            "max_up_message_size() = {} but MAX_PUSH_BYTES = {}",
            max_up_message_size(),
            *MAX_PUSH_BYTES,
        );
    }
}
