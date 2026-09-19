use anyhow::Context;
use common::types::{
    IndexId,
    IndexRef,
    PersistenceIndexId,
    RepeatableReason,
    RepeatableTimestamp,
    Timestamp,
};
use value::{
    InternalId,
    TabletId,
};

pub fn tablet_id_to_bytes(id: TabletId) -> Vec<u8> {
    id.0.to_vec()
}

pub fn tablet_id_from_bytes(bytes: &[u8]) -> anyhow::Result<TabletId> {
    Ok(TabletId(InternalId::try_from(bytes)?))
}

pub fn index_id_to_bytes(id: IndexId) -> Vec<u8> {
    id.0.to_vec()
}

pub fn index_id_from_bytes(bytes: &[u8]) -> anyhow::Result<IndexId> {
    Ok(IndexId(InternalId::try_from(bytes)?))
}

pub fn index_ref_to_proto(r: IndexRef) -> pb_funrun::funrun::IndexRef {
    pb_funrun::funrun::IndexRef {
        index_id: index_id_to_bytes(r.id()),
        persistence_index_id: r.persistence_index_id().map(PersistenceIndexId::value),
    }
}

pub fn index_ref_from_proto(p: pb_funrun::funrun::IndexRef) -> anyhow::Result<IndexRef> {
    let persistence = p
        .persistence_index_id
        .map(|v| PersistenceIndexId::new(v).context("persistence_index_id must be nonzero"))
        .transpose()?;
    Ok(IndexRef::from_parts(
        index_id_from_bytes(&p.index_id)?,
        persistence,
    ))
}

/// The conductor chose `ts` as repeatable; the worker trusts an authenticated
/// conductor, same as upstream Funrun's `repeatable_ts` field.
pub fn repeatable_ts_from_u64(ts: u64) -> anyhow::Result<RepeatableTimestamp> {
    Ok(RepeatableTimestamp::new_validated(
        Timestamp::try_from(ts)?,
        RepeatableReason::InductiveRepeatableTimestamp,
    ))
}
