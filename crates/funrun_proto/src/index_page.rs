use std::sync::Arc;

use anyhow::Context;
use common::{
    document::{
        PackedDocument,
        ResolvedDocument,
    },
    index::IndexKeyBytes,
    interval::{
        Interval,
        IntervalSet,
    },
    query::{
        CursorPosition,
        Order,
    },
    types::{
        IndexRef,
        RepeatableTimestamp,
        Timestamp,
    },
};
use indexing::index_reader::{
    IndexEntry,
    IndexPage,
};
use pb_funrun::funrun::{
    IndexEntry as IndexEntryProto,
    IndexPageRequest,
    IndexPageResponse,
};
use prost::Message;
use value::TabletId;

use crate::ids::{
    index_ref_from_proto,
    index_ref_to_proto,
    repeatable_ts_from_u64,
    tablet_id_from_bytes,
    tablet_id_to_bytes,
};

/// Decoded `IndexPageRequest`: the arguments of `IndexReader::index_page`
/// plus the snapshot timestamp to read at.
#[derive(Debug)]
pub struct IndexPageArgs {
    pub ts: RepeatableTimestamp,
    pub index: IndexRef,
    pub tablet_id: TabletId,
    pub interval: Interval,
    pub order: Order,
    pub max_results: usize,
}

pub fn index_page_request_to_proto(
    ts: RepeatableTimestamp,
    index: IndexRef,
    tablet_id: TabletId,
    interval: &Interval,
    order: Order,
    max_results: usize,
) -> anyhow::Result<IndexPageRequest> {
    let mut set = IntervalSet::new();
    set.add(interval.clone());
    let order = match order {
        Order::Asc => pb::common::Order::Asc,
        Order::Desc => pb::common::Order::Desc,
    };
    Ok(IndexPageRequest {
        ts: (*ts).into(),
        index: Some(index_ref_to_proto(index)),
        tablet_id: tablet_id_to_bytes(tablet_id),
        interval: set.into(),
        order: order.into(),
        max_results: max_results.try_into()?,
    })
}

pub fn index_page_request_from_proto(p: IndexPageRequest) -> anyhow::Result<IndexPageArgs> {
    // An empty `Interval` encodes as an empty `IntervalSet`, i.e. no elements.
    anyhow::ensure!(
        p.interval.len() <= 1,
        "IndexPageRequest must carry at most one interval, got {}",
        p.interval.len()
    );
    let set = IntervalSet::try_from(p.interval)?;
    let interval = set.iter().next().unwrap_or(Interval::empty());
    let order = match pb::common::Order::try_from(p.order)? {
        pb::common::Order::Asc => Order::Asc,
        pb::common::Order::Desc => Order::Desc,
    };
    Ok(IndexPageArgs {
        ts: repeatable_ts_from_u64(p.ts)?,
        index: index_ref_from_proto(p.index.context("Missing index")?)?,
        tablet_id: tablet_id_from_bytes(&p.tablet_id)?,
        interval,
        order,
        max_results: p.max_results.try_into()?,
    })
}

/// Encodes `page`, cut short after the last entry that keeps the entries
/// within `max_bytes` (but never fewer than one entry), with the cursor after
/// that entry. `RemoteIndexReader` requests the rest.
pub fn index_page_to_proto(
    page: &IndexPage,
    max_bytes: usize,
) -> anyhow::Result<IndexPageResponse> {
    let mut entries: Vec<IndexEntryProto> = Vec::new();
    let mut bytes = 0;
    for e in &page.entries {
        let entry = IndexEntryProto {
            key: e.key.0.clone(),
            ts: e.ts.into(),
            value: Some(e.value.unpack().try_into()?),
        };
        bytes += entry.encoded_len();
        if let Some(last) = entries.last()
            && bytes > max_bytes
        {
            let cursor_after = Some(last.key.clone());
            return Ok(IndexPageResponse {
                entries,
                cursor_after,
            });
        }
        entries.push(entry);
    }
    Ok(IndexPageResponse {
        entries,
        cursor_after: match &page.cursor {
            CursorPosition::After(key) => Some(key.0.clone()),
            CursorPosition::End => None,
        },
    })
}

pub fn index_page_from_proto(p: IndexPageResponse) -> anyhow::Result<IndexPage> {
    Ok(IndexPage {
        entries: p
            .entries
            .into_iter()
            .map(|e| {
                let doc = ResolvedDocument::try_from(e.value.context("Missing value")?)?;
                Ok(Arc::new(IndexEntry {
                    key: IndexKeyBytes(e.key),
                    ts: Timestamp::try_from(e.ts)?,
                    value: PackedDocument::pack(&doc),
                }))
            })
            .collect::<anyhow::Result<_>>()?,
        cursor: match p.cursor_after {
            Some(key) => CursorPosition::After(IndexKeyBytes(key)),
            None => CursorPosition::End,
        },
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common::{
        index::IndexKeyBytes,
        interval::{
            BinaryKey,
            Interval,
        },
        query::{
            CursorPosition,
            Order,
        },
        types::{
            IndexId,
            IndexRef,
            PersistenceIndexId,
            Timestamp,
        },
    };
    use indexing::index_reader::{
        IndexEntry,
        IndexPage,
    };
    use value::InternalId;

    use super::*;
    use crate::{
        ids::repeatable_ts_from_u64,
        test_samples::*,
    };

    fn assert_page_round_trips(page: IndexPage) {
        let proto = index_page_to_proto(&page, usize::MAX).unwrap();
        let back = index_page_from_proto(proto.clone()).unwrap();
        assert_eq!(index_page_to_proto(&back, usize::MAX).unwrap(), proto);
        assert_eq!(back, page);
    }

    fn entry(key: u8, ts: u64, body: &str) -> Arc<IndexEntry> {
        Arc::new(IndexEntry {
            key: IndexKeyBytes(vec![key]),
            ts: Timestamp::try_from(ts).unwrap(),
            value: sample_document("alice", body),
        })
    }

    fn three_entries_page() -> IndexPage {
        IndexPage {
            entries: vec![
                entry(1, 10, "one"),
                entry(2, 20, "two"),
                entry(3, 30, "three"),
            ],
            cursor: CursorPosition::End,
        }
    }

    #[test]
    fn index_page_over_the_byte_budget_is_cut_after_the_last_entry_that_fits() {
        let page = three_entries_page();
        let one = index_page_to_proto(&page, usize::MAX).unwrap().entries[0].encoded_len();
        let proto = index_page_to_proto(&page, 2 * one + 1).unwrap();
        assert_eq!(proto.entries.len(), 2);
        assert_eq!(proto.cursor_after, Some(vec![2]));
    }

    #[test]
    fn index_page_always_carries_at_least_one_entry() {
        let proto = index_page_to_proto(&three_entries_page(), 1).unwrap();
        assert_eq!(proto.entries.len(), 1);
        assert_eq!(proto.cursor_after, Some(vec![1]));
    }

    #[test]
    fn index_page_within_the_budget_keeps_its_cursor() {
        let proto = index_page_to_proto(&three_entries_page(), usize::MAX).unwrap();
        assert_eq!(proto.entries.len(), 3);
        assert_eq!(proto.cursor_after, None);
    }

    #[test]
    fn index_page_with_entries_round_trips() {
        assert_page_round_trips(IndexPage {
            entries: vec![entry(1, 10, "one"), entry(2, 20, "two")],
            cursor: CursorPosition::After(IndexKeyBytes(vec![2])),
        });
    }

    #[test]
    fn empty_index_page_round_trips() {
        assert_page_round_trips(IndexPage {
            entries: vec![],
            cursor: CursorPosition::End,
        });
    }

    #[test]
    fn index_page_request_round_trips_both_orders() {
        for order in [Order::Asc, Order::Desc] {
            let proto = index_page_request_to_proto(
                repeatable_ts_from_u64(1_000).unwrap(),
                IndexRef::from_parts(
                    IndexId(InternalId::from([2u8; 16])),
                    PersistenceIndexId::new(2),
                ),
                sample_tablet(),
                &Interval::prefix(BinaryKey::from(vec![1u8])),
                order,
                64,
            )
            .unwrap();
            let args = index_page_request_from_proto(proto.clone()).unwrap();
            assert_eq!(args.order, order);
            let again = index_page_request_to_proto(
                args.ts,
                args.index,
                args.tablet_id,
                &args.interval,
                args.order,
                args.max_results,
            )
            .unwrap();
            assert_eq!(again, proto);
        }
    }

    #[test]
    fn empty_interval_request_round_trips() {
        let proto = index_page_request_to_proto(
            repeatable_ts_from_u64(1_000).unwrap(),
            IndexRef::from_parts(
                IndexId(InternalId::from([2u8; 16])),
                PersistenceIndexId::new(2),
            ),
            sample_tablet(),
            &Interval::empty(),
            Order::Asc,
            64,
        )
        .unwrap();
        let args = index_page_request_from_proto(proto).unwrap();
        assert!(args.interval.is_empty());
    }

    #[test]
    fn index_page_request_with_two_intervals_is_an_error() {
        let mut proto = index_page_request_to_proto(
            repeatable_ts_from_u64(1_000).unwrap(),
            IndexRef::from_parts(
                IndexId(InternalId::from([2u8; 16])),
                PersistenceIndexId::new(2),
            ),
            sample_tablet(),
            &Interval::prefix(BinaryKey::from(vec![1u8])),
            Order::Asc,
            64,
        )
        .unwrap();
        let second = proto.interval[0].clone();
        proto.interval[0].start_inclusive = vec![0];
        proto.interval[0].end = Some(pb::common::interval::End::Exclusive(vec![0, 1]));
        proto.interval.push(second);
        assert!(index_page_request_from_proto(proto).is_err());
    }
}
