//! Text search conversions. `InternalSearch` and `SearchVersion` have no
//! serde/JSON impls upstream, so they travel as mirror messages
//! (`funrun.InternalSearch`, `funrun.SearchVersion`) converted field by field.

use anyhow::Context;
use common::{
    document::DocumentUpdate,
    index::IndexKeyBytes,
    query::{
        InternalSearch,
        InternalSearchFilterExpression,
        SearchVersion,
    },
    types::{
        IndexId,
        RepeatableTimestamp,
    },
};
use pb_funrun::funrun::{
    internal_search_filter::Filter as FilterProto,
    CandidateRevisionWithKey,
    InternalSearchFilter,
    TextSearchRequest,
    TextSearchResponse,
};
use search::{
    CandidateRevision,
    QueryResults,
};

use crate::{
    ids::{
        index_id_from_bytes,
        index_id_to_bytes,
        repeatable_ts_from_u64,
    },
    transaction::{
        field_path_from_proto,
        index_name_from_proto,
        index_name_to_proto,
        query_reads_from_proto,
        query_reads_to_proto,
    },
};

/// Arguments of `TransactionTextSnapshot::search`. The conductor resolves the
/// `Index` from `index_id` itself.
#[derive(Debug)]
pub struct TextSearchArgs {
    pub ts: RepeatableTimestamp,
    pub index_id: IndexId,
    pub search: InternalSearch,
    pub version: SearchVersion,
    pub pending_updates: Vec<DocumentUpdate>,
}

pub fn text_search_request_to_proto(args: &TextSearchArgs) -> anyhow::Result<TextSearchRequest> {
    let search = &args.search;
    let version = match args.version {
        SearchVersion::V1 => pb_funrun::funrun::SearchVersion::V1,
        SearchVersion::V2 => pb_funrun::funrun::SearchVersion::V2,
    };
    Ok(TextSearchRequest {
        ts: (*args.ts).into(),
        index_id: index_id_to_bytes(args.index_id),
        search: Some(pb_funrun::funrun::InternalSearch {
            index_name: Some(index_name_to_proto(&search.index_name)),
            table_name: search.table_name.to_string(),
            filters: search
                .filters
                .iter()
                .map(|f| match f {
                    InternalSearchFilterExpression::Search(field_path, text) => {
                        InternalSearchFilter {
                            field_path: Some(field_path.clone().into()),
                            filter: Some(FilterProto::Search(text.clone())),
                        }
                    },
                    InternalSearchFilterExpression::Eq(field_path, value) => InternalSearchFilter {
                        field_path: Some(field_path.clone().into()),
                        filter: Some(FilterProto::Eq(value.to_vec())),
                    },
                })
                .collect(),
        }),
        version: version.into(),
        pending_updates: args
            .pending_updates
            .iter()
            .cloned()
            .map(pb::common::DocumentUpdate::try_from)
            .collect::<anyhow::Result<_>>()?,
    })
}

pub fn text_search_request_from_proto(p: TextSearchRequest) -> anyhow::Result<TextSearchArgs> {
    let search = p.search.context("Missing search")?;
    let filters = search
        .filters
        .into_iter()
        .map(|f| {
            let field_path = field_path_from_proto(f.field_path)?;
            Ok(match f.filter.context("Missing filter")? {
                FilterProto::Search(text) => {
                    InternalSearchFilterExpression::Search(field_path, text)
                },
                FilterProto::Eq(value) => {
                    InternalSearchFilterExpression::Eq(field_path, value.into())
                },
            })
        })
        .collect::<anyhow::Result<_>>()?;
    let version = match pb_funrun::funrun::SearchVersion::try_from(p.version)? {
        pb_funrun::funrun::SearchVersion::Unspecified => anyhow::bail!("missing search version"),
        pb_funrun::funrun::SearchVersion::V1 => SearchVersion::V1,
        pb_funrun::funrun::SearchVersion::V2 => SearchVersion::V2,
    };
    Ok(TextSearchArgs {
        ts: repeatable_ts_from_u64(p.ts)?,
        index_id: index_id_from_bytes(&p.index_id)?,
        search: InternalSearch {
            index_name: index_name_from_proto(search.index_name)?,
            table_name: search.table_name.parse()?,
            filters,
        },
        version,
        pending_updates: p
            .pending_updates
            .into_iter()
            .map(DocumentUpdate::try_from)
            .collect::<anyhow::Result<_>>()?,
    })
}

pub fn query_results_to_proto(r: &QueryResults) -> TextSearchResponse {
    TextSearchResponse {
        revisions_with_keys: r
            .revisions_with_keys
            .iter()
            .map(|(revision, key)| CandidateRevisionWithKey {
                revision: Some(revision.clone().into()),
                index_key: key.0.clone(),
            })
            .collect(),
        reads: Some(query_reads_to_proto(&r.reads)),
        filtered_bytes_searched: r.filtered_bytes_searched,
    }
}

pub fn query_results_from_proto(p: TextSearchResponse) -> anyhow::Result<QueryResults> {
    Ok(QueryResults {
        revisions_with_keys: p
            .revisions_with_keys
            .into_iter()
            .map(|r| {
                let revision =
                    CandidateRevision::try_from(r.revision.context("Missing revision")?)?;
                Ok((revision, IndexKeyBytes(r.index_key)))
            })
            .collect::<anyhow::Result<_>>()?,
        reads: query_reads_from_proto(p.reads.context("Missing reads")?)?,
        filtered_bytes_searched: p.filtered_bytes_searched,
    })
}

#[cfg(test)]
mod tests {
    use common::{
        document::{
            CreationTime,
            DocumentUpdate,
        },
        index::IndexKeyBytes,
        query::{
            FilterValue,
            InternalSearch,
            InternalSearchFilterExpression,
            SearchVersion,
        },
        types::{
            IndexDescriptor,
            IndexId,
            TabletIndexName,
            Timestamp,
            WriteTimestamp,
        },
    };
    use search::{
        CandidateRevision,
        QueryResults,
    };
    use value::{
        ConvexValue,
        InternalId,
    };

    use super::*;
    use crate::{
        ids::repeatable_ts_from_u64,
        test_samples::*,
    };

    fn sample_search_args() -> TextSearchArgs {
        let doc = sample_document("alice", "hello").unpack();
        TextSearchArgs {
            ts: repeatable_ts_from_u64(1_000).unwrap(),
            index_id: IndexId(InternalId::from([9u8; 16])),
            search: InternalSearch {
                index_name: TabletIndexName::new(
                    sample_tablet(),
                    IndexDescriptor::new("search_body").unwrap(),
                )
                .unwrap(),
                table_name: "messages".parse().unwrap(),
                filters: vec![
                    InternalSearchFilterExpression::Search(field_path("body"), "hello".into()),
                    InternalSearchFilterExpression::Eq(
                        field_path("author"),
                        FilterValue::from_search_value(Some(
                            &ConvexValue::try_from("alice").unwrap(),
                        )),
                    ),
                ],
            },
            version: SearchVersion::V2,
            pending_updates: vec![DocumentUpdate {
                id: doc.id(),
                old_document: None,
                new_document: Some(doc),
            }],
        }
    }

    #[test]
    fn text_search_request_round_trips() {
        let args = sample_search_args();
        let proto = text_search_request_to_proto(&args).unwrap();
        let back = text_search_request_from_proto(proto.clone()).unwrap();
        assert_eq!(text_search_request_to_proto(&back).unwrap(), proto);
        assert_eq!(back.search, args.search);
        assert_eq!(back.version, SearchVersion::V2);
        assert_eq!(back.pending_updates, args.pending_updates);
    }

    #[test]
    fn unspecified_search_version_is_rejected() {
        let mut proto = text_search_request_to_proto(&sample_search_args()).unwrap();
        proto.version = 0;
        let err = text_search_request_from_proto(proto).unwrap_err();
        assert!(format!("{err:#}").contains("search version"), "{err:#}");
    }

    #[test]
    fn query_results_round_trip() {
        let read_set = sample_read_set();
        let (_, reads) = read_set.iter_search().next().unwrap();
        let revision = |ts: WriteTimestamp| CandidateRevision {
            score: 0.5,
            id: InternalId::from([7u8; 16]),
            ts,
            creation_time: CreationTime::try_from(1.0).unwrap(),
        };
        let results = QueryResults {
            revisions_with_keys: vec![
                (
                    revision(WriteTimestamp::Committed(
                        Timestamp::try_from(10u64).unwrap(),
                    )),
                    IndexKeyBytes(vec![1, 2]),
                ),
                (revision(WriteTimestamp::Pending), IndexKeyBytes(vec![3])),
            ],
            reads: reads.clone(),
            filtered_bytes_searched: 77,
        };
        let proto = query_results_to_proto(&results);
        let back = query_results_from_proto(proto.clone()).unwrap();
        assert_eq!(query_results_to_proto(&back), proto);
        assert_eq!(back.revisions_with_keys, results.revisions_with_keys);
    }
}
