use anyhow::Context;
use common::{
    bootstrap_model::index::database_index::IndexedFields,
    document::PendingDocumentUpdate,
    identity::InertIdentity,
    types::{
        IndexDescriptor,
        TabletIndexName,
        Timestamp,
    },
};
use database::{
    reads::IndexReads,
    ReadSet,
    TransactionReadSize,
};
use function_runner::{
    FunctionFinalTransaction,
    FunctionReads,
    FunctionWrites,
};
use pb_funrun::funrun::text_query_term_read::Term as TermProto;
use search::{
    query::TextQueryTerm,
    FilterConditionRead,
    QueryReads,
    TextQueryTermRead,
};
use udf::{
    validation::{
        ValidatedHttpPath,
        ValidatedPathAndArgs,
    },
    FunctionOutcome,
    HttpActionRequestHead,
};
use usage_tracking::FunctionUsageStats;
use value::FieldPath;

use crate::{
    collect_unique,
    ids::{
        tablet_id_from_bytes,
        tablet_id_to_bytes,
    },
};

pub fn writes_to_proto(w: &FunctionWrites) -> anyhow::Result<pb_funrun::funrun::Writes> {
    Ok(pb_funrun::funrun::Writes {
        updates: w
            .updates
            .iter()
            .cloned()
            .map(pb::common::PendingDocumentUpdate::try_from)
            .collect::<anyhow::Result<_>>()?,
    })
}

pub fn writes_from_proto(p: pb_funrun::funrun::Writes) -> anyhow::Result<FunctionWrites> {
    Ok(FunctionWrites {
        updates: p
            .updates
            .into_iter()
            .map(PendingDocumentUpdate::try_from)
            .collect::<anyhow::Result<_>>()?,
    })
}

pub fn final_transaction_to_proto(
    tx: &FunctionFinalTransaction,
) -> anyhow::Result<pb_funrun::funrun::FunctionFinalTransaction> {
    let FunctionReads {
        reads,
        num_intervals,
        user_tx_size,
        system_tx_size,
    } = &tx.reads;
    Ok(pb_funrun::funrun::FunctionFinalTransaction {
        begin_timestamp: tx.begin_timestamp.into(),
        reads: Some(pb_funrun::funrun::FunctionReads {
            reads: Some(read_set_to_proto(reads)?),
            num_intervals: u64::try_from(*num_intervals)?,
            user_tx_size: Some(read_size_to_proto(user_tx_size)?),
            system_tx_size: Some(read_size_to_proto(system_tx_size)?),
        }),
        writes: Some(writes_to_proto(&tx.writes)?),
        rows_read_by_tablet: tx
            .rows_read_by_tablet
            .iter()
            .map(|(tablet, rows)| pb_funrun::funrun::TabletRows {
                tablet_id: tablet_id_to_bytes(*tablet),
                rows: *rows,
            })
            .collect(),
    })
}

pub fn final_transaction_from_proto(
    p: pb_funrun::funrun::FunctionFinalTransaction,
) -> anyhow::Result<FunctionFinalTransaction> {
    let reads = p.reads.context("Missing reads")?;
    Ok(FunctionFinalTransaction {
        begin_timestamp: Timestamp::try_from(p.begin_timestamp)?,
        reads: FunctionReads {
            reads: read_set_from_proto(reads.reads.context("Missing read set")?)?,
            num_intervals: usize::try_from(reads.num_intervals)?,
            user_tx_size: read_size_from_proto(
                reads.user_tx_size.context("Missing user_tx_size")?,
            )?,
            system_tx_size: read_size_from_proto(
                reads.system_tx_size.context("Missing system_tx_size")?,
            )?,
        },
        writes: writes_from_proto(p.writes.context("Missing writes")?)?,
        rows_read_by_tablet: collect_unique(
            "rows_read_by_tablet",
            p.rows_read_by_tablet
                .into_iter()
                .map(|r| Ok((tablet_id_from_bytes(&r.tablet_id)?, r.rows))),
        )?,
    })
}

fn read_size_to_proto(s: &TransactionReadSize) -> anyhow::Result<pb_funrun::funrun::ReadSize> {
    Ok(pb_funrun::funrun::ReadSize {
        total_document_size: u64::try_from(s.total_document_size)?,
        total_document_count: u64::try_from(s.total_document_count)?,
    })
}

fn read_size_from_proto(p: pb_funrun::funrun::ReadSize) -> anyhow::Result<TransactionReadSize> {
    Ok(TransactionReadSize {
        total_document_size: usize::try_from(p.total_document_size)?,
        total_document_count: usize::try_from(p.total_document_count)?,
    })
}

pub(crate) fn index_name_to_proto(name: &TabletIndexName) -> pb_funrun::funrun::TabletIndexName {
    pb_funrun::funrun::TabletIndexName {
        tablet_id: tablet_id_to_bytes(*name.table()),
        descriptor: name.descriptor().to_string(),
    }
}

pub(crate) fn index_name_from_proto(
    p: Option<pb_funrun::funrun::TabletIndexName>,
) -> anyhow::Result<TabletIndexName> {
    let p = p.context("Missing index name")?;
    let tablet = tablet_id_from_bytes(&p.tablet_id)?;
    let descriptor = IndexDescriptor::new(p.descriptor)?;
    // `new` rejects reserved descriptors (`by_id`, `by_creation_time`), which
    // are the most common reads; same split as `TabletIndexMetadata`'s decoder.
    if descriptor.is_reserved() {
        TabletIndexName::new_reserved(tablet, descriptor)
    } else {
        TabletIndexName::new(tablet, descriptor)
    }
}

pub(crate) fn field_path_from_proto(p: Option<pb::common::FieldPath>) -> anyhow::Result<FieldPath> {
    FieldPath::try_from(p.context("Missing field_path")?)
}

pub fn read_set_to_proto(rs: &ReadSet) -> anyhow::Result<pb_funrun::funrun::ReadSet> {
    Ok(pb_funrun::funrun::ReadSet {
        indexed: rs
            .iter_indexed()
            .map(|(name, reads)| pb_funrun::funrun::IndexReads {
                index: Some(index_name_to_proto(name)),
                fields: reads.fields.clone().into(),
                intervals: reads.intervals.clone().into(),
            })
            .collect(),
        search: rs
            .iter_search()
            .map(|(name, reads)| pb_funrun::funrun::SearchReads {
                index: Some(index_name_to_proto(name)),
                reads: Some(query_reads_to_proto(reads)),
            })
            .collect(),
    })
}

pub fn read_set_from_proto(p: pb_funrun::funrun::ReadSet) -> anyhow::Result<ReadSet> {
    let indexed = collect_unique(
        "indexed read set",
        p.indexed.into_iter().map(|r| {
            let fields = r
                .fields
                .into_iter()
                .map(FieldPath::try_from)
                .collect::<anyhow::Result<Vec<_>>>()?;
            let reads = IndexReads {
                fields: IndexedFields::try_from(fields)?,
                intervals: r.intervals.try_into()?,
                // ponytail: stack traces are debug-only (READ_SET_CAPTURE_BACKTRACES)
                // and never sent; OCC errors from remote runs lack them. Add a proto
                // field if remote OCC debugging needs them.
                stack_traces: None,
            };
            Ok((index_name_from_proto(r.index)?, reads))
        }),
    )?;
    let search = collect_unique(
        "search read set",
        p.search.into_iter().map(|r| {
            Ok((
                index_name_from_proto(r.index)?,
                query_reads_from_proto(r.reads.context("Missing query reads")?)?,
            ))
        }),
    )?;
    Ok(ReadSet::new(indexed, search))
}

pub fn query_reads_to_proto(q: &QueryReads) -> pb_funrun::funrun::QueryReads {
    pb_funrun::funrun::QueryReads {
        text_queries: q
            .text_queries
            .iter()
            .map(|t| pb_funrun::funrun::TextQueryTermRead {
                field_path: Some(t.field_path.clone().into()),
                term: Some(match &t.term {
                    TextQueryTerm::Exact(token) => TermProto::Exact(token.clone()),
                    TextQueryTerm::Prefix(token) => TermProto::Prefix(token.clone()),
                }),
            })
            .collect(),
        filter_conditions: q
            .filter_conditions
            .iter()
            .map(|f| match f {
                FilterConditionRead::Must(field_path, value) => {
                    pb_funrun::funrun::FilterConditionRead {
                        field_path: Some(field_path.clone().into()),
                        filter_value: Some(value.to_vec()),
                    }
                },
            })
            .collect(),
    }
}

pub fn query_reads_from_proto(p: pb_funrun::funrun::QueryReads) -> anyhow::Result<QueryReads> {
    let text_queries = p
        .text_queries
        .into_iter()
        .map(|t| {
            let term = match t.term.context("Missing term")? {
                TermProto::Exact(token) => TextQueryTerm::Exact(token),
                TermProto::Prefix(token) => TextQueryTerm::Prefix(token),
            };
            Ok(TextQueryTermRead::new(
                field_path_from_proto(t.field_path)?,
                term,
            ))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let filter_conditions = p
        .filter_conditions
        .into_iter()
        .map(|f| {
            Ok(FilterConditionRead::Must(
                field_path_from_proto(f.field_path)?,
                f.filter_value.context("Missing filter_value")?.into(),
            ))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    // `new` rebuilds the derived fuzzy-term tries used for overlap checks.
    Ok(QueryReads::new(
        text_queries.into(),
        filter_conditions.into(),
    ))
}

pub fn run_result_to_proto(
    transaction: Option<FunctionFinalTransaction>,
    outcome: FunctionOutcome,
    usage: FunctionUsageStats,
) -> anyhow::Result<pb_funrun::funrun::RunResult> {
    Ok(pb_funrun::funrun::RunResult {
        transaction: transaction
            .as_ref()
            .map(final_transaction_to_proto)
            .transpose()?,
        outcome: Some(outcome.try_into()?),
        usage: Some(usage.into()),
    })
}

/// A decoded `RunResult`.
pub struct RunResultParts {
    pub transaction: Option<FunctionFinalTransaction>,
    pub outcome: FunctionOutcome,
    pub usage: FunctionUsageStats,
}

pub fn run_result_from_proto(
    p: pb_funrun::funrun::RunResult,
    path_and_args: Option<ValidatedPathAndArgs>,
    http: Option<(ValidatedHttpPath, HttpActionRequestHead)>,
    identity: InertIdentity,
) -> anyhow::Result<RunResultParts> {
    Ok(RunResultParts {
        transaction: p
            .transaction
            .map(final_transaction_from_proto)
            .transpose()?,
        outcome: FunctionOutcome::from_proto(
            p.outcome.context("Missing outcome")?,
            path_and_args,
            http,
            identity,
        )?,
        usage: p.usage.context("Missing usage")?.try_into()?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_samples::*;

    #[test]
    fn read_set_round_trips() {
        let rs = sample_read_set();
        let proto = read_set_to_proto(&rs).unwrap();
        let back = read_set_from_proto(proto.clone()).unwrap();
        assert_eq!(read_set_to_proto(&back).unwrap(), proto);
        assert_eq!(back.iter_indexed().count(), 1);
        assert_eq!(back.iter_search().count(), 1);
    }

    #[test]
    fn read_set_round_trip_preserves_conflict_semantics() {
        let rs = sample_read_set();
        let back = read_set_from_proto(read_set_to_proto(&rs).unwrap()).unwrap();

        // Index reads: `writes_overlap_by_index` is crate-private, so compare
        // the interval membership it relies on for writes inside/outside.
        let (_, orig) = rs.iter_indexed().next().unwrap();
        let (_, decoded) = back.iter_indexed().next().unwrap();
        for key in [&[1u8, 2][..], &[5], &[5, 9, 9], &[3], &[], &[6]] {
            assert_eq!(
                orig.intervals.contains(key),
                decoded.intervals.contains(key),
                "{key:?}"
            );
        }
        assert!(decoded.intervals.contains(&[1, 2]));
        assert!(!decoded.intervals.contains(&[3]));

        // Search reads: exercises the fuzzy tries rebuilt by `QueryReads::new`.
        for (author, body, overlaps) in [
            ("alice", "hello there", true),
            ("alice", "worldwide news", true),
            ("alice", "goodbye", false),
            ("bob", "hello there", false),
        ] {
            let doc = sample_document(author, body);
            assert_eq!(
                rs.search_overlaps_document(&doc).is_some(),
                overlaps,
                "{author}/{body}"
            );
            assert_eq!(
                back.search_overlaps_document(&doc).is_some(),
                overlaps,
                "{author}/{body}"
            );
        }
    }

    #[test]
    fn final_transaction_round_trips() {
        let tx = sample_final_transaction();
        let proto = final_transaction_to_proto(&tx).unwrap();
        let back = final_transaction_from_proto(proto.clone()).unwrap();
        assert_eq!(final_transaction_to_proto(&back).unwrap(), proto);
        assert_eq!(back.begin_timestamp, tx.begin_timestamp);
        assert_eq!(back.rows_read_by_tablet, tx.rows_read_by_tablet);
    }

    #[test]
    fn run_result_round_trips_query_outcome() {
        let (tx, outcome, usage, path_and_args, identity) = sample_query_result();
        let proto = run_result_to_proto(Some(tx), outcome, usage).unwrap();
        let back =
            run_result_from_proto(proto.clone(), Some(path_and_args), None, identity).unwrap();
        assert!(matches!(back.outcome, FunctionOutcome::Query(_)));
        assert_eq!(
            run_result_to_proto(back.transaction, back.outcome, back.usage).unwrap(),
            proto
        );
    }

    #[test]
    fn duplicate_index_reads_entry_is_an_error() {
        let mut proto = read_set_to_proto(&sample_read_set()).unwrap();
        proto.indexed.push(proto.indexed[0].clone());
        let err = read_set_from_proto(proto).err().unwrap();
        assert!(err.to_string().contains("duplicate"), "{err}");
    }

    #[test]
    fn duplicate_rows_read_by_tablet_is_an_error() {
        let mut proto = final_transaction_to_proto(&sample_final_transaction()).unwrap();
        proto
            .rows_read_by_tablet
            .push(proto.rows_read_by_tablet[0].clone());
        let err = final_transaction_from_proto(proto).err().unwrap();
        assert!(err.to_string().contains("duplicate"), "{err}");
    }
}
