use std::{
    collections::BTreeMap,
    str::FromStr,
};

use common::{
    bootstrap_model::index::database_index::IndexedFields,
    document::{
        CreationTime,
        PackedDocument,
        ResolvedDocument,
    },
    execution_context::{
        ExecutionContext,
        ExecutionId,
        RequestId,
        RequestMetadata,
    },
    http::RoutedHttpPath,
    identity::InertIdentity,
    interval::{
        BinaryKey,
        Interval,
        IntervalSet,
    },
    query::FilterValue,
    query_journal::QueryJournal,
    runtime::UnixTimestamp,
    types::{
        ConvexOrigin,
        DeploymentClass,
        DeploymentMetadata,
        EnvVarName,
        EnvVarValue,
        IndexDescriptor,
        IndexId,
        IndexRef,
        PersistenceIndexId,
        RepeatableReason,
        RepeatableTimestamp,
        TabletIndexName,
        Timestamp,
        UdfType,
    },
};
use database::{
    reads::IndexReads,
    BootstrapMetadata,
    ReadSet,
    TransactionReadSize,
};
use function_runner::{
    server::FunctionMetadata,
    FunctionFinalTransaction,
    FunctionReads,
    FunctionWrites,
};
use keybroker::Identity;
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
    SyscallTrace,
};
use usage_tracking::FunctionUsageStats;
use value::{
    ConvexObject,
    ConvexValue,
    DeveloperDocumentId,
    FieldName,
    FieldPath,
    InternalId,
    ResolvedDocumentId,
    TableNumber,
    TabletId,
};

use crate::request::{
    HttpRequestParts,
    RunRequestParts,
};

fn sample_path_and_args() -> ValidatedPathAndArgs {
    ValidatedPathAndArgs::from_proto(pb::common::ValidatedPathAndArgs {
        path: Some("messages:send".to_string()),
        args: Some(b"[{}]".to_vec()),
        npm_version: None,
        component_path: Some(pb::common::ComponentPath::default()),
        component_id: None,
        reuse_context: None,
    })
    .expect("sample ValidatedPathAndArgs proto should convert")
}

pub fn sample_run_request_parts() -> RunRequestParts {
    let mut default_system_env_vars = BTreeMap::new();
    default_system_env_vars.insert(
        EnvVarName::from_str("CONVEX_CLOUD_URL").unwrap(),
        EnvVarValue::from_str("http://127.0.0.1:3210").unwrap(),
    );

    let mut in_memory_index_last_modified = BTreeMap::new();
    in_memory_index_last_modified.insert(
        IndexId(InternalId::MIN),
        Timestamp::try_from(900u64).unwrap(),
    );

    let mut table_counts = BTreeMap::new();
    table_counts.insert(TabletId(InternalId::from([5u8; 16])), 42);

    RunRequestParts {
        instance_name: "carnitas-instance".to_string(),
        udf_type: UdfType::Mutation,
        identity: Identity::system(),
        ts: RepeatableTimestamp::new_validated(
            Timestamp::try_from(1_000u64).unwrap(),
            RepeatableReason::InductiveRepeatableTimestamp,
        ),
        existing_writes: FunctionWrites::default(),
        function_metadata: Some(FunctionMetadata {
            path_and_args: sample_path_and_args(),
            journal: QueryJournal::new(),
        }),
        http: None,
        default_system_env_vars,
        in_memory_index_last_modified,
        context: ExecutionContext::new_from_parts(
            RequestId::new(),
            ExecutionId::new(),
            None,
            true,
            RequestMetadata::system(),
        ),
        bootstrap_metadata: BootstrapMetadata {
            tables_by_id: IndexRef::from_parts(
                IndexId(InternalId::from([1u8; 16])),
                PersistenceIndexId::new(1),
            ),
            index_by_id: IndexRef::from_parts(
                IndexId(InternalId::from([2u8; 16])),
                PersistenceIndexId::new(2),
            ),
            tables_tablet_id: TabletId(InternalId::from([3u8; 16])),
            index_tablet_id: TabletId(InternalId::from([4u8; 16])),
        },
        table_counts: Some(table_counts),
        deployment: DeploymentMetadata {
            name: "carnitas".to_string(),
            region: None,
            class: DeploymentClass::S16,
        },
        convex_origin: ConvexOrigin::from("http://127.0.0.1:3210"),
        subfunctions_in_same_isolate: true,
    }
}

pub fn sample_tablet() -> TabletId {
    TabletId(InternalId::from([6u8; 16]))
}

fn field_path(name: &str) -> FieldPath {
    FieldPath::new(vec![name.parse().unwrap()]).unwrap()
}

/// One indexed read (reserved `by_creation_time`, two intervals covering keys
/// prefixed 0x01 and 0x05) and one search read (non-reserved `search_body`).
pub fn sample_read_set() -> ReadSet {
    let mut intervals = IntervalSet::new();
    intervals.add(Interval::prefix(BinaryKey::from(vec![1u8])));
    intervals.add(Interval::prefix(BinaryKey::from(vec![5u8])));
    let indexed = BTreeMap::from([(
        TabletIndexName::by_creation_time(sample_tablet()),
        IndexReads {
            fields: IndexedFields::creation_time(),
            intervals,
            stack_traces: None,
        },
    )]);
    let search_reads = QueryReads::new(
        vec![
            TextQueryTermRead::new(field_path("body"), TextQueryTerm::Exact("hello".into())),
            TextQueryTermRead::new(field_path("body"), TextQueryTerm::Prefix("wor".into())),
        ]
        .into(),
        vec![FilterConditionRead::Must(
            field_path("author"),
            FilterValue::from_search_value(Some(&ConvexValue::try_from("alice").unwrap())),
        )]
        .into(),
    );
    let search = BTreeMap::from([(
        TabletIndexName::new(
            sample_tablet(),
            IndexDescriptor::new("search_body").unwrap(),
        )
        .unwrap(),
        search_reads,
    )]);
    ReadSet::new(indexed, search)
}

/// A document in `sample_tablet()` with the given `author` and `body` fields.
pub fn sample_document(author: &str, body: &str) -> PackedDocument {
    let id = ResolvedDocumentId {
        tablet_id: sample_tablet(),
        developer_id: DeveloperDocumentId::new(
            TableNumber::try_from(10001u32).unwrap(),
            InternalId::from([7u8; 16]),
        ),
    };
    let value = ConvexObject::try_from(BTreeMap::from([
        (
            "author".parse::<FieldName>().unwrap(),
            ConvexValue::try_from(author).unwrap(),
        ),
        (
            "body".parse::<FieldName>().unwrap(),
            ConvexValue::try_from(body).unwrap(),
        ),
    ]))
    .unwrap();
    PackedDocument::pack(
        &ResolvedDocument::new(id, CreationTime::try_from(1.0).unwrap(), value).unwrap(),
    )
}

pub fn sample_final_transaction() -> FunctionFinalTransaction {
    FunctionFinalTransaction {
        begin_timestamp: Timestamp::try_from(1_000u64).unwrap(),
        reads: FunctionReads {
            reads: sample_read_set(),
            num_intervals: 2,
            user_tx_size: TransactionReadSize {
                total_document_size: 300,
                total_document_count: 3,
            },
            system_tx_size: TransactionReadSize {
                total_document_size: 40,
                total_document_count: 1,
            },
        },
        writes: FunctionWrites::default(),
        rows_read_by_tablet: BTreeMap::from([(sample_tablet(), 3)]),
    }
}

pub fn sample_query_result() -> (
    FunctionFinalTransaction,
    FunctionOutcome,
    FunctionUsageStats,
    ValidatedPathAndArgs,
    InertIdentity,
) {
    let udf_outcome = pb::outcome::UdfOutcome {
        rng_seed: Some(vec![0u8; 32]),
        observed_rng: Some(false),
        unix_timestamp: Some(UnixTimestamp::from_millis(1_000).into()),
        observed_time: Some(false),
        log_lines: vec![],
        audit_log_lines: vec![],
        journal: Some(QueryJournal::new().into()),
        result: Some(pb::common::FunctionResult {
            result: Some(pb::common::function_result::Result::JsonPackedValue(
                "\"ok\"".to_string(),
            )),
        }),
        syscall_trace: Some(SyscallTrace::new().try_into().unwrap()),
        observed_identity: Some(false),
        memory_in_mb: 0,
        user_execution_time: None,
    };
    let outcome = FunctionOutcome::from_proto(
        pb::outcome::FunctionOutcome {
            outcome: Some(pb::outcome::function_outcome::Outcome::Query(udf_outcome)),
        },
        Some(sample_path_and_args()),
        None,
        InertIdentity::System,
    )
    .unwrap();
    (
        sample_final_transaction(),
        outcome,
        FunctionUsageStats::default(),
        sample_path_and_args(),
        InertIdentity::System,
    )
}

/// An `HttpAction` request with one header carrying two values.
pub fn sample_http_run_request_parts() -> RunRequestParts {
    let head = pb::common::HttpActionRequestHead {
        http_headers: vec![
            pb::common::HttpHeader {
                key: "x-multi".to_string(),
                value: b"a".to_vec(),
            },
            pb::common::HttpHeader {
                key: "x-multi".to_string(),
                value: b"b".to_vec(),
            },
        ],
        url: "http://127.0.0.1:3211/hello".to_string(),
        method: "POST".to_string(),
    };
    RunRequestParts {
        udf_type: UdfType::HttpAction,
        function_metadata: None,
        http: Some(HttpRequestParts {
            http_module_path: ValidatedHttpPath::from_proto(pb::common::ValidatedHttpPath {
                path: Some("http.js".to_string()),
                component_path: Some(pb::common::ComponentPath::default()),
                component_id: None,
                npm_version: None,
                reuse_context: None,
            })
            .unwrap(),
            routed_path: RoutedHttpPath("/hello".to_string()),
            head: HttpActionRequestHead::try_from(head).unwrap(),
            has_body: true,
        }),
        ..sample_run_request_parts()
    }
}
