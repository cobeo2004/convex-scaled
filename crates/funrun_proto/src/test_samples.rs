use std::{
    collections::BTreeMap,
    str::FromStr,
};

use common::{
    execution_context::{
        ExecutionContext,
        ExecutionId,
        RequestId,
        RequestMetadata,
    },
    query_journal::QueryJournal,
    types::{
        ConvexOrigin,
        DeploymentClass,
        DeploymentMetadata,
        EnvVarName,
        EnvVarValue,
        IndexId,
        IndexRef,
        PersistenceIndexId,
        RepeatableReason,
        RepeatableTimestamp,
        Timestamp,
        UdfType,
    },
};
use database::BootstrapMetadata;
use function_runner::{
    server::FunctionMetadata,
    FunctionWrites,
};
use keybroker::Identity;
use udf::validation::ValidatedPathAndArgs;
use value::{
    InternalId,
    TabletId,
};

use crate::request::RunRequestParts;

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
