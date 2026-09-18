use std::{
    collections::BTreeMap,
    str::FromStr,
};

use anyhow::Context;
use common::{
    execution_context::ExecutionContext,
    http::RoutedHttpPath,
    query_journal::QueryJournal,
    types::{
        ConvexOrigin,
        DeploymentClass,
        DeploymentMetadata,
        EnvVarName,
        EnvVarValue,
        IndexId,
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
use pb::common::HttpHeader;
use udf::{
    validation::{
        ValidatedHttpPath,
        ValidatedPathAndArgs,
    },
    HttpActionRequestHead,
};
use value::TabletId;

use crate::{
    ids::{
        index_ref_from_proto,
        index_ref_to_proto,
        repeatable_ts_from_u64,
        tablet_id_from_bytes,
        tablet_id_to_bytes,
    },
    transaction::{
        writes_from_proto,
        writes_to_proto,
    },
};

/// The HTTP-action-specific parts of a `RunRequest`. Only present for
/// `UdfType::HttpAction` requests.
pub struct HttpRequestParts {
    pub http_module_path: ValidatedHttpPath,
    pub routed_path: RoutedHttpPath,
    pub head: HttpActionRequestHead,
    pub has_body: bool,
}

pub struct RunRequestParts {
    pub instance_name: String,
    pub udf_type: UdfType,
    pub identity: Identity,
    pub ts: RepeatableTimestamp,
    pub existing_writes: FunctionWrites,
    pub function_metadata: Option<FunctionMetadata>,
    pub http: Option<HttpRequestParts>,
    pub default_system_env_vars: BTreeMap<EnvVarName, EnvVarValue>,
    pub in_memory_index_last_modified: BTreeMap<IndexId, Timestamp>,
    pub context: ExecutionContext,
    pub bootstrap_metadata: BootstrapMetadata,
    pub table_counts: Option<BTreeMap<TabletId, u64>>,
    pub deployment: DeploymentMetadata,
    pub convex_origin: ConvexOrigin,
    pub subfunctions_in_same_isolate: bool,
}

fn udf_type_to_proto(udf_type: UdfType) -> i32 {
    pb::common::UdfType::from(udf_type) as i32
}

fn udf_type_from_proto(udf_type: i32) -> anyhow::Result<UdfType> {
    let proto = pb::common::UdfType::try_from(udf_type).context("Invalid UdfType")?;
    Ok(UdfType::from(proto))
}

fn deployment_metadata_to_proto(
    deployment: DeploymentMetadata,
) -> pb_funrun::funrun::DeploymentMetadata {
    pb_funrun::funrun::DeploymentMetadata {
        name: deployment.name,
        region: deployment.region.map(|r| r.to_string()),
        class: deployment.class.to_string(),
    }
}

fn deployment_metadata_from_proto(
    p: pb_funrun::funrun::DeploymentMetadata,
) -> anyhow::Result<DeploymentMetadata> {
    Ok(DeploymentMetadata {
        name: p.name,
        region: p.region.map(Into::into),
        class: DeploymentClass::from_str(&p.class).context("Invalid DeploymentClass")?,
    })
}

fn bootstrap_metadata_to_proto(b: BootstrapMetadata) -> pb_funrun::funrun::BootstrapMetadata {
    pb_funrun::funrun::BootstrapMetadata {
        tables_by_id: Some(index_ref_to_proto(b.tables_by_id)),
        index_by_id: Some(index_ref_to_proto(b.index_by_id)),
        tables_tablet_id: tablet_id_to_bytes(b.tables_tablet_id),
        index_tablet_id: tablet_id_to_bytes(b.index_tablet_id),
    }
}

fn bootstrap_metadata_from_proto(
    p: pb_funrun::funrun::BootstrapMetadata,
) -> anyhow::Result<BootstrapMetadata> {
    Ok(BootstrapMetadata {
        tables_by_id: index_ref_from_proto(p.tables_by_id.context("Missing tables_by_id")?)?,
        index_by_id: index_ref_from_proto(p.index_by_id.context("Missing index_by_id")?)?,
        tables_tablet_id: tablet_id_from_bytes(&p.tables_tablet_id)?,
        index_tablet_id: tablet_id_from_bytes(&p.index_tablet_id)?,
    })
}

fn http_request_head_to_proto(head: HttpActionRequestHead) -> pb::common::HttpActionRequestHead {
    pb::common::HttpActionRequestHead {
        http_headers: head
            .headers
            .iter()
            .map(|(name, value)| HttpHeader::from((name.clone(), value.clone())))
            .collect(),
        url: head.url.to_string(),
        method: head.method.to_string(),
    }
}

pub fn run_request_to_proto(
    parts: &RunRequestParts,
) -> anyhow::Result<pb_funrun::funrun::RunRequest> {
    let (path_and_args, journal) = match &parts.function_metadata {
        Some(metadata) => (
            Some(pb::common::ValidatedPathAndArgs::try_from(
                metadata.path_and_args.clone(),
            )?),
            Some(pb::convex_query_journal::QueryJournal::from(
                metadata.journal.clone(),
            )),
        ),
        None => (None, None),
    };
    let http = match &parts.http {
        Some(http) => Some(pb_funrun::funrun::HttpActionMetadata {
            http_module_path: Some(pb::common::ValidatedHttpPath::try_from(
                http.http_module_path.clone(),
            )?),
            routed_path: http.routed_path.0.clone(),
            head: Some(http_request_head_to_proto(http.head.clone())),
            has_body: http.has_body,
        }),
        None => None,
    };
    let table_counts = parts
        .table_counts
        .as_ref()
        .map(|counts| pb_funrun::funrun::TableCounts {
            tables: counts
                .iter()
                .map(|(tablet_id, count)| pb_funrun::funrun::TableCount {
                    tablet_id: tablet_id_to_bytes(*tablet_id),
                    count: *count,
                })
                .collect(),
        });
    Ok(pb_funrun::funrun::RunRequest {
        instance_name: parts.instance_name.clone(),
        udf_type: udf_type_to_proto(parts.udf_type),
        identity: Some(pb::convex_identity::UncheckedIdentity::try_from(
            parts.identity.clone(),
        )?),
        ts: u64::from(*parts.ts),
        existing_writes: Some(writes_to_proto(&parts.existing_writes)?),
        path_and_args,
        journal,
        http,
        default_system_env_vars: parts
            .default_system_env_vars
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect(),
        in_memory_index_last_modified: parts
            .in_memory_index_last_modified
            .iter()
            .map(
                |(index_id, ts)| pb_funrun::funrun::InMemoryIndexLastModified {
                    index_id: crate::ids::index_id_to_bytes(*index_id),
                    last_modified: u64::from(*ts),
                },
            )
            .collect(),
        context: Some(pb::common::ExecutionContext::from(parts.context.clone())),
        bootstrap_metadata: Some(bootstrap_metadata_to_proto(
            parts.bootstrap_metadata.clone(),
        )),
        table_counts,
        deployment: Some(deployment_metadata_to_proto(parts.deployment.clone())),
        convex_origin: parts.convex_origin.to_string(),
        subfunctions_in_same_isolate: parts.subfunctions_in_same_isolate,
    })
}

pub fn run_request_from_proto(
    proto: pb_funrun::funrun::RunRequest,
) -> anyhow::Result<RunRequestParts> {
    let function_metadata = match (proto.path_and_args, proto.journal) {
        (Some(path_and_args), Some(journal)) => Some(FunctionMetadata {
            path_and_args: ValidatedPathAndArgs::from_proto(path_and_args)?,
            journal: QueryJournal::try_from(journal)?,
        }),
        (None, None) => None,
        _ => anyhow::bail!("path_and_args and journal must both be present or both absent"),
    };
    let http = match proto.http {
        Some(http) => Some(HttpRequestParts {
            http_module_path: ValidatedHttpPath::from_proto(
                http.http_module_path.context("Missing http_module_path")?,
            )?,
            routed_path: RoutedHttpPath(http.routed_path),
            head: HttpActionRequestHead::try_from(http.head.context("Missing head")?)?,
            has_body: http.has_body,
        }),
        None => None,
    };
    let table_counts = proto
        .table_counts
        .map(|counts| {
            counts
                .tables
                .into_iter()
                .map(|table_count| {
                    anyhow::Ok((
                        tablet_id_from_bytes(&table_count.tablet_id)?,
                        table_count.count,
                    ))
                })
                .collect::<anyhow::Result<BTreeMap<_, _>>>()
        })
        .transpose()?;
    let default_system_env_vars = proto
        .default_system_env_vars
        .into_iter()
        .map(|(name, value)| {
            anyhow::Ok((EnvVarName::from_str(&name)?, EnvVarValue::from_str(&value)?))
        })
        .collect::<anyhow::Result<BTreeMap<_, _>>>()?;
    let in_memory_index_last_modified = proto
        .in_memory_index_last_modified
        .into_iter()
        .map(|entry| {
            anyhow::Ok((
                crate::ids::index_id_from_bytes(&entry.index_id)?,
                Timestamp::try_from(entry.last_modified)?,
            ))
        })
        .collect::<anyhow::Result<BTreeMap<_, _>>>()?;
    Ok(RunRequestParts {
        instance_name: proto.instance_name,
        udf_type: udf_type_from_proto(proto.udf_type)?,
        identity: Identity::from_proto_unchecked(proto.identity.context("Missing identity")?)?,
        ts: repeatable_ts_from_u64(proto.ts)?,
        existing_writes: writes_from_proto(
            proto.existing_writes.context("Missing existing_writes")?,
        )?,
        function_metadata,
        http,
        default_system_env_vars,
        in_memory_index_last_modified,
        context: ExecutionContext::try_from(proto.context.context("Missing context")?)?,
        bootstrap_metadata: bootstrap_metadata_from_proto(
            proto
                .bootstrap_metadata
                .context("Missing bootstrap_metadata")?,
        )?,
        table_counts,
        deployment: deployment_metadata_from_proto(
            proto.deployment.context("Missing deployment")?,
        )?,
        convex_origin: ConvexOrigin::from(proto.convex_origin),
        subfunctions_in_same_isolate: proto.subfunctions_in_same_isolate,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> RunRequestParts {
        crate::test_samples::sample_run_request_parts()
    }

    #[test]
    fn run_request_round_trips() {
        let parts = sample();
        let proto = run_request_to_proto(&parts).unwrap();
        let back = run_request_from_proto(proto.clone()).unwrap();
        // Compare via proto encoding: many upstream types lack PartialEq.
        assert_eq!(run_request_to_proto(&back).unwrap(), proto);
    }

    #[test]
    fn missing_bootstrap_metadata_is_an_error() {
        let mut proto = run_request_to_proto(&sample()).unwrap();
        proto.bootstrap_metadata = None;
        assert!(run_request_from_proto(proto).is_err());
    }
}
