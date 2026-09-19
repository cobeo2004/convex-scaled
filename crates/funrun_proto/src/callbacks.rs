//! `ActionCallbacks` calls and replies. One `CallbackCall` / `CallbackReply`
//! variant per trait method; replies hold the method's return value as-is.

use std::str::FromStr;

use anyhow::Context;
use common::{
    bootstrap_model::components::handles::FunctionHandle,
    components::{
        CanonicalizedComponentFunctionPath,
        ComponentId,
        ComponentPath,
    },
    execution_context::ExecutionContext,
    runtime::UnixTimestamp,
    types::{
        AttributedCaller,
        HttpActionRoute,
    },
};
use keybroker::Identity;
use model::file_storage::{
    types::FileStorageEntry,
    FileStorageId,
};
use pb_funrun::funrun::{
    action_callback_request::Call,
    action_callback_response::Result as ReplyProto,
    attributed_caller::Caller as CallerProto,
    ActionCallbackRequest,
    ActionCallbackResponse,
    ComponentFunctionPath,
    ExecuteUdf,
    StorageRef,
};
use serde_json::{
    value::RawValue,
    Value as JsonValue,
};
use sync_types::types::SerializedArgs;
use udf::FunctionResult;
use usage_tracking::FunctionUsageStats;
use value::DeveloperDocumentId;
use vector::PublicVectorSearchQueryResult;

pub enum CallbackCall {
    CreateAiGatewayToken {
        caller: AttributedCaller,
    },
    ExecuteQuery {
        path: CanonicalizedComponentFunctionPath,
        args: SerializedArgs,
        context: ExecutionContext,
    },
    ExecuteMutation {
        path: CanonicalizedComponentFunctionPath,
        args: SerializedArgs,
        context: ExecutionContext,
    },
    ExecuteAction {
        path: CanonicalizedComponentFunctionPath,
        args: SerializedArgs,
        context: ExecutionContext,
    },
    StorageGetUrl {
        component: ComponentId,
        storage_id: FileStorageId,
    },
    StorageDelete {
        component: ComponentId,
        storage_id: FileStorageId,
    },
    StorageGetFileEntry {
        component: ComponentId,
        storage_id: FileStorageId,
    },
    StorageStoreFileEntry {
        component: ComponentId,
        entry: FileStorageEntry,
    },
    ScheduleJob {
        scheduling_component: ComponentId,
        scheduled_path: CanonicalizedComponentFunctionPath,
        udf_args: SerializedArgs,
        scheduled_ts: UnixTimestamp,
        context: ExecutionContext,
    },
    CancelJob {
        virtual_id: DeveloperDocumentId,
    },
    VectorSearch {
        query: JsonValue,
    },
    LookupFunctionHandle {
        handle: FunctionHandle,
    },
    CreateFunctionHandle {
        path: CanonicalizedComponentFunctionPath,
    },
}

pub enum CallbackReply {
    CreateAiGatewayToken(String),
    ExecuteQuery(FunctionResult),
    ExecuteMutation(FunctionResult),
    ExecuteAction(FunctionResult),
    StorageGetUrl(Option<String>),
    StorageDelete,
    StorageGetFileEntry(Option<(ComponentPath, FileStorageEntry)>),
    StorageStoreFileEntry((ComponentPath, DeveloperDocumentId)),
    ScheduleJob(DeveloperDocumentId),
    CancelJob,
    VectorSearch((Vec<PublicVectorSearchQueryResult>, FunctionUsageStats)),
    LookupFunctionHandle(CanonicalizedComponentFunctionPath),
    CreateFunctionHandle(FunctionHandle),
}

fn path_to_proto(p: CanonicalizedComponentFunctionPath) -> ComponentFunctionPath {
    ComponentFunctionPath {
        component: String::from(p.component),
        udf_path: String::from(p.udf_path),
    }
}

fn path_from_proto(
    p: Option<ComponentFunctionPath>,
) -> anyhow::Result<CanonicalizedComponentFunctionPath> {
    let p = p.context("Missing path")?;
    Ok(CanonicalizedComponentFunctionPath {
        component: p.component.parse()?,
        udf_path: p.udf_path.parse()?,
    })
}

fn args_from_proto(json: String) -> anyhow::Result<SerializedArgs> {
    Ok(SerializedArgs::from_raw(RawValue::from_string(json)?))
}

fn execute_to_proto(
    path: CanonicalizedComponentFunctionPath,
    args: SerializedArgs,
    context: ExecutionContext,
) -> ExecuteUdf {
    ExecuteUdf {
        path: Some(path_to_proto(path)),
        args_json: args.get().to_owned(),
        context: Some(context.into()),
    }
}

fn execute_from_proto(
    p: ExecuteUdf,
) -> anyhow::Result<(
    CanonicalizedComponentFunctionPath,
    SerializedArgs,
    ExecutionContext,
)> {
    Ok((
        path_from_proto(p.path)?,
        args_from_proto(p.args_json)?,
        context_from_proto(p.context)?,
    ))
}

fn context_from_proto(p: Option<pb::common::ExecutionContext>) -> anyhow::Result<ExecutionContext> {
    ExecutionContext::try_from(p.context("Missing context")?)
}

fn component_from_proto(id: Option<String>) -> anyhow::Result<ComponentId> {
    ComponentId::deserialize_from_string(id.as_deref())
}

fn storage_ref_to_proto(component: ComponentId, storage_id: FileStorageId) -> StorageRef {
    StorageRef {
        component_id: component.serialize_to_string(),
        storage_id: Some(storage_id.into()),
    }
}

fn storage_ref_from_proto(p: StorageRef) -> anyhow::Result<(ComponentId, FileStorageId)> {
    Ok((
        component_from_proto(p.component_id)?,
        p.storage_id.context("Missing storage_id")?.try_into()?,
    ))
}

fn caller_to_proto(caller: AttributedCaller) -> pb_funrun::funrun::AttributedCaller {
    let (component_path, caller) = match caller {
        AttributedCaller::Action {
            component_path,
            udf_path,
        } => (component_path, CallerProto::ActionUdfPath(udf_path.into())),
        AttributedCaller::HttpAction {
            component_path,
            route,
        } => (
            component_path,
            CallerProto::HttpActionRoute(pb_funrun::funrun::HttpActionRoute {
                method: route.method.to_string(),
                path: route.path,
                matched: route.matched,
            }),
        ),
    };
    pb_funrun::funrun::AttributedCaller {
        component_path: component_path.into(),
        caller: Some(caller),
    }
}

fn caller_from_proto(p: pb_funrun::funrun::AttributedCaller) -> anyhow::Result<AttributedCaller> {
    let component_path = p.component_path.parse()?;
    Ok(match p.caller.context("Missing caller")? {
        CallerProto::ActionUdfPath(udf_path) => AttributedCaller::Action {
            component_path,
            udf_path: udf_path.parse()?,
        },
        CallerProto::HttpActionRoute(route) => AttributedCaller::HttpAction {
            component_path,
            route: HttpActionRoute {
                method: route.method.parse()?,
                path: route.path,
                matched: route.matched,
            },
        },
    })
}

pub fn callback_request_to_proto(
    identity: Identity,
    call: CallbackCall,
) -> anyhow::Result<ActionCallbackRequest> {
    let call = match call {
        CallbackCall::CreateAiGatewayToken { caller } => {
            Call::CreateAiGatewayToken(caller_to_proto(caller))
        },
        CallbackCall::ExecuteQuery {
            path,
            args,
            context,
        } => Call::ExecuteQuery(execute_to_proto(path, args, context)),
        CallbackCall::ExecuteMutation {
            path,
            args,
            context,
        } => Call::ExecuteMutation(execute_to_proto(path, args, context)),
        CallbackCall::ExecuteAction {
            path,
            args,
            context,
        } => Call::ExecuteAction(execute_to_proto(path, args, context)),
        CallbackCall::StorageGetUrl {
            component,
            storage_id,
        } => Call::StorageGetUrl(storage_ref_to_proto(component, storage_id)),
        CallbackCall::StorageDelete {
            component,
            storage_id,
        } => Call::StorageDelete(storage_ref_to_proto(component, storage_id)),
        CallbackCall::StorageGetFileEntry {
            component,
            storage_id,
        } => Call::StorageGetFileEntry(storage_ref_to_proto(component, storage_id)),
        CallbackCall::StorageStoreFileEntry { component, entry } => {
            Call::StorageStoreFileEntry(pb_funrun::funrun::StoreFileEntry {
                component_id: component.serialize_to_string(),
                entry: Some(entry.into()),
            })
        },
        CallbackCall::ScheduleJob {
            scheduling_component,
            scheduled_path,
            udf_args,
            scheduled_ts,
            context,
        } => Call::ScheduleJob(pb_funrun::funrun::ScheduleJob {
            scheduling_component_id: scheduling_component.serialize_to_string(),
            scheduled_path: Some(path_to_proto(scheduled_path)),
            args_json: udf_args.get().to_owned(),
            scheduled_ts_nanos: scheduled_ts
                .as_nanos()
                .try_into()
                .context("scheduled_ts out of range")?,
            context: Some(context.into()),
        }),
        CallbackCall::CancelJob { virtual_id } => Call::CancelJob(virtual_id.encode()),
        CallbackCall::VectorSearch { query } => {
            Call::VectorSearchJson(serde_json::to_string(&query)?)
        },
        CallbackCall::LookupFunctionHandle { handle } => {
            Call::LookupFunctionHandle(String::from(handle))
        },
        CallbackCall::CreateFunctionHandle { path } => {
            Call::CreateFunctionHandle(path_to_proto(path))
        },
    };
    Ok(ActionCallbackRequest {
        identity: Some(identity.try_into()?),
        call: Some(call),
    })
}

pub fn callback_request_from_proto(
    p: ActionCallbackRequest,
) -> anyhow::Result<(Identity, CallbackCall)> {
    let identity = Identity::from_proto_unchecked(p.identity.context("Missing identity")?)?;
    let call = match p.call.context("Missing call")? {
        Call::CreateAiGatewayToken(caller) => CallbackCall::CreateAiGatewayToken {
            caller: caller_from_proto(caller)?,
        },
        Call::ExecuteQuery(e) => {
            let (path, args, context) = execute_from_proto(e)?;
            CallbackCall::ExecuteQuery {
                path,
                args,
                context,
            }
        },
        Call::ExecuteMutation(e) => {
            let (path, args, context) = execute_from_proto(e)?;
            CallbackCall::ExecuteMutation {
                path,
                args,
                context,
            }
        },
        Call::ExecuteAction(e) => {
            let (path, args, context) = execute_from_proto(e)?;
            CallbackCall::ExecuteAction {
                path,
                args,
                context,
            }
        },
        Call::StorageGetUrl(r) => {
            let (component, storage_id) = storage_ref_from_proto(r)?;
            CallbackCall::StorageGetUrl {
                component,
                storage_id,
            }
        },
        Call::StorageDelete(r) => {
            let (component, storage_id) = storage_ref_from_proto(r)?;
            CallbackCall::StorageDelete {
                component,
                storage_id,
            }
        },
        Call::StorageGetFileEntry(r) => {
            let (component, storage_id) = storage_ref_from_proto(r)?;
            CallbackCall::StorageGetFileEntry {
                component,
                storage_id,
            }
        },
        Call::StorageStoreFileEntry(s) => CallbackCall::StorageStoreFileEntry {
            component: component_from_proto(s.component_id)?,
            entry: s.entry.context("Missing entry")?.try_into()?,
        },
        Call::ScheduleJob(s) => CallbackCall::ScheduleJob {
            scheduling_component: component_from_proto(s.scheduling_component_id)?,
            scheduled_path: path_from_proto(s.scheduled_path)?,
            udf_args: args_from_proto(s.args_json)?,
            scheduled_ts: UnixTimestamp::from_nanos(s.scheduled_ts_nanos),
            context: context_from_proto(s.context)?,
        },
        Call::CancelJob(id) => CallbackCall::CancelJob {
            virtual_id: DeveloperDocumentId::decode(&id)?,
        },
        Call::VectorSearchJson(json) => CallbackCall::VectorSearch {
            query: serde_json::from_str(&json)?,
        },
        Call::LookupFunctionHandle(handle) => CallbackCall::LookupFunctionHandle {
            handle: FunctionHandle::from_str(&handle)?,
        },
        Call::CreateFunctionHandle(path) => CallbackCall::CreateFunctionHandle {
            path: path_from_proto(Some(path))?,
        },
    };
    Ok((identity, call))
}

pub fn callback_reply_to_proto(reply: CallbackReply) -> anyhow::Result<ActionCallbackResponse> {
    let result = match reply {
        CallbackReply::CreateAiGatewayToken(token) => ReplyProto::CreateAiGatewayToken(token),
        CallbackReply::ExecuteQuery(r) => ReplyProto::ExecuteQuery(r.try_into()?),
        CallbackReply::ExecuteMutation(r) => ReplyProto::ExecuteMutation(r.try_into()?),
        CallbackReply::ExecuteAction(r) => ReplyProto::ExecuteAction(r.try_into()?),
        CallbackReply::StorageGetUrl(url) => {
            ReplyProto::StorageGetUrl(pb_funrun::funrun::OptionalString { value: url })
        },
        CallbackReply::StorageDelete => ReplyProto::StorageDelete(pb_funrun::funrun::Unit {}),
        CallbackReply::StorageGetFileEntry(entry) => {
            ReplyProto::StorageGetFileEntry(pb_funrun::funrun::OptionalFileEntry {
                entry: entry.map(|(component_path, entry)| pb_funrun::funrun::FileEntry {
                    component_path: component_path.into(),
                    entry: Some(entry.into()),
                }),
            })
        },
        CallbackReply::StorageStoreFileEntry((component_path, id)) => {
            ReplyProto::StorageStoreFileEntry(pb_funrun::funrun::StoredFile {
                component_path: component_path.into(),
                document_id: id.encode(),
            })
        },
        CallbackReply::ScheduleJob(id) => ReplyProto::ScheduleJob(id.encode()),
        CallbackReply::CancelJob => ReplyProto::CancelJob(pb_funrun::funrun::Unit {}),
        CallbackReply::VectorSearch((results, usage)) => {
            ReplyProto::VectorSearch(pb_funrun::funrun::VectorSearchResults {
                results: results
                    .into_iter()
                    .map(|r| pb_funrun::funrun::VectorSearchResult {
                        score: r.score,
                        id: r.id.encode(),
                    })
                    .collect(),
                usage: Some(usage.into()),
            })
        },
        CallbackReply::LookupFunctionHandle(path) => {
            ReplyProto::LookupFunctionHandle(path_to_proto(path))
        },
        CallbackReply::CreateFunctionHandle(handle) => {
            ReplyProto::CreateFunctionHandle(String::from(handle))
        },
    };
    Ok(ActionCallbackResponse {
        result: Some(result),
    })
}

pub fn callback_reply_from_proto(p: ActionCallbackResponse) -> anyhow::Result<CallbackReply> {
    Ok(match p.result.context("Missing result")? {
        ReplyProto::CreateAiGatewayToken(token) => CallbackReply::CreateAiGatewayToken(token),
        ReplyProto::ExecuteQuery(r) => CallbackReply::ExecuteQuery(r.try_into()?),
        ReplyProto::ExecuteMutation(r) => CallbackReply::ExecuteMutation(r.try_into()?),
        ReplyProto::ExecuteAction(r) => CallbackReply::ExecuteAction(r.try_into()?),
        ReplyProto::StorageGetUrl(url) => CallbackReply::StorageGetUrl(url.value),
        ReplyProto::StorageDelete(pb_funrun::funrun::Unit {}) => CallbackReply::StorageDelete,
        ReplyProto::StorageGetFileEntry(entry) => CallbackReply::StorageGetFileEntry(
            entry
                .entry
                .map(|e| -> anyhow::Result<_> {
                    Ok((
                        e.component_path.parse()?,
                        e.entry.context("Missing entry")?.try_into()?,
                    ))
                })
                .transpose()?,
        ),
        ReplyProto::StorageStoreFileEntry(stored) => CallbackReply::StorageStoreFileEntry((
            stored.component_path.parse()?,
            DeveloperDocumentId::decode(&stored.document_id)?,
        )),
        ReplyProto::ScheduleJob(id) => {
            CallbackReply::ScheduleJob(DeveloperDocumentId::decode(&id)?)
        },
        ReplyProto::CancelJob(pb_funrun::funrun::Unit {}) => CallbackReply::CancelJob,
        ReplyProto::VectorSearch(v) => CallbackReply::VectorSearch((
            v.results
                .into_iter()
                .map(|r| {
                    Ok(PublicVectorSearchQueryResult {
                        score: r.score,
                        id: DeveloperDocumentId::decode(&r.id)?,
                    })
                })
                .collect::<anyhow::Result<_>>()?,
            v.usage.context("Missing usage")?.try_into()?,
        )),
        ReplyProto::LookupFunctionHandle(path) => {
            CallbackReply::LookupFunctionHandle(path_from_proto(Some(path))?)
        },
        ReplyProto::CreateFunctionHandle(handle) => {
            CallbackReply::CreateFunctionHandle(FunctionHandle::from_str(&handle)?)
        },
    })
}

#[cfg(test)]
mod tests {
    use common::{
        components::{
            CanonicalizedComponentFunctionPath,
            ComponentId,
            ComponentPath,
        },
        runtime::UnixTimestamp,
        types::{
            AttributedCaller,
            HttpActionRoute,
            RoutableMethod,
        },
    };
    use keybroker::Identity;
    use model::file_storage::{
        types::FileStorageEntry,
        FileStorageId,
    };
    use serde_json::json;
    use sync_types::types::SerializedArgs;
    use udf::FunctionResult;
    use usage_tracking::FunctionUsageStats;
    use value::{
        DeveloperDocumentId,
        InternalId,
        JsonPackedValue,
        TableNumber,
    };
    use vector::PublicVectorSearchQueryResult;

    use super::*;
    use crate::test_samples::*;

    fn dev_id(byte: u8) -> DeveloperDocumentId {
        DeveloperDocumentId::new(
            TableNumber::try_from(10001u32).unwrap(),
            InternalId::from([byte; 16]),
        )
    }

    fn fn_path(component: &str) -> CanonicalizedComponentFunctionPath {
        CanonicalizedComponentFunctionPath {
            component: component.parse().unwrap(),
            udf_path: "messages.js:send".parse().unwrap(),
        }
    }

    fn file_entry() -> FileStorageEntry {
        FileStorageEntry::try_from(pb::storage::FileStorageEntry {
            storage_id: Some("7d4b7a0a-6d3a-4b1b-9f8d-2f5a1e3c9b10".to_string()),
            storage_key: Some("some-key".to_string()),
            sha256: Some(vec![3u8; 32]),
            size: Some(5),
            content_type: Some("text/plain".to_string()),
        })
        .unwrap()
    }

    fn assert_call_round_trips(call: CallbackCall) {
        let proto = callback_request_to_proto(Identity::system(), call).unwrap();
        let (identity, back) = callback_request_from_proto(proto.clone()).unwrap();
        assert_eq!(callback_request_to_proto(identity, back).unwrap(), proto);
    }

    fn assert_reply_round_trips(reply: CallbackReply) {
        let proto = callback_reply_to_proto(reply).unwrap();
        let back = callback_reply_from_proto(proto.clone()).unwrap();
        assert_eq!(callback_reply_to_proto(back).unwrap(), proto);
    }

    #[test]
    fn execute_mutation_call_round_trips() {
        assert_call_round_trips(CallbackCall::ExecuteMutation {
            path: fn_path("widgets/inner"),
            args: SerializedArgs::from_args(vec![json!({"a": 1})]).unwrap(),
            context: sample_context(),
        });
    }

    #[test]
    fn schedule_job_call_round_trips() {
        assert_call_round_trips(CallbackCall::ScheduleJob {
            scheduling_component: ComponentId::Child(dev_id(1)),
            scheduled_path: fn_path(""),
            udf_args: SerializedArgs::from_args(vec![json!({})]).unwrap(),
            scheduled_ts: UnixTimestamp::from_nanos(1_234_567_890),
            context: sample_context(),
        });
    }

    #[test]
    fn vector_search_call_round_trips() {
        assert_call_round_trips(CallbackCall::VectorSearch {
            query: json!({"indexName": "by_embedding", "vector": [0.5, 1.0], "limit": 3}),
        });
    }

    #[test]
    fn storage_get_url_root_call_round_trips() {
        let call = CallbackCall::StorageGetUrl {
            component: ComponentId::Root,
            storage_id: FileStorageId::DocumentId(dev_id(2)),
        };
        let proto = callback_request_to_proto(Identity::system(), call).unwrap();
        let Some(pb_funrun::funrun::action_callback_request::Call::StorageGetUrl(r)) = &proto.call
        else {
            panic!("wrong call variant");
        };
        assert_eq!(r.component_id, None);
        let (_, back) = callback_request_from_proto(proto.clone()).unwrap();
        let CallbackCall::StorageGetUrl { component, .. } = &back else {
            panic!("wrong call variant");
        };
        assert_eq!(*component, ComponentId::Root);
        assert_eq!(
            callback_request_to_proto(Identity::system(), back).unwrap(),
            proto
        );
    }

    #[test]
    fn remaining_calls_round_trip() {
        for call in [
            CallbackCall::CreateAiGatewayToken {
                caller: AttributedCaller::HttpAction {
                    component_path: ComponentPath::root(),
                    route: HttpActionRoute {
                        method: RoutableMethod::Post,
                        path: "/hook".to_string(),
                        matched: true,
                    },
                },
            },
            CallbackCall::CreateAiGatewayToken {
                caller: AttributedCaller::Action {
                    component_path: "widgets".parse().unwrap(),
                    udf_path: "a.js:b".parse().unwrap(),
                },
            },
            CallbackCall::StorageStoreFileEntry {
                component: ComponentId::Child(dev_id(3)),
                entry: file_entry(),
            },
            CallbackCall::CancelJob {
                virtual_id: dev_id(4),
            },
            CallbackCall::LookupFunctionHandle {
                handle: format!("function://{}#a.js:b", String::from(dev_id(5)))
                    .parse()
                    .unwrap(),
            },
            CallbackCall::CreateFunctionHandle {
                path: fn_path("widgets"),
            },
        ] {
            assert_call_round_trips(call);
        }
    }

    #[test]
    fn replies_round_trip() {
        let ok = || FunctionResult {
            result: Ok(JsonPackedValue::from_network("\"ok\"".to_string()).unwrap()),
        };
        for reply in [
            CallbackReply::ExecuteQuery(ok()),
            CallbackReply::ExecuteMutation(ok()),
            CallbackReply::ExecuteAction(ok()),
            CallbackReply::StorageGetUrl(None),
            CallbackReply::StorageGetUrl(Some("https://x/y".to_string())),
            CallbackReply::StorageDelete,
            CallbackReply::StorageGetFileEntry(None),
            CallbackReply::StorageGetFileEntry(Some(("widgets".parse().unwrap(), file_entry()))),
            CallbackReply::StorageStoreFileEntry((ComponentPath::root(), dev_id(6))),
            CallbackReply::ScheduleJob(dev_id(7)),
            CallbackReply::CancelJob,
            CallbackReply::VectorSearch((
                vec![PublicVectorSearchQueryResult {
                    score: 0.25,
                    id: dev_id(8),
                }],
                FunctionUsageStats::default(),
            )),
            CallbackReply::LookupFunctionHandle(fn_path("widgets")),
            CallbackReply::CreateFunctionHandle(
                format!("function://{}#a.js:b", String::from(dev_id(9)))
                    .parse()
                    .unwrap(),
            ),
            CallbackReply::CreateAiGatewayToken("tok".to_string()),
        ] {
            assert_reply_round_trips(reply);
        }
    }
}
