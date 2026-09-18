//! gRPC-backed adapters for the upstream traits `FunctionRunnerCore` needs.
//! The worker has no database: every read and callback is a `FunctionHost`
//! RPC to the conductor.

use std::collections::BTreeMap;

use async_trait::async_trait;
use common::{
    bootstrap_model::components::handles::FunctionHandle,
    components::{
        CanonicalizedComponentFunctionPath,
        ComponentId,
        ComponentPath,
    },
    document::DocumentUpdate,
    execution_context::ExecutionContext,
    interval::Interval,
    knobs::{
        MAX_FUNRUN_RUN_FUNCTION_REQUEST_MESSAGE_SIZE,
        MAX_FUNRUN_RUN_FUNCTION_RESPONSE_MESSAGE_SIZE,
    },
    query::{
        InternalSearch,
        Order,
        SearchVersion,
    },
    runtime::UnixTimestamp,
    types::{
        AttributedCaller,
        IndexRef,
        RepeatableTimestamp,
    },
};
use database::{
    TableCountSnapshot,
    TransactionTextSnapshot,
};
use funrun_proto::{
    auth::BearerInterceptor,
    callbacks::{
        callback_reply_from_proto,
        callback_request_to_proto,
        CallbackCall,
        CallbackReply,
    },
    index_page::{
        index_page_from_proto,
        index_page_request_to_proto,
    },
    search::{
        query_results_from_proto,
        text_search_request_to_proto,
        TextSearchArgs,
    },
};
use indexing::{
    index_reader::{
        IndexPage,
        IndexReader,
    },
    index_registry::Index,
};
use keybroker::Identity;
use model::file_storage::{
    types::FileStorageEntry,
    FileStorageId,
};
use pb::error_metadata::ErrorMetadataStatusExt;
use pb_funrun::funrun::function_host_client::FunctionHostClient;
use search::QueryResults;
use serde_json::Value as JsonValue;
use sync_types::types::SerializedArgs;
use tonic::{
    codegen::InterceptedService,
    transport::Channel,
};
use udf::{
    ActionCallbacks,
    FunctionResult,
};
use usage_tracking::FunctionUsageStats;
use value::{
    DeveloperDocumentId,
    TabletId,
};
use vector::PublicVectorSearchQueryResult;

use crate::metrics::log_index_page_rpc;

pub type HostChannel = FunctionHostClient<InterceptedService<Channel, BearerInterceptor>>;

pub async fn connect_host(url: &str, token: String) -> anyhow::Result<HostChannel> {
    let channel = Channel::from_shared(url.to_string())?.connect().await?;
    Ok(
        FunctionHostClient::with_interceptor(channel, BearerInterceptor { token })
            .max_encoding_message_size(*MAX_FUNRUN_RUN_FUNCTION_REQUEST_MESSAGE_SIZE)
            .max_decoding_message_size(*MAX_FUNRUN_RUN_FUNCTION_RESPONSE_MESSAGE_SIZE),
    )
}

/// Reads index pages at a fixed snapshot `ts` through the conductor.
pub struct RemoteIndexReader {
    client: HostChannel,
    ts: RepeatableTimestamp,
}

impl RemoteIndexReader {
    pub fn new(client: HostChannel, ts: RepeatableTimestamp) -> Self {
        Self { client, ts }
    }
}

#[async_trait]
impl IndexReader for RemoteIndexReader {
    async fn index_page(
        &self,
        index: IndexRef,
        tablet_id: TabletId,
        interval: &Interval,
        order: Order,
        max_results: usize,
    ) -> anyhow::Result<IndexPage> {
        let req =
            index_page_request_to_proto(self.ts, index, tablet_id, interval, order, max_results)?;
        log_index_page_rpc();
        let resp = self
            .client
            .clone()
            .index_page(req)
            .await
            .map_err(|s| s.into_anyhow())?;
        index_page_from_proto(resp.into_inner())
    }

    fn timestamp(&self) -> RepeatableTimestamp {
        self.ts
    }
}

/// Text search at a fixed snapshot `ts`. Only `index.id()` is sent; the
/// conductor resolves the `Index` itself.
pub struct RemoteTextSnapshot {
    client: HostChannel,
    ts: RepeatableTimestamp,
}

impl RemoteTextSnapshot {
    pub fn new(client: HostChannel, ts: RepeatableTimestamp) -> Self {
        Self { client, ts }
    }
}

#[async_trait]
impl TransactionTextSnapshot for RemoteTextSnapshot {
    async fn search(
        &self,
        index: &Index,
        search: &InternalSearch,
        version: SearchVersion,
        pending_updates: &Vec<DocumentUpdate>,
    ) -> anyhow::Result<QueryResults> {
        let req = text_search_request_to_proto(&TextSearchArgs {
            ts: self.ts,
            index_id: index.id(),
            search: search.clone(),
            version,
            pending_updates: pending_updates.clone(),
        })?;
        let resp = self
            .client
            .clone()
            .text_search(req)
            .await
            .map_err(|s| s.into_anyhow())?;
        query_results_from_proto(resp.into_inner())
    }
}

/// Table counts fetched up front with the run request. Same semantics as
/// upstream's `impl TableCountSnapshot for Option<TableCounts>`: no counts
/// -> `None`, a tablet missing from the counts -> `Some(0)`.
pub struct EagerTableCounts(pub Option<BTreeMap<TabletId, u64>>);

#[async_trait]
impl TableCountSnapshot for EagerTableCounts {
    async fn count(&self, table: TabletId) -> anyhow::Result<Option<u64>> {
        Ok(self
            .0
            .as_ref()
            .map(|counts| counts.get(&table).copied().unwrap_or(0)))
    }
}

pub struct RemoteActionCallbacks {
    client: HostChannel,
}

impl RemoteActionCallbacks {
    pub fn new(client: HostChannel) -> Self {
        Self { client }
    }

    async fn call(&self, identity: Identity, call: CallbackCall) -> anyhow::Result<CallbackReply> {
        let req = callback_request_to_proto(identity, call)?;
        let resp = self
            .client
            .clone()
            .action_callback(req)
            .await
            .map_err(|s| s.into_anyhow())?;
        callback_reply_from_proto(resp.into_inner())
    }
}

fn reply_name(reply: &CallbackReply) -> &'static str {
    match reply {
        CallbackReply::CreateAiGatewayToken(_) => "CreateAiGatewayToken",
        CallbackReply::ExecuteQuery(_) => "ExecuteQuery",
        CallbackReply::ExecuteMutation(_) => "ExecuteMutation",
        CallbackReply::ExecuteAction(_) => "ExecuteAction",
        CallbackReply::StorageGetUrl(_) => "StorageGetUrl",
        CallbackReply::StorageDelete => "StorageDelete",
        CallbackReply::StorageGetFileEntry(_) => "StorageGetFileEntry",
        CallbackReply::StorageStoreFileEntry(_) => "StorageStoreFileEntry",
        CallbackReply::ScheduleJob(_) => "ScheduleJob",
        CallbackReply::CancelJob => "CancelJob",
        CallbackReply::VectorSearch(_) => "VectorSearch",
        CallbackReply::LookupFunctionHandle(_) => "LookupFunctionHandle",
        CallbackReply::CreateFunctionHandle(_) => "CreateFunctionHandle",
    }
}

/// Unwraps the reply variant matching the call that was sent. Any other
/// variant is an error, never a silently wrong value.
macro_rules! expect_reply {
    ($reply:expr, $variant:ident) => {
        match $reply {
            CallbackReply::$variant(v) => Ok(v),
            other => Err(reply_mismatch(stringify!($variant), &other)),
        }
    };
    ($reply:expr, $variant:ident,unit) => {
        match $reply {
            CallbackReply::$variant => Ok(()),
            other => Err(reply_mismatch(stringify!($variant), &other)),
        }
    };
}

fn reply_mismatch(expected: &str, got: &CallbackReply) -> anyhow::Error {
    anyhow::anyhow!(
        "action callback reply mismatch: expected {expected}, got {}",
        reply_name(got)
    )
}

#[async_trait]
impl ActionCallbacks for RemoteActionCallbacks {
    async fn create_ai_gateway_token(
        &self,
        identity: Identity,
        caller: AttributedCaller,
    ) -> anyhow::Result<String> {
        let reply = self
            .call(identity, CallbackCall::CreateAiGatewayToken { caller })
            .await?;
        expect_reply!(reply, CreateAiGatewayToken)
    }

    async fn execute_query(
        &self,
        identity: Identity,
        path: CanonicalizedComponentFunctionPath,
        args: SerializedArgs,
        context: ExecutionContext,
    ) -> anyhow::Result<FunctionResult> {
        let reply = self
            .call(
                identity,
                CallbackCall::ExecuteQuery {
                    path,
                    args,
                    context,
                },
            )
            .await?;
        expect_reply!(reply, ExecuteQuery)
    }

    async fn execute_mutation(
        &self,
        identity: Identity,
        path: CanonicalizedComponentFunctionPath,
        args: SerializedArgs,
        context: ExecutionContext,
    ) -> anyhow::Result<FunctionResult> {
        let reply = self
            .call(
                identity,
                CallbackCall::ExecuteMutation {
                    path,
                    args,
                    context,
                },
            )
            .await?;
        expect_reply!(reply, ExecuteMutation)
    }

    async fn execute_action(
        &self,
        identity: Identity,
        path: CanonicalizedComponentFunctionPath,
        args: SerializedArgs,
        context: ExecutionContext,
    ) -> anyhow::Result<FunctionResult> {
        let reply = self
            .call(
                identity,
                CallbackCall::ExecuteAction {
                    path,
                    args,
                    context,
                },
            )
            .await?;
        expect_reply!(reply, ExecuteAction)
    }

    async fn storage_get_url(
        &self,
        identity: Identity,
        component: ComponentId,
        storage_id: FileStorageId,
    ) -> anyhow::Result<Option<String>> {
        let reply = self
            .call(
                identity,
                CallbackCall::StorageGetUrl {
                    component,
                    storage_id,
                },
            )
            .await?;
        expect_reply!(reply, StorageGetUrl)
    }

    async fn storage_delete(
        &self,
        identity: Identity,
        component: ComponentId,
        storage_id: FileStorageId,
    ) -> anyhow::Result<()> {
        let reply = self
            .call(
                identity,
                CallbackCall::StorageDelete {
                    component,
                    storage_id,
                },
            )
            .await?;
        expect_reply!(reply, StorageDelete, unit)
    }

    async fn storage_get_file_entry(
        &self,
        identity: Identity,
        component: ComponentId,
        storage_id: FileStorageId,
    ) -> anyhow::Result<Option<(ComponentPath, FileStorageEntry)>> {
        let reply = self
            .call(
                identity,
                CallbackCall::StorageGetFileEntry {
                    component,
                    storage_id,
                },
            )
            .await?;
        expect_reply!(reply, StorageGetFileEntry)
    }

    async fn storage_store_file_entry(
        &self,
        identity: Identity,
        component: ComponentId,
        entry: FileStorageEntry,
    ) -> anyhow::Result<(ComponentPath, DeveloperDocumentId)> {
        let reply = self
            .call(
                identity,
                CallbackCall::StorageStoreFileEntry { component, entry },
            )
            .await?;
        expect_reply!(reply, StorageStoreFileEntry)
    }

    async fn schedule_job(
        &self,
        identity: Identity,
        scheduling_component: ComponentId,
        scheduled_path: CanonicalizedComponentFunctionPath,
        udf_args: SerializedArgs,
        scheduled_ts: UnixTimestamp,
        context: ExecutionContext,
    ) -> anyhow::Result<DeveloperDocumentId> {
        let reply = self
            .call(
                identity,
                CallbackCall::ScheduleJob {
                    scheduling_component,
                    scheduled_path,
                    udf_args,
                    scheduled_ts,
                    context,
                },
            )
            .await?;
        expect_reply!(reply, ScheduleJob)
    }

    async fn cancel_job(
        &self,
        identity: Identity,
        virtual_id: DeveloperDocumentId,
    ) -> anyhow::Result<()> {
        let reply = self
            .call(identity, CallbackCall::CancelJob { virtual_id })
            .await?;
        expect_reply!(reply, CancelJob, unit)
    }

    async fn vector_search(
        &self,
        identity: Identity,
        query: JsonValue,
    ) -> anyhow::Result<(Vec<PublicVectorSearchQueryResult>, FunctionUsageStats)> {
        let reply = self
            .call(identity, CallbackCall::VectorSearch { query })
            .await?;
        expect_reply!(reply, VectorSearch)
    }

    async fn lookup_function_handle(
        &self,
        identity: Identity,
        handle: FunctionHandle,
    ) -> anyhow::Result<CanonicalizedComponentFunctionPath> {
        let reply = self
            .call(identity, CallbackCall::LookupFunctionHandle { handle })
            .await?;
        expect_reply!(reply, LookupFunctionHandle)
    }

    async fn create_function_handle(
        &self,
        identity: Identity,
        path: CanonicalizedComponentFunctionPath,
    ) -> anyhow::Result<FunctionHandle> {
        let reply = self
            .call(identity, CallbackCall::CreateFunctionHandle { path })
            .await?;
        expect_reply!(reply, CreateFunctionHandle)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matching_reply_is_unwrapped() {
        let reply = CallbackReply::StorageGetUrl(Some("u".to_string()));
        let url: anyhow::Result<Option<String>> = expect_reply!(reply, StorageGetUrl);
        assert_eq!(url.unwrap().as_deref(), Some("u"));
        let unit: anyhow::Result<()> = expect_reply!(CallbackReply::CancelJob, CancelJob, unit);
        unit.unwrap();
    }

    #[test]
    fn mismatched_reply_is_an_error() {
        let err: anyhow::Result<Option<String>> =
            expect_reply!(CallbackReply::CancelJob, StorageGetUrl);
        let msg = err.unwrap_err().to_string();
        assert!(msg.contains("expected StorageGetUrl"), "{msg}");
        assert!(msg.contains("got CancelJob"), "{msg}");

        let err: anyhow::Result<()> =
            expect_reply!(CallbackReply::StorageGetUrl(None), StorageDelete, unit);
        assert!(err.unwrap_err().to_string().contains("got StorageGetUrl"));
    }
}
