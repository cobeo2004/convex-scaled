//! Conductor-side gRPC server that answers worker callbacks: index pages,
//! text search and `ActionCallbacks`. It is a trust boundary: every RPC checks
//! the funrun bearer token before touching any dependency.

use std::{
    future::Future,
    sync::{
        Arc,
        Weak,
    },
};

use common::{
    grpc::ConvexGrpcService,
    http::MakeSocket,
    knobs::{
        MAX_FUNRUN_RUN_FUNCTION_REQUEST_MESSAGE_SIZE,
        MAX_FUNRUN_RUN_FUNCTION_RESPONSE_MESSAGE_SIZE,
    },
    types::{
        IndexId,
        RepeatableTimestamp,
    },
};
use database::TransactionTextSnapshot;
use funrun_proto::{
    auth::{
        check_bearer,
        check_protocol,
    },
    callbacks::{
        callback_reply_to_proto,
        callback_request_from_proto,
        CallbackCall,
        CallbackReply,
    },
    index_page::{
        index_page_request_from_proto,
        index_page_to_proto,
    },
    search::{
        query_results_to_proto,
        text_search_request_from_proto,
    },
};
use indexing::{
    index_reader::IndexReader,
    index_registry::Index,
};
use parking_lot::RwLock;
use pb::error_metadata::ErrorMetadataStatusExt;
use pb_funrun::funrun::{
    function_host_server::{
        FunctionHost as FunctionHostService,
        FunctionHostServer,
    },
    ActionCallbackRequest,
    ActionCallbackResponse,
    IndexPageRequest,
    IndexPageResponse,
    TextSearchRequest,
    TextSearchResponse,
};
use tonic::{
    Request,
    Response,
    Status,
};
use udf::ActionCallbacks;

pub type IndexReaderAt =
    Arc<dyn Fn(RepeatableTimestamp) -> anyhow::Result<Arc<dyn IndexReader>> + Send + Sync>;
pub type TextSnapshotAt = Arc<
    dyn Fn(RepeatableTimestamp) -> anyhow::Result<Arc<dyn TransactionTextSnapshot>> + Send + Sync,
>;
/// Resolves an index id to the `Index` metadata at `ts` for text search.
pub type IndexAt = Arc<dyn Fn(RepeatableTimestamp, IndexId) -> anyhow::Result<Index> + Send + Sync>;

pub struct FunctionHost {
    index_reader_at: IndexReaderAt,
    text_snapshot_at: TextSnapshotAt,
    index_at: IndexAt,
    /// Weak so the host never keeps `Application` alive.
    action_callbacks: RwLock<Option<Weak<dyn ActionCallbacks>>>,
    token: String,
    /// IndexPage responses stop adding entries past this many bytes: a page
    /// of large documents could otherwise exceed the gRPC message limit.
    index_page_max_bytes: usize,
}

impl FunctionHost {
    pub fn new(
        index_reader_at: IndexReaderAt,
        text_snapshot_at: TextSnapshotAt,
        index_at: IndexAt,
        token: String,
    ) -> Self {
        Self {
            index_reader_at,
            text_snapshot_at,
            index_at,
            action_callbacks: RwLock::new(None),
            token,
            index_page_max_bytes: *MAX_FUNRUN_RUN_FUNCTION_RESPONSE_MESSAGE_SIZE / 2,
        }
    }

    pub fn with_index_page_max_bytes(mut self, index_page_max_bytes: usize) -> Self {
        self.index_page_max_bytes = index_page_max_bytes;
        self
    }

    pub fn set_action_callbacks(&self, cb: Arc<dyn ActionCallbacks>) {
        *self.action_callbacks.write() = Some(Arc::downgrade(&cb));
    }

    /// Serves until `shutdown` resolves.
    pub async fn serve(
        self: Arc<Self>,
        addr: impl MakeSocket,
        shutdown: impl Future<Output = ()> + Send + 'static,
    ) -> anyhow::Result<()> {
        let service = FunctionHostServer::from_arc(self)
            .max_decoding_message_size(*MAX_FUNRUN_RUN_FUNCTION_REQUEST_MESSAGE_SIZE)
            .max_encoding_message_size(*MAX_FUNRUN_RUN_FUNCTION_RESPONSE_MESSAGE_SIZE);
        ConvexGrpcService::new()
            .add_service(service)
            .serve(addr, shutdown)
            .await
    }

    async fn index_page(&self, req: IndexPageRequest) -> anyhow::Result<IndexPageResponse> {
        let args = index_page_request_from_proto(req)?;
        let page = (self.index_reader_at)(args.ts)?
            .index_page(
                args.index,
                args.tablet_id,
                &args.interval,
                args.order,
                args.max_results,
            )
            .await?;
        index_page_to_proto(&page, self.index_page_max_bytes)
    }

    async fn text_search(&self, req: TextSearchRequest) -> anyhow::Result<TextSearchResponse> {
        let args = text_search_request_from_proto(req)?;
        let index = (self.index_at)(args.ts, args.index_id)?;
        let results = (self.text_snapshot_at)(args.ts)?
            .search(&index, &args.search, args.version, &args.pending_updates)
            .await?;
        Ok(query_results_to_proto(&results))
    }

    async fn action_callback(
        cb: Arc<dyn ActionCallbacks>,
        req: ActionCallbackRequest,
    ) -> anyhow::Result<ActionCallbackResponse> {
        let (identity, call) = callback_request_from_proto(req)?;
        let reply = match call {
            CallbackCall::CreateAiGatewayToken { caller } => CallbackReply::CreateAiGatewayToken(
                cb.create_ai_gateway_token(identity, caller).await?,
            ),
            CallbackCall::ExecuteQuery {
                path,
                args,
                context,
            } => {
                CallbackReply::ExecuteQuery(cb.execute_query(identity, path, args, context).await?)
            },
            CallbackCall::ExecuteMutation {
                path,
                args,
                context,
            } => CallbackReply::ExecuteMutation(
                cb.execute_mutation(identity, path, args, context).await?,
            ),
            CallbackCall::ExecuteAction {
                path,
                args,
                context,
            } => CallbackReply::ExecuteAction(
                cb.execute_action(identity, path, args, context).await?,
            ),
            CallbackCall::StorageGetUrl {
                component,
                storage_id,
            } => CallbackReply::StorageGetUrl(
                cb.storage_get_url(identity, component, storage_id).await?,
            ),
            CallbackCall::StorageDelete {
                component,
                storage_id,
            } => {
                cb.storage_delete(identity, component, storage_id).await?;
                CallbackReply::StorageDelete
            },
            CallbackCall::StorageGetFileEntry {
                component,
                storage_id,
            } => CallbackReply::StorageGetFileEntry(
                cb.storage_get_file_entry(identity, component, storage_id)
                    .await?,
            ),
            CallbackCall::StorageStoreFileEntry { component, entry } => {
                CallbackReply::StorageStoreFileEntry(
                    cb.storage_store_file_entry(identity, component, entry)
                        .await?,
                )
            },
            CallbackCall::ScheduleJob {
                scheduling_component,
                scheduled_path,
                udf_args,
                scheduled_ts,
                context,
            } => CallbackReply::ScheduleJob(
                cb.schedule_job(
                    identity,
                    scheduling_component,
                    scheduled_path,
                    udf_args,
                    scheduled_ts,
                    context,
                )
                .await?,
            ),
            CallbackCall::CancelJob { virtual_id } => {
                cb.cancel_job(identity, virtual_id).await?;
                CallbackReply::CancelJob
            },
            CallbackCall::VectorSearch { query } => {
                CallbackReply::VectorSearch(cb.vector_search(identity, query).await?)
            },
            CallbackCall::LookupFunctionHandle { handle } => CallbackReply::LookupFunctionHandle(
                cb.lookup_function_handle(identity, handle).await?,
            ),
            CallbackCall::CreateFunctionHandle { path } => CallbackReply::CreateFunctionHandle(
                cb.create_function_handle(identity, path).await?,
            ),
        };
        callback_reply_to_proto(reply)
    }
}

// Errors go through `Status::from_anyhow` so `ErrorMetadata` (user errors, OCC,
// overloaded, ...) survives the hop back to the worker.
#[tonic::async_trait]
impl FunctionHostService for FunctionHost {
    async fn index_page(
        &self,
        req: Request<IndexPageRequest>,
    ) -> Result<Response<IndexPageResponse>, Status> {
        check_bearer(req.metadata(), &self.token)?;
        check_protocol(req.metadata())?;
        self.index_page(req.into_inner())
            .await
            .map(Response::new)
            .map_err(Status::from_anyhow)
    }

    async fn text_search(
        &self,
        req: Request<TextSearchRequest>,
    ) -> Result<Response<TextSearchResponse>, Status> {
        check_bearer(req.metadata(), &self.token)?;
        check_protocol(req.metadata())?;
        self.text_search(req.into_inner())
            .await
            .map(Response::new)
            .map_err(Status::from_anyhow)
    }

    async fn action_callback(
        &self,
        req: Request<ActionCallbackRequest>,
    ) -> Result<Response<ActionCallbackResponse>, Status> {
        check_bearer(req.metadata(), &self.token)?;
        check_protocol(req.metadata())?;
        let cb = self
            .action_callbacks
            .read()
            .as_ref()
            .and_then(Weak::upgrade)
            .ok_or_else(|| Status::unavailable("action callbacks are not available"))?;
        Self::action_callback(cb, req.into_inner())
            .await
            .map(Response::new)
            .map_err(Status::from_anyhow)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        net::SocketAddr,
        sync::Arc,
    };

    use async_trait::async_trait;
    use common::{
        bootstrap_model::components::handles::FunctionHandle,
        components::{
            CanonicalizedComponentFunctionPath,
            ComponentId,
            ComponentPath,
        },
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
        index::IndexKeyBytes,
        interval::Interval,
        query::{
            CursorPosition,
            Order,
        },
        runtime::UnixTimestamp,
        types::{
            AttributedCaller,
            IndexId,
            IndexRef,
            PersistenceIndexId,
            RepeatableTimestamp,
            Timestamp,
        },
    };
    use errors::{
        ErrorMetadata,
        ErrorMetadataAnyhowExt,
    };
    use funrun_proto::{
        auth::{
            host_token,
            worker_token,
            BearerInterceptor,
            AUTHORIZATION,
            PROTOCOL_HEADER,
        },
        callbacks::{
            callback_request_to_proto,
            CallbackCall,
        },
        ids::repeatable_ts_from_u64,
        index_page::index_page_request_to_proto,
    };
    use indexing::index_reader::{
        IndexEntry,
        IndexPage,
        IndexReader,
    };
    use keybroker::Identity;
    use model::file_storage::{
        types::FileStorageEntry,
        FileStorageId,
    };
    use pb::error_metadata::ErrorMetadataStatusExt;
    use pb_funrun::funrun::{
        action_callback_response,
        function_host_client::FunctionHostClient,
        ActionCallbackRequest,
        IndexPageRequest,
    };
    use serde_json::{
        json,
        Value as JsonValue,
    };
    use sync_types::types::SerializedArgs;
    use tokio::{
        net::TcpSocket,
        sync::oneshot,
    };
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
        ConvexObject,
        DeveloperDocumentId,
        InternalId,
        JsonPackedValue,
        ResolvedDocumentId,
        TableNumber,
        TabletId,
    };
    use vector::PublicVectorSearchQueryResult;

    use super::*;

    struct FakeIndexReader(RepeatableTimestamp);

    #[async_trait]
    impl IndexReader for FakeIndexReader {
        async fn index_page(
            &self,
            _index: IndexRef,
            tablet_id: TabletId,
            _interval: &Interval,
            _order: Order,
            _max_results: usize,
        ) -> anyhow::Result<IndexPage> {
            let id = ResolvedDocumentId {
                tablet_id,
                developer_id: DeveloperDocumentId::new(
                    TableNumber::try_from(10001u32)?,
                    InternalId::from([7u8; 16]),
                ),
            };
            let doc =
                ResolvedDocument::new(id, CreationTime::try_from(1.0)?, ConvexObject::empty())?;
            Ok(IndexPage {
                entries: vec![Arc::new(IndexEntry {
                    key: IndexKeyBytes(vec![1]),
                    ts: Timestamp::MIN,
                    value: PackedDocument::pack(&doc),
                })],
                cursor: CursorPosition::End,
            })
        }

        fn timestamp(&self) -> RepeatableTimestamp {
            self.0
        }
    }

    /// `execute_mutation` succeeds, `execute_query` fails with a user-facing
    /// `ErrorMetadata`, everything else is unused.
    struct FakeCallbacks;

    #[async_trait]
    impl ActionCallbacks for FakeCallbacks {
        async fn create_ai_gateway_token(
            &self,
            _: Identity,
            _: AttributedCaller,
        ) -> anyhow::Result<String> {
            anyhow::bail!("unused")
        }

        async fn execute_query(
            &self,
            _: Identity,
            _: CanonicalizedComponentFunctionPath,
            _: SerializedArgs,
            _: ExecutionContext,
        ) -> anyhow::Result<FunctionResult> {
            Err(ErrorMetadata::bad_request("FakeBadRequest", "fake bad request").into())
        }

        async fn execute_mutation(
            &self,
            _: Identity,
            _: CanonicalizedComponentFunctionPath,
            _: SerializedArgs,
            _: ExecutionContext,
        ) -> anyhow::Result<FunctionResult> {
            Ok(FunctionResult {
                result: Ok(JsonPackedValue::from_network("\"mutated\"".to_string())?),
            })
        }

        async fn execute_action(
            &self,
            _: Identity,
            _: CanonicalizedComponentFunctionPath,
            _: SerializedArgs,
            _: ExecutionContext,
        ) -> anyhow::Result<FunctionResult> {
            anyhow::bail!("unused")
        }

        async fn storage_get_url(
            &self,
            _: Identity,
            _: ComponentId,
            _: FileStorageId,
        ) -> anyhow::Result<Option<String>> {
            anyhow::bail!("unused")
        }

        async fn storage_delete(
            &self,
            _: Identity,
            _: ComponentId,
            _: FileStorageId,
        ) -> anyhow::Result<()> {
            anyhow::bail!("unused")
        }

        async fn storage_get_file_entry(
            &self,
            _: Identity,
            _: ComponentId,
            _: FileStorageId,
        ) -> anyhow::Result<Option<(ComponentPath, FileStorageEntry)>> {
            anyhow::bail!("unused")
        }

        async fn storage_store_file_entry(
            &self,
            _: Identity,
            _: ComponentId,
            _: FileStorageEntry,
        ) -> anyhow::Result<(ComponentPath, DeveloperDocumentId)> {
            anyhow::bail!("unused")
        }

        async fn schedule_job(
            &self,
            _: Identity,
            _: ComponentId,
            _: CanonicalizedComponentFunctionPath,
            _: SerializedArgs,
            _: UnixTimestamp,
            _: ExecutionContext,
        ) -> anyhow::Result<DeveloperDocumentId> {
            anyhow::bail!("unused")
        }

        async fn cancel_job(&self, _: Identity, _: DeveloperDocumentId) -> anyhow::Result<()> {
            anyhow::bail!("unused")
        }

        async fn vector_search(
            &self,
            _: Identity,
            _: JsonValue,
        ) -> anyhow::Result<(Vec<PublicVectorSearchQueryResult>, FunctionUsageStats)> {
            anyhow::bail!("unused")
        }

        async fn lookup_function_handle(
            &self,
            _: Identity,
            _: FunctionHandle,
        ) -> anyhow::Result<CanonicalizedComponentFunctionPath> {
            anyhow::bail!("unused")
        }

        async fn create_function_handle(
            &self,
            _: Identity,
            _: CanonicalizedComponentFunctionPath,
        ) -> anyhow::Result<FunctionHandle> {
            anyhow::bail!("unused")
        }
    }

    struct TestHost {
        addr: SocketAddr,
        host: Arc<FunctionHost>,
        // Dropping this sender stops the server.
        _shutdown: oneshot::Sender<()>,
    }

    async fn start_host() -> TestHost {
        let index_reader_at: IndexReaderAt =
            Arc::new(|ts| Ok(Arc::new(FakeIndexReader(ts)) as Arc<dyn IndexReader>));
        let text_snapshot_at: TextSnapshotAt = Arc::new(|_| anyhow::bail!("unused"));
        let index_at: IndexAt = Arc::new(|_, _| anyhow::bail!("unused"));
        let host = Arc::new(FunctionHost::new(
            index_reader_at,
            text_snapshot_at,
            index_at,
            host_token("secret"),
        ));
        let socket = TcpSocket::new_v4().unwrap();
        socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = socket.local_addr().unwrap();
        let (tx, rx) = oneshot::channel();
        let shutdown = async {
            let _ = rx.await;
        };
        tokio::spawn(host.clone().serve(socket, shutdown));
        TestHost {
            addr,
            host,
            _shutdown: tx,
        }
    }

    async fn connect(
        addr: SocketAddr,
        token: &str,
    ) -> FunctionHostClient<InterceptedService<Channel, BearerInterceptor>> {
        // The socket is bound before `serve` is spawned, but only listens once
        // the task runs; connect lazily so the first RPC waits for it.
        let channel = Channel::from_shared(format!("http://{addr}"))
            .unwrap()
            .connect_lazy();
        FunctionHostClient::with_interceptor(
            channel,
            BearerInterceptor {
                token: token.to_string(),
            },
        )
    }

    fn sample_index_page_request() -> IndexPageRequest {
        index_page_request_to_proto(
            repeatable_ts_from_u64(1_000).unwrap(),
            IndexRef::from_parts(
                IndexId(InternalId::from([2u8; 16])),
                PersistenceIndexId::new(2),
            ),
            TabletId(InternalId::from([6u8; 16])),
            &Interval::all(),
            Order::Asc,
            10,
        )
        .unwrap()
    }

    fn sample_callback(
        call: impl FnOnce(ExecutionContext) -> CallbackCall,
    ) -> ActionCallbackRequest {
        let context = ExecutionContext::new_from_parts(
            RequestId::new(),
            ExecutionId::new(),
            None,
            true,
            RequestMetadata::system(),
        );
        callback_request_to_proto(Identity::system(), call(context)).unwrap()
    }

    fn fn_path() -> CanonicalizedComponentFunctionPath {
        CanonicalizedComponentFunctionPath {
            component: ComponentPath::root(),
            udf_path: "messages.js:send".parse().unwrap(),
        }
    }

    fn sample_mutation_callback() -> ActionCallbackRequest {
        sample_callback(|context| CallbackCall::ExecuteMutation {
            path: fn_path(),
            args: SerializedArgs::from_args(vec![json!({})]).unwrap(),
            context,
        })
    }

    fn sample_query_callback() -> ActionCallbackRequest {
        sample_callback(|context| CallbackCall::ExecuteQuery {
            path: fn_path(),
            args: SerializedArgs::from_args(vec![json!({})]).unwrap(),
            context,
        })
    }

    #[tokio::test]
    async fn index_page_round_trip_over_grpc() {
        let t = start_host().await;
        let mut client = connect(t.addr, &t.host.token).await;
        let resp = client
            .index_page(sample_index_page_request())
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.entries.len(), 1);
    }

    #[tokio::test]
    async fn wrong_token_is_unauthenticated() {
        let t = start_host().await;
        let mut client = connect(t.addr, &host_token("wrong")).await;
        let err = client
            .index_page(sample_index_page_request())
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[tokio::test]
    async fn worker_with_another_protocol_version_is_rejected() {
        let t = start_host().await;
        let channel = Channel::from_shared(format!("http://{}", t.addr))
            .unwrap()
            .connect_lazy();
        let token = format!("Bearer {}", host_token("secret"));
        let mut client =
            FunctionHostClient::with_interceptor(channel, move |mut req: Request<()>| {
                req.metadata_mut()
                    .insert(AUTHORIZATION, token.parse().unwrap());
                req.metadata_mut()
                    .insert(PROTOCOL_HEADER, "999".parse().unwrap());
                Ok(req)
            });
        let err = client
            .index_page(sample_index_page_request())
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    }

    #[tokio::test]
    async fn conductor_to_worker_token_is_unauthenticated() {
        let t = start_host().await;
        let mut client = connect(t.addr, &worker_token("secret")).await;
        let err = client
            .index_page(sample_index_page_request())
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[tokio::test]
    async fn callbacks_unset_is_unavailable() {
        let t = start_host().await;
        let mut client = connect(t.addr, &t.host.token).await;
        let err = client
            .action_callback(sample_mutation_callback())
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unavailable);
    }

    #[tokio::test]
    async fn callbacks_dropped_is_unavailable() {
        let t = start_host().await;
        t.host.set_action_callbacks(Arc::new(FakeCallbacks));
        let mut client = connect(t.addr, &t.host.token).await;
        let err = client
            .action_callback(sample_mutation_callback())
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unavailable);
    }

    #[tokio::test]
    async fn execute_mutation_callback_reaches_action_callbacks() {
        let t = start_host().await;
        let callbacks: Arc<dyn ActionCallbacks> = Arc::new(FakeCallbacks);
        t.host.set_action_callbacks(callbacks.clone());
        let mut client = connect(t.addr, &t.host.token).await;
        let resp = client
            .action_callback(sample_mutation_callback())
            .await
            .unwrap()
            .into_inner();
        assert!(matches!(
            resp.result,
            Some(action_callback_response::Result::ExecuteMutation(_))
        ));
    }

    #[tokio::test]
    async fn callback_error_metadata_survives_the_hop() {
        let t = start_host().await;
        let callbacks: Arc<dyn ActionCallbacks> = Arc::new(FakeCallbacks);
        t.host.set_action_callbacks(callbacks.clone());
        let mut client = connect(t.addr, &t.host.token).await;
        let err = client
            .action_callback(sample_query_callback())
            .await
            .unwrap_err()
            .into_anyhow();
        assert!(err.is_bad_request());
        assert_eq!(err.short_msg(), "FakeBadRequest");
    }
}
