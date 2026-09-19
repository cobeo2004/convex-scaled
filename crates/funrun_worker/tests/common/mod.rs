//! Loopback `function_host` with fakes, copied minimal from the
//! `function_host` tests.

use std::{
    net::SocketAddr,
    sync::Arc,
    time::Duration,
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
use errors::ErrorMetadata;
use function_host::{
    FunctionHost,
    IndexAt,
    IndexReaderAt,
    TextSnapshotAt,
};
use funrun_proto::{
    auth::funrun_token,
    ids::repeatable_ts_from_u64,
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
use serde_json::{
    json,
    Value as JsonValue,
};
use sync_types::types::SerializedArgs;
use tokio::net::{
    TcpSocket,
    TcpStream,
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

pub fn sample_ts() -> RepeatableTimestamp {
    repeatable_ts_from_u64(1_000).unwrap()
}

pub fn sample_tablet() -> TabletId {
    TabletId(InternalId::from([6u8; 16]))
}

pub fn sample_index_ref() -> IndexRef {
    IndexRef::from_parts(
        IndexId(InternalId::from([2u8; 16])),
        PersistenceIndexId::new(2),
    )
}

pub fn sample_path() -> CanonicalizedComponentFunctionPath {
    CanonicalizedComponentFunctionPath {
        component: ComponentPath::root(),
        udf_path: "messages.js:send".parse().unwrap(),
    }
}

pub fn sample_args() -> SerializedArgs {
    SerializedArgs::from_args(vec![json!({})]).unwrap()
}

pub fn sample_context() -> ExecutionContext {
    ExecutionContext::new_from_parts(
        RequestId::new(),
        ExecutionId::new(),
        None,
        true,
        RequestMetadata::system(),
    )
}

/// Three documents with index keys `[1]`, `[2]`, `[3]`, paged like
/// `PersistenceSnapshot::index_page`.
struct FakeIndexReader(RepeatableTimestamp);

#[async_trait]
impl IndexReader for FakeIndexReader {
    async fn index_page(
        &self,
        _index: IndexRef,
        tablet_id: TabletId,
        interval: &Interval,
        order: Order,
        max_results: usize,
    ) -> anyhow::Result<IndexPage> {
        let mut keys = vec![1u8, 2, 3];
        if order == Order::Desc {
            keys.reverse();
        }
        let mut entries = vec![];
        for key in keys.into_iter().filter(|k| interval.contains(&[*k])) {
            let id = ResolvedDocumentId {
                tablet_id,
                developer_id: DeveloperDocumentId::new(
                    TableNumber::try_from(10001u32)?,
                    InternalId::from([key; 16]),
                ),
            };
            let doc =
                ResolvedDocument::new(id, CreationTime::try_from(1.0)?, ConvexObject::empty())?;
            entries.push(Arc::new(IndexEntry {
                key: IndexKeyBytes(vec![key]),
                ts: Timestamp::MIN,
                value: PackedDocument::pack(&doc),
            }));
            if entries.len() == max_results {
                let cursor = CursorPosition::After(IndexKeyBytes(vec![key]));
                return Ok(IndexPage { entries, cursor });
            }
        }
        Ok(IndexPage {
            entries,
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

pub struct TestHost {
    pub addr: SocketAddr,
    pub token: String,
    // The host only holds a `Weak` to the callbacks.
    _callbacks: Arc<dyn ActionCallbacks>,
}

/// Serves until the test's runtime shuts down.
pub async fn start_host_with_fakes() -> TestHost {
    start_host_with_index_page_max_bytes(None).await
}

/// `index_page_max_bytes` overrides the host's IndexPage byte budget.
pub async fn start_host_with_index_page_max_bytes(index_page_max_bytes: Option<usize>) -> TestHost {
    let index_reader_at: IndexReaderAt =
        Arc::new(|ts| Ok(Arc::new(FakeIndexReader(ts)) as Arc<dyn IndexReader>));
    let text_snapshot_at: TextSnapshotAt = Arc::new(|_| anyhow::bail!("unused"));
    let index_at: IndexAt = Arc::new(|_, _| anyhow::bail!("unused"));
    let token = funrun_token("secret");
    let mut host = FunctionHost::new(index_reader_at, text_snapshot_at, index_at, token.clone());
    if let Some(bytes) = index_page_max_bytes {
        host = host.with_index_page_max_bytes(bytes);
    }
    let host = Arc::new(host);
    let callbacks: Arc<dyn ActionCallbacks> = Arc::new(FakeCallbacks);
    host.set_action_callbacks(callbacks.clone());
    let socket = TcpSocket::new_v4().unwrap();
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let addr = socket.local_addr().unwrap();
    tokio::spawn(host.serve(socket, std::future::pending()));
    // The host channel is lazy and does not retry a refused first dial, so
    // wait until the server listens.
    tokio::time::timeout(Duration::from_secs(5), async {
        while TcpStream::connect(addr).await.is_err() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    TestHost {
        addr,
        token,
        _callbacks: callbacks,
    }
}
