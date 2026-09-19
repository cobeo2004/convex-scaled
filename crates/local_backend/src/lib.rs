#![feature(try_blocks)]
#![feature(try_blocks_heterogeneous)]
#![feature(iterator_try_collect)]
#![feature(coroutines)]
#![feature(exhaustive_patterns)]

use std::{
    self,
    sync::Arc,
    time::Duration,
};

use ::authentication::{
    access_token_auth::NullAccessTokenAuth,
    application_auth::ApplicationAuth,
};
use ::usage_limits::NoopUsageLimitNotifier;
use anyhow::Context;
use application::{
    self,
    api::ApplicationApi,
    log_visibility::RedactLogsToClient,
    Application,
    QueryCache,
    SourceMapCache,
};
use common::{
    self,
    http::{
        fetch::ProxiedFetchClient,
        RouteMapper,
    },
    knobs::{
        DOCUMENT_RETENTION_RATE_LIMIT,
        INDEX_CACHE_SIZE,
        NODE_ACTION_USER_TIMEOUT,
        UDF_CACHE_MAX_SIZE,
    },
    persistence::{
        Persistence,
        RepeatablePersistence,
    },
    runtime::{
        new_rate_limiter,
        Runtime,
    },
    shutdown::ShutdownSignal,
    types::{
        ConvexOrigin,
        ConvexSite,
        DeploymentClass,
        DeploymentMetadata,
        RepeatableTimestamp,
        TEST_REGION_NAME,
    },
};
use config::{
    FunctionRunnerMode,
    FunrunFallback,
    LocalConfig,
};
use database::{
    Database,
    TextIndexManagerSnapshot,
    TransactionTextSnapshot,
};
use events::usage::NoOpUsageEventLogger;
use exports::interface::InProcessExportProvider;
use file_storage::{
    FileStorage,
    TransactionalFileStorage,
};
use function_host::FunctionHost;
use function_runner::{
    in_process_function_runner::InProcessFunctionRunner,
    server::DeploymentStorage,
    FunctionRunner,
};
use funrun_proto::auth::{
    host_token,
    worker_token,
};
use governor::Quota;
use http_client::CachedHttpClient;
use indexing::{
    index_cache::IndexCache,
    index_reader::IndexReader,
};
use model::{
    database_globals::{
        types::StorageType,
        DatabaseGlobalsModel,
    },
    initialize_application_system_tables,
    virtual_system_mapping,
};
use node_executor::{
    local::LocalNodeExecutor,
    NodeActions,
    NodeExecutor,
};
use performance_stats::exporter::register_prometheus_exporter;
use remote_function_runner::{
    pool::WorkerPool,
    RemoteFunctionRunner,
    RemoteNodeExecutor,
};
use runtime::prod::ProdRuntime;
use search::{
    searcher::InProcessSearcher,
    Searcher,
    SegmentTermMetadataFetcher,
};
use serde::Serialize;
pub use sync::subscription_reconnect::SubscriptionReconnectRateLimiter;

pub mod admin;
mod ai_gateway;
mod app_metrics;
mod args_structs;
pub mod authentication;
pub mod beacon;
pub mod canonical_urls;
pub mod config;
pub mod custom_headers;
pub mod dashboard;
pub mod deploy_config;
pub mod deploy_config2;
pub mod deployment_audit_log;
pub mod deployment_info;
pub mod deployment_state;
pub mod environment_variables;
pub mod http_actions;
pub mod log_sinks;
pub mod logs;
pub mod node_action_callbacks;
pub mod parse;
pub mod proxy;
pub mod public_api;
pub mod router;
pub mod scheduling;
pub mod schema;
pub mod snapshot_export;
pub mod snapshot_import;
pub mod storage;
pub mod streaming_export;
pub mod streaming_import;
pub mod subs;
pub mod usage_limits;

pub const MAX_CONCURRENT_REQUESTS: usize = 128;

#[derive(Clone)]
pub struct LocalAppState {
    // Origin for the server (e.g. http://127.0.0.1:3210, https://demo.convex.cloud)
    pub origin: ConvexOrigin,
    // Origin for the corresponding convex.site (where we serve HTTP) (e.g. http://127.0.0.1:8001, https://crazy-giraffe-123.convex.site)
    pub site_origin: ConvexSite,
    // Name of the instance. (e.g. crazy-giraffe-123)
    pub instance_name: String,
    pub application: Application<ProdRuntime>,
    pub zombify_rx: async_broadcast::Receiver<()>,
}

impl LocalAppState {
    pub async fn shutdown(self) -> anyhow::Result<()> {
        self.application.shutdown().await?;

        Ok(())
    }
}

// Contains state needed to serve most http routes. Similar to LocalAppState,
// but uses ApplicationApi instead of Application, which allows it to be used
// in both Backend and Usher.
#[derive(Clone)]
pub struct RouterState {
    pub api: Arc<dyn ApplicationApi>,
    pub runtime: ProdRuntime,
    pub subscription_reconnect_rate_limiter: Option<Arc<SubscriptionReconnectRateLimiter>>,
}

#[derive(Serialize)]
pub struct EmptyResponse {}

pub async fn make_app(
    runtime: ProdRuntime,
    config: LocalConfig,
    persistence: Arc<dyn Persistence>,
    zombify_rx: async_broadcast::Receiver<()>,
    preempt_tx: ShutdownSignal,
) -> anyhow::Result<LocalAppState> {
    // Fail before touching the database: workers load modules from shared
    // storage, so remote mode needs S3.
    anyhow::ensure!(
        config.function_runner == FunctionRunnerMode::Local || config.s3_storage,
        "FUNCTION_RUNNER=remote requires --s3-storage: workers load modules from shared storage"
    );
    let key_broker = config.key_broker()?;
    let in_process_searcher = Arc::new(InProcessSearcher::new(runtime.clone())?);
    let searcher: Arc<dyn Searcher> = in_process_searcher.clone();
    // TODO(CX-6572) Separate `SegmentMetadataFetcher` from `SearcherImpl`
    let segment_metadata_fetcher: Arc<dyn SegmentTermMetadataFetcher> = in_process_searcher;
    let (deleted_tablet_sender, deleted_tablet_receiver) = tokio::sync::mpsc::channel(100);
    let usage_event_logger = Arc::new(NoOpUsageEventLogger);
    let database = Database::load(
        persistence.clone(),
        runtime.clone(),
        searcher.clone(),
        preempt_tx.clone(),
        virtual_system_mapping().clone(),
        IndexCache::new(*INDEX_CACHE_SIZE).new_handle(),
        Arc::new(new_rate_limiter(
            runtime.clone(),
            Quota::per_second(*DOCUMENT_RETENTION_RATE_LIMIT),
        )),
        deleted_tablet_sender,
        config.name(),
    )
    .await?;
    initialize_application_system_tables(&database).await?;
    let application_storage = Application::initialize_storage(
        runtime.clone(),
        &database,
        config.storage_tag_initializer(),
        config.name(),
    )
    .await?;

    let file_storage = FileStorage {
        transactional_file_storage: TransactionalFileStorage::new(
            runtime.clone(),
            application_storage.files_storage.clone(),
            config.convex_origin_url()?,
        ),
        database: database.clone(),
    };

    let deployment = DeploymentMetadata {
        name: config.name(),
        region: None,
        class: DeploymentClass::S16,
    };
    // `key_broker()` above already required the secret.
    let instance_secret = config
        .instance_secret
        .as_deref()
        .context("--instance-secret is required")?;
    let remote = config.function_runner == FunctionRunnerMode::Remote;
    let node_process_timeout = *NODE_ACTION_USER_TIMEOUT + Duration::from_secs(5);
    let local_node: Arc<dyn NodeExecutor> =
        Arc::new(LocalNodeExecutor::new(node_process_timeout).await?);
    let (node_executor, node_origin): (Arc<dyn NodeExecutor>, ConvexOrigin) =
        match (remote, config.node_workers()) {
            (true, Some(target)) => {
                let origin = config.node_callback_origin()?;
                if origin_is_loopback(&origin) {
                    tracing::warn!(
                        "FUNRUN_NODE_CALLBACK_ORIGIN resolves to loopback ({origin}); node \
                         workers will call back to themselves. Set it to the conductor's private \
                         address."
                    );
                }
                let pool = WorkerPool::start(
                    runtime.clone(),
                    target.to_owned(),
                    config.funrun_routing,
                    worker_token(instance_secret),
                    "node",
                )?;
                let fallback =
                    (config.funrun_fallback == FunrunFallback::Local).then(|| local_node.clone());
                (Arc::new(RemoteNodeExecutor::new(pool, fallback)), origin)
            },
            (true, None) => {
                tracing::info!(
                    "FUNRUN_NODE_WORKERS unset: \"use node\" actions run on the conductor"
                );
                (local_node, config.convex_origin_url()?)
            },
            (false, _) => (local_node, config.convex_origin_url()?),
        };
    let node_actions = NodeActions::new(
        node_executor,
        node_origin,
        *NODE_ACTION_USER_TIMEOUT,
        runtime.clone(),
        deployment.clone(),
    );

    #[cfg(not(debug_assertions))]
    if config.convex_http_proxy.is_none() {
        tracing::warn!(
            "Running without a proxy in release mode -- UDF `fetch` requests are unrestricted!"
        );
    }
    let fetch_client = Arc::new(ProxiedFetchClient::new(
        config.convex_http_proxy.clone(),
        config.name(),
        reqwest::redirect::Policy::none(),
    ));
    let oidc_http_client = CachedHttpClient::new(
        config.convex_http_proxy.clone(),
        config.name(),
        reqwest::redirect::Policy::default(),
    );
    let local_runner = InProcessFunctionRunner::new(
        deployment.clone(),
        key_broker.function_runner_keybroker(),
        config.convex_origin_url()?,
        runtime.clone(),
        persistence.reader(),
        DeploymentStorage {
            files_storage: application_storage.files_storage.clone(),
            modules_storage: application_storage.modules_storage.clone(),
        },
        database.clone(),
        fetch_client.clone(),
    )?;
    let function_runner: Arc<dyn FunctionRunner<ProdRuntime>> = match config.function_runner {
        FunctionRunnerMode::Local => Arc::new(local_runner),
        FunctionRunnerMode::Remote => {
            // Fail fast on a missing/non-S3 storage config before starting the
            // worker pool (which opens network connections).
            let s3_prefix = s3_prefix(&database).await?;
            let pool = WorkerPool::start(
                runtime.clone(),
                config
                    .funrun_workers
                    .clone()
                    .context("FUNRUN_WORKERS is required")?,
                config.funrun_routing,
                worker_token(instance_secret),
                "isolate",
            )?;
            Arc::new(RemoteFunctionRunner::new(
                pool,
                (config.funrun_fallback == FunrunFallback::Local).then_some(local_runner),
                database.clone(),
                deployment,
                config.convex_origin_url()?,
                s3_prefix,
            ))
        },
    };

    let persistence_reader = persistence.reader();
    let application = Application::new(
        runtime.clone(),
        database.clone(),
        file_storage.clone(),
        application_storage,
        usage_event_logger,
        Arc::new(NoopUsageLimitNotifier),
        key_broker.clone(),
        DeploymentMetadata {
            name: config.name(),
            region: Some(TEST_REGION_NAME.clone()),
            class: DeploymentClass::S16,
        },
        function_runner,
        config.convex_origin_url()?,
        config.convex_site_url()?,
        searcher.clone(),
        segment_metadata_fetcher,
        persistence,
        node_actions,
        Arc::new(RedactLogsToClient::new(config.redact_logs_to_client)),
        Arc::new(ApplicationAuth::new(
            key_broker.clone(),
            Arc::new(NullAccessTokenAuth),
            runtime.clone(),
        )),
        QueryCache::new(*UDF_CACHE_MAX_SIZE),
        fetch_client,
        config.local_log_sink.clone(),
        preempt_tx.clone(),
        Arc::new(InProcessExportProvider),
        deleted_tablet_receiver,
        oidc_http_client,
        Some(Arc::new(ai_gateway::LocalAiGatewayTokenMinter::new(
            config.control_plane_url.clone(),
            config.control_plane_access_token.clone(),
        ))),
        SourceMapCache::new(runtime.clone()),
    )
    .await?;

    if config.function_runner == FunctionRunnerMode::Remote {
        start_function_host(
            &runtime,
            &database,
            persistence_reader,
            &application,
            config.function_host_listen,
            host_token(instance_secret),
        )?;
    }

    if let Some(addr) = config.funrun_conductor_metrics_listen {
        // Serves for the life of the process.
        let (handle, _flush) = register_prometheus_exporter(runtime.clone(), addr);
        handle.detach();
    }

    let origin = config.convex_origin_url()?;
    let instance_name = config.name();

    if !config.disable_beacon {
        let beacon_future = beacon::start_beacon(
            runtime.clone(),
            database.clone(),
            config.beacon_tag.clone(),
            config.beacon_fields.clone(),
        );
        runtime.spawn_background("beacon_worker", beacon_future);
    }

    let app_state = LocalAppState {
        origin,
        site_origin: config.convex_site_url()?,
        instance_name,
        application,
        zombify_rx,
    };

    Ok(app_state)
}

/// Whether `origin`'s host is `localhost` or a loopback IP.
fn origin_is_loopback(origin: &str) -> bool {
    let Ok(url) = url::Url::parse(origin) else {
        return false;
    };
    match url.host() {
        Some(url::Host::Domain(d)) => d == "localhost",
        Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
        Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
        None => false,
    }
}

/// The `StorageType::S3` prefix `Application::initialize_storage` persisted in
/// the database globals.
async fn s3_prefix(database: &Database<ProdRuntime>) -> anyhow::Result<String> {
    let mut tx = database.begin_system().await?;
    let globals = DatabaseGlobalsModel::new(&mut tx)
        .database_globals()
        .await?;
    match globals.into_value().storage_type {
        Some(StorageType::S3 { s3_prefix }) => Ok(s3_prefix),
        other @ (Some(StorageType::Local { .. }) | None) => {
            anyhow::bail!("FUNCTION_RUNNER=remote requires S3 storage, found {other:?}")
        },
    }
}

/// `ts` comes from a worker; only read at timestamps the database already
/// made repeatable.
fn worker_ts(
    latest: RepeatableTimestamp,
    ts: RepeatableTimestamp,
) -> anyhow::Result<RepeatableTimestamp> {
    latest
        .prior_ts(*ts)
        .with_context(|| format!("worker timestamp {ts} is past the repeatable timestamp {latest}"))
}

/// Serves the worker-facing FunctionHost gRPC service in the background.
fn start_function_host(
    runtime: &ProdRuntime,
    database: &Database<ProdRuntime>,
    reader: Arc<dyn common::persistence::PersistenceReader>,
    application: &Application<ProdRuntime>,
    listen: std::net::SocketAddr,
    token: String,
) -> anyhow::Result<()> {
    let db_reader = database.clone();
    let db_text = database.clone();
    let db_index = database.clone();
    let host = Arc::new(FunctionHost::new(
        Arc::new(move |ts| {
            let ts = worker_ts(db_reader.now_ts_for_reads(), ts)?;
            let rp =
                RepeatablePersistence::new(reader.clone(), ts, db_reader.retention_validator());
            Ok(Arc::new(rp.read_snapshot(ts)?) as Arc<dyn IndexReader>)
        }),
        Arc::new(move |ts| {
            let snapshot = db_text.snapshot(worker_ts(db_text.now_ts_for_reads(), ts)?)?;
            Ok(Arc::new(TextIndexManagerSnapshot::new(
                snapshot.index_registry,
                snapshot.text_indexes,
                db_text.searcher.clone(),
                db_text.search_storage.clone(),
            )) as Arc<dyn TransactionTextSnapshot>)
        }),
        Arc::new(move |ts, index_id| {
            let snapshot = db_index.snapshot(worker_ts(db_index.now_ts_for_reads(), ts)?)?;
            snapshot
                .index_registry
                .enabled_index_by_index_id(&index_id)
                .cloned()
                .context("index not found")
        }),
        token,
    ));
    // The host holds a Weak; `Application` owns this runner for the process
    // lifetime.
    host.set_action_callbacks(application.runner());
    // Bind now so a taken port fails startup instead of a background task.
    // `host.serve` itself logs "gRPC services funrun.FunctionHost listening
    // on ..." once bound, so no separate info log here.
    let socket = common::http::server_socket(listen)?;
    runtime.spawn_background("function_host", async move {
        // ponytail: host dies with the process; wire the shutdown/preempt
        // signal if draining worker callbacks matters.
        if let Err(e) = host.serve(socket, std::future::pending()).await {
            tracing::error!("function_host exited: {e:#}");
        }
    });
    Ok(())
}

#[derive(Clone)]
pub struct HttpActionRouteMapper;

impl RouteMapper for HttpActionRouteMapper {
    fn map_route(&self, route: String) -> String {
        // Backend can receive arbitrary HTTP requests, so group all of these
        // under one tag.
        if route.starts_with("/http/") {
            "/http/:user_http_action".into()
        } else {
            route
        }
    }
}

#[cfg(test)]
mod tests {
    use common::types::{
        RepeatableReason,
        RepeatableTimestamp,
        Timestamp,
    };

    use super::{
        origin_is_loopback,
        worker_ts,
    };

    #[test]
    fn funrun_origin_is_loopback_detects_loopback_hosts() {
        assert!(origin_is_loopback("http://127.0.0.1:3210"));
        assert!(origin_is_loopback("http://localhost:3210"));
        assert!(origin_is_loopback("http://[::1]:3210"));
        assert!(!origin_is_loopback("http://conductor:3210"));
        assert!(!origin_is_loopback("https://api.example.com"));
    }

    fn repeatable(ts: u64) -> RepeatableTimestamp {
        RepeatableTimestamp::new_validated(
            Timestamp::try_from(ts).unwrap(),
            RepeatableReason::SnapshotManagerLatest,
        )
    }

    #[test]
    fn worker_ts_accepts_timestamps_up_to_the_repeatable_ts() -> anyhow::Result<()> {
        assert_eq!(worker_ts(repeatable(10), repeatable(10))?, repeatable(10));
        assert_eq!(worker_ts(repeatable(10), repeatable(3))?, repeatable(3));
        Ok(())
    }

    #[test]
    fn worker_ts_rejects_timestamps_past_the_repeatable_ts() {
        assert!(worker_ts(repeatable(10), repeatable(11)).is_err());
    }
}
