//! Conductor side of remote function execution: `RemoteFunctionRunner` sends
//! `run_function` to a funrun worker and retries only when that cannot run a
//! function's side effects twice. Everything else runs in process.

use std::{
    collections::{
        BTreeMap,
        BTreeSet,
    },
    sync::Arc,
    time::Duration,
};

use anyhow::Context;
use async_trait::async_trait;
use bytes::Bytes;
use common::{
    auth::AuthConfig,
    bootstrap_model::components::definition::ComponentDefinitionMetadata,
    components::{
        ComponentDefinitionPath,
        ComponentName,
        Resource,
    },
    errors::JsError,
    execution_context::ExecutionContext,
    knobs::{
        FUNRUN_CLIENT_MAX_RETRIES,
        FUNRUN_RUN_FUNCTION_TIMEOUT,
        SUBFUNCTIONS_IN_SAME_ISOLATE,
    },
    log_lines::LogLine,
    runtime::{
        Runtime,
        UnixTimestamp,
    },
    schemas::DatabaseSchema,
    types::{
        ConvexOrigin,
        DeploymentMetadata,
        IndexId,
        RepeatableTimestamp,
        UdfType,
    },
};
use database::Database;
use errors::ErrorMetadata;
use function_runner::{
    in_process_function_runner::InProcessFunctionRunner,
    server::{
        validate_run_function_result,
        FunctionMetadata,
        HttpActionMetadata,
    },
    FunctionFinalTransaction,
    FunctionRunner,
    FunctionWrites,
};
use funrun_proto::{
    auth::MODULE_HEADER,
    deploy::{
        decode_return,
        DeployCall,
        DeployReturn,
    },
    http::down_to_response_part,
    request::{
        run_request_to_proto,
        HttpRequestParts,
        RunRequestParts,
    },
    transaction::{
        run_result_from_proto,
        RunResultParts,
    },
};
use futures::{
    future::{
        BoxFuture,
        OptionFuture,
    },
    stream::BoxStream,
    FutureExt,
    StreamExt,
};
use keybroker::Identity;
use model::{
    components::auth::propagate_component_auth,
    config::types::ModuleConfig,
    environment_variables::types::{
        EnvVarName,
        EnvVarValue,
    },
    modules::module_versions::{
        AnalyzedModule,
        ModuleSource,
        SourceMap,
    },
    udf_config::types::UdfConfig,
};
use pb::error_metadata::ErrorMetadataStatusExt;
use pb_funrun::funrun::{
    deploy_result,
    execute_down::Inner as Down,
    execute_up::Inner as Up,
    BodyChunk,
    DeployResult,
    ExecuteDown,
    ExecuteUp,
    NodeResult,
    Overloaded,
    RunResult,
    Started,
};
use sync_types::{
    CanonicalizedModulePath,
    Timestamp,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::metadata::AsciiMetadataValue;
use udf::{
    ActionCallbacks,
    EvaluateAppDefinitionsResult,
    FunctionOutcome,
    HttpActionResponseStreamer,
};
use usage_tracking::FunctionUsageStats;
use value::identifier::Identifier;

use crate::{
    metrics::{
        log_fallback,
        FallbackKind,
    },
    pool::{
        FunrunChannel,
        WorkerPool,
    },
    retry::{
        failure_stage,
        is_transport_failure,
        may_retry,
        Delivery,
        FailureStage,
        RequestKind,
    },
};

pub mod metrics;
mod node;
pub mod pick;
pub mod pool;
pub mod retry;
pub mod status;

pub use crate::node::RemoteNodeExecutor;

pub struct RemoteFunctionRunner<RT: Runtime> {
    pool: Arc<WorkerPool>,
    /// Set when `FUNRUN_FALLBACK=local`: runs requests in process while the
    /// pool has no healthy worker.
    local: Option<InProcessFunctionRunner<RT>>,
    database: Database<RT>,
    deployment: DeploymentMetadata,
    convex_origin: ConvexOrigin,
    s3_prefix: String,
}

impl<RT: Runtime> RemoteFunctionRunner<RT> {
    /// `s3_prefix` is the deployment's `StorageType::S3` prefix; workers
    /// build their module and file storage from it.
    pub fn new(
        pool: Arc<WorkerPool>,
        local: Option<InProcessFunctionRunner<RT>>,
        database: Database<RT>,
        deployment: DeploymentMetadata,
        convex_origin: ConvexOrigin,
        s3_prefix: String,
    ) -> Self {
        Self {
            pool,
            local,
            database,
            deployment,
            convex_origin,
            s3_prefix,
        }
    }

    /// `Some(local)` when `FUNRUN_FALLBACK=local` and the pool has no healthy
    /// worker.
    fn fallback(&self, kind: FallbackKind) -> Option<&InProcessFunctionRunner<RT>> {
        let local = self.local.as_ref()?;
        if self.pool.has_healthy() {
            return None;
        }
        log_fallback(kind);
        Some(local)
    }

    /// Runs one deploy-time evaluation on a worker. Only `analyze` may
    /// answer with a user `JsError` value.
    async fn deploy(&self, call: DeployCall) -> anyhow::Result<Result<DeployReturn, JsError>> {
        let up = Up::Deploy(call.try_into()?);
        // ponytail: in process these evaluations have no outer deadline; the
        // isolate's own user/system timeouts bound them on the worker too.
        // This is only the conductor's safety net, so it reuses the run one.
        let Terminal::Deploy(r) = execute_with_retries(
            &self.pool,
            "_deploy",
            RequestKind::Deploy,
            up,
            None,
            None,
            None,
            *FUNRUN_RUN_FUNCTION_TIMEOUT,
        )
        .await?
        else {
            anyhow::bail!("worker answered a deploy request with a non-deploy frame");
        };
        match r.result.context("empty DeployResult")? {
            deploy_result::Result::Json(b) => Ok(Ok(decode_return(&b)?)),
            deploy_result::Result::JsError(e) => Ok(Err(JsError::try_from(e)?)),
        }
    }
}

#[async_trait]
impl<RT: Runtime> FunctionRunner<RT> for RemoteFunctionRunner<RT> {
    async fn run_function(
        &self,
        udf_type: UdfType,
        identity: Identity,
        ts: RepeatableTimestamp,
        existing_writes: FunctionWrites,
        log_line_sender: Option<mpsc::UnboundedSender<LogLine>>,
        function_metadata: Option<FunctionMetadata>,
        http_action_metadata: Option<HttpActionMetadata>,
        default_system_env_vars: BTreeMap<EnvVarName, EnvVarValue>,
        in_memory_index_last_modified: BTreeMap<IndexId, Timestamp>,
        context: ExecutionContext,
    ) -> anyhow::Result<(
        Option<FunctionFinalTransaction>,
        FunctionOutcome,
        FunctionUsageStats,
    )> {
        if let Some(local) = self.fallback(FallbackKind::Isolate) {
            return local
                .run_function(
                    udf_type,
                    identity,
                    ts,
                    existing_writes,
                    log_line_sender,
                    function_metadata,
                    http_action_metadata,
                    default_system_env_vars,
                    in_memory_index_last_modified,
                    context,
                )
                .await;
        }
        let module = match (&function_metadata, &http_action_metadata) {
            (Some(f), _) => f.path_and_args.path().udf_path.module().as_str().to_owned(),
            (None, Some(h)) => h
                .http_module_path
                .path()
                .udf_path
                .module()
                .as_str()
                .to_owned(),
            (None, None) => anyhow::bail!("run_function needs function or HTTP action metadata"),
        };
        let (mut http_response, http_body, http) = match http_action_metadata {
            Some(h) => {
                let has_body = h.http_request.body.is_some();
                (
                    Some(h.http_response_streamer),
                    h.http_request.body,
                    Some(HttpRequestParts {
                        http_module_path: h.http_module_path,
                        routed_path: h.routed_path,
                        head: h.http_request.head,
                        has_body,
                    }),
                )
            },
            None => (None, None, None),
        };
        // Same counts `impl TableCountSnapshot for Option<TableCounts>` serves
        // in process.
        let table_counts = self.database.snapshot(ts)?.table_counts.map(|counts| {
            counts
                .tables
                .iter()
                .map(|(tablet_id, count)| (*tablet_id, count.num_values()))
                .collect()
        });
        let parts = RunRequestParts {
            instance_name: self.deployment.name.clone(),
            udf_type,
            identity,
            ts,
            existing_writes,
            function_metadata,
            http,
            default_system_env_vars,
            in_memory_index_last_modified,
            context,
            bootstrap_metadata: self.database.bootstrap_metadata.clone(),
            table_counts,
            deployment: self.deployment.clone(),
            convex_origin: self.convex_origin.clone(),
            subfunctions_in_same_isolate: *SUBFUNCTIONS_IN_SAME_ISOLATE,
            s3_prefix: self.s3_prefix.clone(),
        };
        // Upstream `FunctionRunnerCore` runs an HTTP action as its component
        // and records that identity in the outcome; rebuild it the same way.
        let outcome_identity = match &parts.http {
            Some(http) => {
                let component = http.http_module_path.path().component;
                propagate_component_auth(&parts.identity, component, component.is_root())
            },
            None => parts.identity.clone(),
        };
        let request = run_request_to_proto(&parts)?;
        // NOTE: as in process, no result or error surfaces before the
        // retention check below.
        let result = execute_with_retries(
            &self.pool,
            &module,
            RequestKind::Run(udf_type),
            Up::Request(request),
            log_line_sender.as_ref(),
            http_response.as_mut(),
            http_body,
            *FUNRUN_RUN_FUNCTION_TIMEOUT,
        )
        .await
        .and_then(|terminal| {
            let result = match terminal {
                Terminal::Run(r) => r,
                other @ (Terminal::Deploy(_) | Terminal::Node(_)) => {
                    anyhow::bail!("worker sent {other:?} for a run request")
                },
            };
            let RunResultParts {
                transaction,
                outcome,
                usage,
            } = run_result_from_proto(
                result,
                parts.function_metadata.map(|m| m.path_and_args),
                parts.http.map(|h| (h.http_module_path, h.head)),
                outcome_identity.into(),
            )?;
            Ok((transaction, outcome, usage))
        });
        validate_run_function_result(udf_type, *ts, self.database.retention_validator()).await?;
        result
    }

    async fn analyze(
        &self,
        udf_config: UdfConfig,
        modules: BTreeMap<CanonicalizedModulePath, ModuleConfig>,
        environment_variables: BTreeMap<EnvVarName, EnvVarValue>,
    ) -> anyhow::Result<Result<BTreeMap<CanonicalizedModulePath, AnalyzedModule>, JsError>> {
        if let Some(local) = self.fallback(FallbackKind::Deploy) {
            return local
                .analyze(udf_config, modules, environment_variables)
                .await;
        }
        let call = DeployCall::Analyze {
            udf_config,
            modules,
            environment_variables,
        };
        match self.deploy(call).await? {
            Ok(DeployReturn::Analyze(m)) => Ok(Ok(m)),
            Ok(other) => anyhow::bail!("worker returned {} for analyze", other.kind_name()),
            Err(js) => Ok(Err(js)),
        }
    }

    async fn evaluate_app_definitions(
        &self,
        app_definition: ModuleConfig,
        component_definitions: BTreeMap<ComponentDefinitionPath, ModuleConfig>,
        dependency_graph: BTreeSet<(ComponentDefinitionPath, ComponentDefinitionPath)>,
        user_environment_variables: BTreeMap<EnvVarName, EnvVarValue>,
        system_env_vars: BTreeMap<EnvVarName, EnvVarValue>,
    ) -> anyhow::Result<EvaluateAppDefinitionsResult> {
        if let Some(local) = self.fallback(FallbackKind::Deploy) {
            return local
                .evaluate_app_definitions(
                    app_definition,
                    component_definitions,
                    dependency_graph,
                    user_environment_variables,
                    system_env_vars,
                )
                .await;
        }
        let call = DeployCall::AppDefinitions {
            app_definition,
            component_definitions,
            dependency_graph,
            user_environment_variables,
            system_env_vars,
        };
        match self.deploy(call).await? {
            Ok(DeployReturn::AppDefinitions(r)) => Ok(r),
            Ok(other) => anyhow::bail!("worker returned {} for app definitions", other.kind_name()),
            Err(js) => anyhow::bail!("unexpected JsError from app definitions: {js}"),
        }
    }

    async fn evaluate_component_initializer(
        &self,
        evaluated_definitions: BTreeMap<ComponentDefinitionPath, ComponentDefinitionMetadata>,
        path: ComponentDefinitionPath,
        definition: ModuleConfig,
        args: BTreeMap<Identifier, Resource>,
        name: ComponentName,
    ) -> anyhow::Result<BTreeMap<Identifier, Resource>> {
        if let Some(local) = self.fallback(FallbackKind::Deploy) {
            return local
                .evaluate_component_initializer(evaluated_definitions, path, definition, args, name)
                .await;
        }
        let call = DeployCall::ComponentInitializer {
            evaluated_definitions,
            path,
            definition,
            args,
            name,
        };
        match self.deploy(call).await? {
            Ok(DeployReturn::ComponentInitializer(r)) => Ok(r),
            Ok(other) => anyhow::bail!(
                "worker returned {} for component initializer",
                other.kind_name()
            ),
            Err(js) => anyhow::bail!("unexpected JsError from component initializer: {js}"),
        }
    }

    async fn evaluate_schema(
        &self,
        schema_bundle: ModuleSource,
        source_map: Option<SourceMap>,
        rng_seed: [u8; 32],
        unix_timestamp: UnixTimestamp,
    ) -> anyhow::Result<DatabaseSchema> {
        if let Some(local) = self.fallback(FallbackKind::Deploy) {
            return local
                .evaluate_schema(schema_bundle, source_map, rng_seed, unix_timestamp)
                .await;
        }
        let call = DeployCall::Schema {
            bundle: schema_bundle,
            source_map,
            rng_seed,
            unix_timestamp,
        };
        match self.deploy(call).await? {
            Ok(DeployReturn::Schema(s)) => Ok(s),
            Ok(other) => anyhow::bail!("worker returned {} for schema", other.kind_name()),
            Err(js) => anyhow::bail!("unexpected JsError from schema: {js}"),
        }
    }

    async fn evaluate_auth_config(
        &self,
        auth_config_bundle: ModuleSource,
        source_map: Option<SourceMap>,
        environment_variables: BTreeMap<EnvVarName, EnvVarValue>,
        explanation: &str,
    ) -> anyhow::Result<AuthConfig> {
        if let Some(local) = self.fallback(FallbackKind::Deploy) {
            return local
                .evaluate_auth_config(
                    auth_config_bundle,
                    source_map,
                    environment_variables,
                    explanation,
                )
                .await;
        }
        let call = DeployCall::AuthConfig {
            bundle: auth_config_bundle,
            source_map,
            environment_variables,
            explanation: explanation.to_string(),
        };
        match self.deploy(call).await? {
            Ok(DeployReturn::AuthConfig(c)) => Ok(c),
            Ok(other) => anyhow::bail!("worker returned {} for auth config", other.kind_name()),
            Err(js) => anyhow::bail!("unexpected JsError from auth config: {js}"),
        }
    }

    fn set_action_callbacks(&self, action_callbacks: Arc<dyn ActionCallbacks>) {
        // Only in-process runs call back. Deploy-time evaluation never does,
        // so without a local runner there is nothing to set.
        if let Some(local) = &self.local {
            local.set_action_callbacks(action_callbacks);
        }
    }
}

type BodyStream = BoxStream<'static, anyhow::Result<Bytes>>;

/// The frame that ends an `Execute` call.
#[derive(Debug)]
pub(crate) enum Terminal {
    Run(RunResult),
    Deploy(DeployResult),
    Node(NodeResult),
}

struct AttemptFailure {
    stage: FailureStage,
    error: anyhow::Error,
}

/// Runs `request` on a worker, retrying on other workers only when
/// `may_retry` allows it. Each attempt gets `run_timeout`; the gRPC deadline
/// alone only bounds the response headers, which the worker sends at once.
pub(crate) async fn execute_with_retries(
    pool: &Arc<WorkerPool>,
    affinity: &str,
    kind: RequestKind,
    up: Up,
    log_line_sender: Option<&mpsc::UnboundedSender<LogLine>>,
    mut http_response: Option<&mut HttpActionResponseStreamer>,
    mut http_body: Option<BodyStream>,
    run_timeout: Duration,
) -> anyhow::Result<Terminal> {
    let module_header =
        AsciiMetadataValue::try_from(affinity).context("module path is not a valid header")?;
    let mut exclude = BTreeSet::new();
    let mut attempt = 0;
    loop {
        // Once every worker is excluded, fall back to all of them: in proxy
        // mode the only address is a load balancer.
        let Some((addr, client)) = pool
            .choose(affinity, &exclude)
            .or_else(|| pool.choose(affinity, &BTreeSet::new()))
        else {
            anyhow::bail!(ErrorMetadata::overloaded(
                "NoFunrunWorker",
                "no healthy funrun worker"
            ));
        };
        let _in_flight = pool.begin(&addr);
        let attempt_result = tokio::time::timeout(
            run_timeout,
            execute_once(
                client,
                kind,
                module_header.clone(),
                up.clone(),
                log_line_sender,
                http_response.as_deref_mut(),
                &mut http_body,
                run_timeout,
            ),
        )
        .await
        // Final for every UdfType: the run may still have side effects in
        // flight. Dropping the call cancels it on the worker.
        .map_err(|_| {
            anyhow::anyhow!(
                "funrun worker {addr} did not finish {kind:?} in {affinity} within \
                 {run_timeout:?}"
            )
        })??;
        let failure = match attempt_result {
            Ok(result) => return Ok(result),
            Err(failure) => failure,
        };
        if !may_retry(kind, failure.stage, attempt, *FUNRUN_CLIENT_MAX_RETRIES) {
            return Err(failure.error);
        }
        tracing::warn!(
            "retrying {kind:?} in {affinity}: funrun worker {addr} failed {:?}: {:#}",
            failure.stage,
            failure.error
        );
        exclude.insert(addr);
        attempt += 1;
    }
}

/// One `Execute` call. The outer error is final; the inner one goes through
/// the retry policy.
async fn execute_once(
    mut client: FunrunChannel,
    kind: RequestKind,
    module: AsciiMetadataValue,
    up: Up,
    log_line_sender: Option<&mpsc::UnboundedSender<LogLine>>,
    mut http_response: Option<&mut HttpActionResponseStreamer>,
    http_body: &mut Option<BodyStream>,
    run_timeout: Duration,
) -> anyhow::Result<Result<Terminal, AttemptFailure>> {
    // Open the call with an empty request stream and send the request only
    // once the worker answered with headers (it does so without waiting for
    // the first frame). A failed call therefore never delivered it.
    let (up_tx, up_rx) = mpsc::channel(8);
    let mut req = tonic::Request::new(ReceiverStream::new(up_rx));
    req.metadata_mut().insert(MODULE_HEADER, module);
    req.set_timeout(run_timeout);
    let mut down = match client.execute(req).await {
        Ok(response) => response.into_inner(),
        // The worker never received the request: it is not sent yet.
        Err(status) if is_transport_failure(&status) => {
            return Ok(Err(AttemptFailure {
                stage: failure_stage(kind, Delivery::NotSent),
                error: status.into_anyhow(),
            }));
        },
        Err(status) => return Err(status.into_anyhow()),
    };
    let request = ExecuteUp { inner: Some(up) };
    // A failed send means the call already ended and the request was not
    // handed to it, so nothing was sent.
    if up_tx.send(request).await.is_err() {
        return Ok(Err(AttemptFailure {
            stage: failure_stage(kind, Delivery::NotSent),
            error: anyhow::anyhow!("funrun Execute stream closed before the request was sent"),
        }));
    }
    // Held until the body pump takes it, so the request half stays open.
    let mut up_tx = Some(up_tx);
    let client_gone = http_response.as_ref().map(|s| s.sender.clone());
    let mut pump: Option<BoxFuture<'static, ()>> = None;
    // Flips only on the worker's `Started`. Actions count as started even
    // before it (see `failure_stage`).
    let mut started = false;
    let lost = |started: bool| {
        if started {
            Delivery::LostAfterStarted
        } else {
            Delivery::LostBeforeStarted
        }
    };
    loop {
        tokio::select! {
            biased;
            Some(()) = OptionFuture::from(client_gone.as_ref().map(|s| s.closed())) => {
                // Dropping `down` cancels the run on the worker.
                anyhow::bail!(ErrorMetadata::client_disconnect());
            },
            frame = down.message() => {
                let inner = match frame {
                    Err(status) if is_transport_failure(&status) => {
                        return Ok(Err(AttemptFailure {
                            stage: failure_stage(kind, lost(started)),
                            error: status.into_anyhow(),
                        }));
                    },
                    Err(status) => return Err(status.into_anyhow()),
                    Ok(None) => {
                        return Ok(Err(AttemptFailure {
                            stage: failure_stage(kind, lost(started)),
                            error: anyhow::anyhow!("funrun Execute stream ended without a result"),
                        }));
                    },
                    Ok(Some(ExecuteDown { inner })) => inner.context("empty ExecuteDown frame")?,
                };
                match inner {
                    Down::Started(Started {}) => {
                        started = true;
                        // The isolate reads the body only after it started, so
                        // a retried (not yet started) attempt never loses it.
                        if let Some(body) = http_body.take() {
                            let up = up_tx.take().context("request stream already handed off")?;
                            pump = Some(pump_body(body, up).boxed());
                        }
                    },
                    Down::LogLine(line) => {
                        if let Some(sender) = log_line_sender {
                            // A dropped receiver does not stop the function.
                            _ = sender.send(LogLine::try_from(line)?);
                        }
                    },
                    inner @ (Down::HttpResponseHead(_) | Down::HttpResponseBody(_)) => {
                        let part = down_to_response_part(ExecuteDown { inner: Some(inner) })?
                            .context("not an HTTP response part")?;
                        let streamer = http_response
                            .as_deref_mut()
                            .context("HTTP response frame for a non-HTTP function")?;
                        if streamer.send_part(part)?.is_err() {
                            anyhow::bail!(ErrorMetadata::client_disconnect());
                        }
                    },
                    Down::Overloaded(Overloaded { reason }) => {
                        // Sent only instead of starting; after `Started` it
                        // is a lost call like any other.
                        let delivery = if started {
                            Delivery::LostAfterStarted
                        } else {
                            Delivery::Refused
                        };
                        let error = ErrorMetadata::overloaded("FunrunWorkerOverloaded", reason);
                        return Ok(Err(AttemptFailure {
                            stage: failure_stage(kind, delivery),
                            error: error.into(),
                        }));
                    },
                    Down::Result(r) => return Ok(Ok(Terminal::Run(r))),
                    Down::DeployResult(r) => return Ok(Ok(Terminal::Deploy(r))),
                    Down::NodeResult(r) => return Ok(Ok(Terminal::Node(r))),
                }
            },
            Some(()) = OptionFuture::from(pump.as_mut()) => pump = None,
        }
    }
}

/// Streams the HTTP request body to the worker, ending with `end = true`.
/// On a body error it just stops: dropping the sender ends the request
/// stream, which fails the worker's body read like a local body error would.
async fn pump_body(mut body: BodyStream, up: mpsc::Sender<ExecuteUp>) {
    loop {
        let chunk = match body.next().await {
            Some(Ok(data)) => BodyChunk {
                data: data.into(),
                end: false,
            },
            None => BodyChunk {
                data: Default::default(),
                end: true,
            },
            Some(Err(e)) => {
                tracing::warn!("HTTP action request body failed: {e:#}");
                return;
            },
        };
        let end = chunk.end;
        let frame = ExecuteUp {
            inner: Some(Up::HttpRequestBody(chunk)),
        };
        if up.send(frame).await.is_err() || end {
            return;
        }
    }
}

/// A scripted fake funrun worker, shared by this crate's tests.
#[cfg(test)]
pub(crate) mod test_util {
    use std::{
        sync::{
            atomic::{
                AtomicUsize,
                Ordering,
            },
            Arc,
        },
        time::Duration,
    };

    use common::grpc::ConvexGrpcService;
    use futures::{
        stream::BoxStream,
        StreamExt,
    };
    use parking_lot::Mutex;
    use pb_funrun::funrun::{
        execute_down::Inner as Down,
        execute_up::Inner as Up,
        funrun_server::{
            Funrun,
            FunrunServer,
        },
        BodyChunk,
        ExecuteDown,
        ExecuteUp,
        LoadReport,
        WatchLoadRequest,
    };
    use tokio::net::{
        TcpSocket,
        TcpStream,
    };
    use tonic::{
        Request,
        Response,
        Status,
        Streaming,
    };

    use crate::pool::{
        connect_for_test,
        WorkerPool,
    };

    pub(crate) type Frame = Result<Down, Status>;

    pub(crate) fn down(inner: Down) -> Frame {
        Ok(inner)
    }

    pub(crate) enum Step {
        Send(Frame),
        /// Counts an up frame arriving within 100ms as early.
        ExpectQuiet,
        /// Records request body chunks up to `end = true`.
        ReadBody,
        /// Never sends anything again.
        Stall,
    }

    /// `Ok(steps)`: answer with headers at once, like the real worker, then
    /// read the RunRequest and play `steps`. `Err(status)`: reject the call.
    pub(crate) type Script = Arc<dyn Fn(usize) -> Result<Vec<Step>, Status> + Send + Sync>;

    /// Answers the n-th Execute call (from 0) with `script(n)`, counting calls
    /// and requests (run, deploy or node) received.
    #[derive(Clone)]
    pub(crate) struct FakeWorker {
        pub(crate) calls: Arc<AtomicUsize>,
        pub(crate) requests: Arc<AtomicUsize>,
        pub(crate) early: Arc<AtomicUsize>,
        pub(crate) bodies: Arc<Mutex<Vec<BodyChunk>>>,
        pub(crate) grpc_timeouts: Arc<Mutex<Vec<String>>>,
        /// The last request frame received.
        pub(crate) last_request: Arc<Mutex<Option<Up>>>,
        script: Script,
    }

    /// Reads the first up frame and, when it is a request, counts and
    /// returns it.
    pub(crate) async fn read_request(
        up: &mut Streaming<ExecuteUp>,
        requests: &AtomicUsize,
    ) -> Option<Up> {
        let Ok(Some(ExecuteUp { inner: Some(inner) })) = up.message().await else {
            return None;
        };
        match inner {
            Up::Request(_) | Up::Deploy(_) | Up::Node(_) => {
                requests.fetch_add(1, Ordering::SeqCst);
                Some(inner)
            },
            Up::HttpRequestBody(_) => None,
        }
    }

    #[tonic::async_trait]
    impl Funrun for FakeWorker {
        type ExecuteStream = BoxStream<'static, Result<ExecuteDown, Status>>;
        type WatchLoadStream = BoxStream<'static, Result<LoadReport, Status>>;

        async fn execute(
            &self,
            request: Request<Streaming<ExecuteUp>>,
        ) -> Result<Response<Self::ExecuteStream>, Status> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if let Some(timeout) = request.metadata().get("grpc-timeout") {
                self.grpc_timeouts
                    .lock()
                    .push(timeout.to_str().unwrap().to_string());
            }
            let mut up = request.into_inner();
            let requests = self.requests.clone();
            match (self.script)(call) {
                Err(status) => {
                    // Give a RunRequest sent before the headers time to land.
                    _ = tokio::time::timeout(
                        Duration::from_millis(200),
                        read_request(&mut up, &requests),
                    )
                    .await;
                    Err(status)
                },
                Ok(steps) => {
                    let fake = self.clone();
                    let stream = futures::stream::once(async move {
                        if let Some(r) = read_request(&mut up, &requests).await {
                            *fake.last_request.lock() = Some(r);
                        }
                        futures::stream::unfold(
                            (up, steps.into_iter()),
                            move |(mut up, mut steps)| {
                                let fake = fake.clone();
                                async move {
                                    loop {
                                        match steps.next()? {
                                            Step::Send(frame) => {
                                                let down = frame.map(|inner| ExecuteDown {
                                                    inner: Some(inner),
                                                });
                                                return Some((down, (up, steps)));
                                            },
                                            Step::ExpectQuiet => {
                                                let early = tokio::time::timeout(
                                                    Duration::from_millis(100),
                                                    up.message(),
                                                )
                                                .await;
                                                if let Ok(Ok(Some(_))) = early {
                                                    fake.early.fetch_add(1, Ordering::SeqCst);
                                                }
                                            },
                                            Step::ReadBody => {
                                                while let Ok(Some(ExecuteUp {
                                                    inner: Some(Up::HttpRequestBody(chunk)),
                                                })) = up.message().await
                                                {
                                                    let end = chunk.end;
                                                    fake.bodies.lock().push(chunk);
                                                    if end {
                                                        break;
                                                    }
                                                }
                                            },
                                            Step::Stall => std::future::pending::<()>().await,
                                        }
                                    }
                                }
                            },
                        )
                    })
                    .flatten();
                    Ok(Response::new(Box::pin(stream)))
                },
            }
        }

        async fn watch_load(
            &self,
            _request: Request<WatchLoadRequest>,
        ) -> Result<Response<Self::WatchLoadStream>, Status> {
            Ok(Response::new(Box::pin(futures::stream::pending())))
        }
    }

    pub(crate) async fn start_fake_worker(
        script: impl Fn(usize) -> Result<Vec<Step>, Status> + Send + Sync + 'static,
    ) -> (String, FakeWorker) {
        let fake = FakeWorker {
            calls: Arc::new(AtomicUsize::new(0)),
            requests: Arc::new(AtomicUsize::new(0)),
            early: Arc::new(AtomicUsize::new(0)),
            bodies: Arc::new(Mutex::new(Vec::new())),
            grpc_timeouts: Arc::new(Mutex::new(Vec::new())),
            last_request: Arc::new(Mutex::new(None)),
            script: Arc::new(script),
        };
        let socket = TcpSocket::new_v4().unwrap();
        socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = socket.local_addr().unwrap();
        tokio::spawn(
            ConvexGrpcService::new()
                .add_service(FunrunServer::new(fake.clone()))
                .serve(socket, std::future::pending()),
        );
        // The client channel is lazy and does not retry a refused first dial.
        tokio::time::timeout(Duration::from_secs(5), async {
            while TcpStream::connect(addr).await.is_err() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        (addr.to_string(), fake)
    }

    pub(crate) fn pool_of(addrs: &[&str]) -> Arc<WorkerPool> {
        let pool = WorkerPool::empty();
        for addr in addrs {
            pool.insert_healthy(addr.to_string(), connect_for_test(addr));
        }
        pool
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{
            BTreeMap,
            BTreeSet,
        },
        sync::{
            atomic::{
                AtomicUsize,
                Ordering,
            },
            Arc,
        },
        time::Duration,
    };

    use bytes::Bytes;
    use common::{
        auth::AuthConfig,
        types::UdfType,
    };
    use errors::ErrorMetadataAnyhowExt;
    use funrun_proto::deploy::{
        DeployCall,
        DeployReturn,
    };
    use futures::StreamExt;
    use model::modules::module_versions::ModuleSource;
    use pb_funrun::funrun::{
        deploy_result,
        execute_down::Inner as Down,
        execute_up::Inner as Up,
        BodyChunk,
        DeployRequest,
        DeployResult,
        Overloaded,
        RunRequest,
        RunResult,
        Started,
    };
    use tokio::sync::mpsc;
    use tonic::Status;
    use udf::{
        HttpActionResponsePart,
        HttpActionResponseStreamer,
    };

    use super::{
        execute_with_retries,
        Terminal,
    };
    use crate::{
        pool::WorkerPool,
        retry::RequestKind,
        test_util::{
            down,
            pool_of,
            start_fake_worker,
            Frame,
            Step,
        },
    };

    /// Long enough that only the run-timeout test ever hits it.
    const RUN_TIMEOUT: Duration = Duration::from_secs(10);

    async fn start_fake(
        script: fn(usize) -> Result<Vec<Step>, Status>,
    ) -> (String, Arc<AtomicUsize>) {
        let (addr, fake) = start_fake_worker(script).await;
        (addr, fake.calls)
    }

    fn auth_call() -> anyhow::Result<DeployRequest> {
        DeployCall::AuthConfig {
            bundle: ModuleSource::new("x"),
            source_map: None,
            environment_variables: BTreeMap::new(),
            explanation: "e".into(),
        }
        .try_into()
    }

    fn auth_result() -> anyhow::Result<Frame> {
        let ret = Vec::<u8>::try_from(DeployReturn::AuthConfig(AuthConfig { providers: vec![] }))?;
        Ok(down(Down::DeployResult(DeployResult {
            result: Some(deploy_result::Result::Json(ret)),
        })))
    }

    #[tokio::test]
    async fn deploy_round_trips_through_worker() -> anyhow::Result<()> {
        let (addr, fake) = start_fake_worker(move |_| {
            Ok(vec![
                Step::Send(down(Down::Started(Default::default()))),
                Step::Send(auth_result().unwrap()),
            ])
        })
        .await;
        let pool = pool_of(&[&addr]);
        let terminal = execute_with_retries(
            &pool,
            "_deploy",
            RequestKind::Deploy,
            Up::Deploy(auth_call()?),
            None,
            None,
            None,
            Duration::from_secs(5),
        )
        .await?;
        assert!(matches!(terminal, Terminal::Deploy(_)));
        assert!(matches!(*fake.last_request.lock(), Some(Up::Deploy(_))));
        Ok(())
    }

    #[tokio::test]
    async fn deploy_retries_after_started_stream_loss() -> anyhow::Result<()> {
        // Same technique as `mutation_is_retried_after_started`: attempt 0
        // ends the stream after `Started`, attempt 1 answers. (A stall would
        // hit the run timeout, which is final.)
        let (addr, fake) = start_fake_worker(move |attempt| match attempt {
            0 => Ok(vec![Step::Send(down(Down::Started(Default::default())))]),
            _ => Ok(vec![Step::Send(auth_result().unwrap())]),
        })
        .await;
        let pool = pool_of(&[&addr]);
        let t = execute_with_retries(
            &pool,
            "_deploy",
            RequestKind::Deploy,
            Up::Deploy(auth_call()?),
            None,
            None,
            None,
            Duration::from_secs(5),
        )
        .await?;
        assert!(matches!(t, Terminal::Deploy(_)));
        assert_eq!(fake.calls.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[test]
    fn empty_pool_has_no_healthy_worker() {
        assert!(!WorkerPool::empty().has_healthy());
    }

    async fn run(
        pool: &Arc<WorkerPool>,
        module: &str,
        udf_type: UdfType,
    ) -> anyhow::Result<RunResult> {
        match execute_with_retries(
            pool,
            module,
            RequestKind::Run(udf_type),
            Up::Request(RunRequest::default()),
            None,
            None,
            None,
            RUN_TIMEOUT,
        )
        .await?
        {
            Terminal::Run(r) => Ok(r),
            Terminal::Deploy(_) | Terminal::Node(_) => anyhow::bail!("not a run result"),
        }
    }

    fn overloaded(_: usize) -> Result<Vec<Step>, Status> {
        Ok(vec![Step::Send(Ok(Down::Overloaded(Overloaded {
            reason: "full".into(),
        })))])
    }

    fn succeeds(_: usize) -> Result<Vec<Step>, Status> {
        Ok(vec![
            Step::Send(Ok(Down::Started(Started {}))),
            Step::Send(Ok(Down::Result(RunResult::default()))),
        ])
    }

    fn drops_after_start(_: usize) -> Result<Vec<Step>, Status> {
        Ok(vec![Step::Send(Ok(Down::Started(Started {})))])
    }

    fn drops_before_start(_: usize) -> Result<Vec<Step>, Status> {
        Ok(vec![])
    }

    fn fails_precondition(_: usize) -> Result<Vec<Step>, Status> {
        Ok(vec![Step::Send(Err(Status::failed_precondition(
            "wrong deployment",
        )))])
    }

    fn rejects_call(_: usize) -> Result<Vec<Step>, Status> {
        Err(Status::unavailable("worker going away"))
    }

    fn drops_after_start_then_succeeds(call: usize) -> Result<Vec<Step>, Status> {
        if call == 0 {
            drops_after_start(call)
        } else {
            succeeds(call)
        }
    }

    fn stalls_after_start(_: usize) -> Result<Vec<Step>, Status> {
        Ok(vec![Step::Send(Ok(Down::Started(Started {}))), Step::Stall])
    }

    fn response_head() -> Frame {
        Ok(Down::HttpResponseHead(pb::common::HttpActionResponseHead {
            status: 200,
            http_headers: vec![],
        }))
    }

    fn echoes_http_body(_: usize) -> Result<Vec<Step>, Status> {
        Ok(vec![
            Step::ExpectQuiet,
            Step::Send(Ok(Down::Started(Started {}))),
            Step::ReadBody,
            Step::Send(response_head()),
            Step::Send(Ok(Down::HttpResponseBody(b"pong".to_vec()))),
            Step::Send(Ok(Down::Result(RunResult::default()))),
        ])
    }

    fn stalls_after_response_head(_: usize) -> Result<Vec<Step>, Status> {
        Ok(vec![
            Step::Send(Ok(Down::Started(Started {}))),
            Step::Send(response_head()),
            Step::Stall,
        ])
    }

    #[tokio::test]
    async fn overloaded_worker_is_excluded_and_another_one_runs_the_action() {
        let (busy, busy_calls) = start_fake(overloaded).await;
        let (idle, idle_calls) = start_fake(succeeds).await;
        let pool = pool_of(&[&busy, &idle]);
        // A module whose home is the overloaded worker.
        let module = (0..)
            .map(|i| format!("m{i}.js"))
            .find(|m| pool.choose(m, &BTreeSet::new()).unwrap().0 == busy)
            .unwrap();

        let result = run(&pool, &module, UdfType::Action).await.unwrap();

        assert_eq!(result, RunResult::default());
        assert_eq!(busy_calls.load(Ordering::SeqCst), 1);
        assert_eq!(idle_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn action_is_not_retried_after_started() {
        let (addr, calls) = start_fake(drops_after_start).await;
        let pool = pool_of(&[&addr]);

        let err = run(&pool, "m.js", UdfType::Action).await.unwrap_err();

        assert!(
            format!("{err:#}").contains("ended without a result"),
            "{err:#}"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn action_is_not_retried_when_stream_drops_before_started() {
        let (addr, calls) = start_fake(drops_before_start).await;
        let pool = pool_of(&[&addr]);

        let err = run(&pool, "m.js", UdfType::Action).await.unwrap_err();

        assert!(
            format!("{err:#}").contains("ended without a result"),
            "{err:#}"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn failed_call_never_delivered_the_run_request() {
        let (addr, fake) = start_fake_worker(rejects_call).await;
        let pool = pool_of(&[&addr]);

        let err = run(&pool, "m.js", UdfType::Action).await.unwrap_err();

        assert!(format!("{err:#}").contains("worker going away"), "{err:#}");
        // Nothing was delivered, so every attempt was safe to retry.
        assert_eq!(fake.requests.load(Ordering::SeqCst), 0);
        assert_eq!(fake.calls.load(Ordering::SeqCst), 5);
    }

    #[tokio::test]
    async fn deterministic_worker_error_is_not_retried() {
        let (addr, calls) = start_fake(fails_precondition).await;
        let pool = pool_of(&[&addr]);

        let err = run(&pool, "m.js", UdfType::Query).await.unwrap_err();

        assert!(format!("{err:#}").contains("wrong deployment"), "{err:#}");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn mutation_is_retried_after_started() {
        let (addr, calls) = start_fake(drops_after_start_then_succeeds).await;
        let pool = pool_of(&[&addr]);

        let result = run(&pool, "m.js", UdfType::Mutation).await.unwrap();

        assert_eq!(result, RunResult::default());
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn empty_pool_is_overloaded() {
        let err = run(&WorkerPool::empty(), "m.js", UdfType::Query)
            .await
            .unwrap_err();
        assert!(
            format!("{err:#}").contains("no healthy funrun worker"),
            "{err:#}"
        );
    }

    #[tokio::test]
    async fn run_timeout_is_final_even_for_a_query() {
        let (addr, calls) = start_fake(stalls_after_start).await;
        let pool = pool_of(&[&addr]);

        let run = execute_with_retries(
            &pool,
            "m.js",
            RequestKind::Run(UdfType::Query),
            Up::Request(RunRequest::default()),
            None,
            None,
            None,
            Duration::from_millis(200),
        );
        let err = tokio::time::timeout(Duration::from_secs(5), run)
            .await
            .expect("the run timeout should end the call")
            .unwrap_err();

        assert!(format!("{err:#}").contains("did not finish"), "{err:#}");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn grpc_deadline_is_the_run_timeout() {
        let (addr, fake) = start_fake_worker(succeeds).await;
        let pool = pool_of(&[&addr]);

        run(&pool, "m.js", UdfType::Query).await.unwrap();

        // tonic encodes the 10s deadline in microseconds.
        assert_eq!(*fake.grpc_timeouts.lock(), ["10000000u"]);
    }

    #[tokio::test]
    async fn http_action_streams_body_after_started_and_response_to_streamer() {
        let (addr, fake) = start_fake_worker(echoes_http_body).await;
        let pool = pool_of(&[&addr]);
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut streamer = HttpActionResponseStreamer::new(tx);
        let body = futures::stream::iter([Ok(Bytes::from("pi")), Ok(Bytes::from("ng"))]).boxed();

        let terminal = execute_with_retries(
            &pool,
            "m.js",
            RequestKind::Run(UdfType::HttpAction),
            Up::Request(RunRequest::default()),
            None,
            Some(&mut streamer),
            Some(body),
            RUN_TIMEOUT,
        )
        .await
        .unwrap();

        assert!(matches!(terminal, Terminal::Run(r) if r == RunResult::default()));
        // Nothing reached the worker before it sent `Started`.
        assert_eq!(fake.early.load(Ordering::SeqCst), 0);
        let chunk = |data: &[u8], end| BodyChunk {
            data: data.to_vec(),
            end,
        };
        assert_eq!(
            *fake.bodies.lock(),
            vec![chunk(b"pi", false), chunk(b"ng", false), chunk(b"", true)]
        );
        assert!(
            matches!(rx.recv().await, Some(HttpActionResponsePart::Head(h)) if h.status == 200)
        );
        assert!(
            matches!(rx.recv().await, Some(HttpActionResponsePart::BodyChunk(b)) if b == "pong")
        );
    }

    #[tokio::test]
    async fn dropped_http_client_ends_the_run_without_retry() {
        let (addr, calls) = start_fake(stalls_after_response_head).await;
        let pool = pool_of(&[&addr]);
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut streamer = HttpActionResponseStreamer::new(tx);

        let run = execute_with_retries(
            &pool,
            "m.js",
            RequestKind::Run(UdfType::HttpAction),
            Up::Request(RunRequest::default()),
            None,
            Some(&mut streamer),
            None,
            RUN_TIMEOUT,
        );
        let client = async move {
            // The client goes away mid-response, after the head.
            assert!(rx.recv().await.is_some());
            drop(rx);
        };
        let (result, ()) = tokio::join!(run, client);

        let err = result.unwrap_err();
        assert!(err.is_client_disconnect(), "{err:#}");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
