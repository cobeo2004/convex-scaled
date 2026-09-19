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
    execute_down::Inner as Down,
    execute_up::Inner as Up,
    BodyChunk,
    ExecuteDown,
    ExecuteUp,
    Overloaded,
    RunRequest,
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

pub mod pick;
pub mod pool;
pub mod retry;

pub struct RemoteFunctionRunner<RT: Runtime> {
    pool: Arc<WorkerPool>,
    local: InProcessFunctionRunner<RT>,
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
        local: InProcessFunctionRunner<RT>,
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
            udf_type,
            request,
            log_line_sender.as_ref(),
            http_response.as_mut(),
            http_body,
            *FUNRUN_RUN_FUNCTION_TIMEOUT,
        )
        .await
        .and_then(|result| {
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
        self.local
            .analyze(udf_config, modules, environment_variables)
            .await
    }

    async fn evaluate_app_definitions(
        &self,
        app_definition: ModuleConfig,
        component_definitions: BTreeMap<ComponentDefinitionPath, ModuleConfig>,
        dependency_graph: BTreeSet<(ComponentDefinitionPath, ComponentDefinitionPath)>,
        user_environment_variables: BTreeMap<EnvVarName, EnvVarValue>,
        system_env_vars: BTreeMap<EnvVarName, EnvVarValue>,
    ) -> anyhow::Result<EvaluateAppDefinitionsResult> {
        self.local
            .evaluate_app_definitions(
                app_definition,
                component_definitions,
                dependency_graph,
                user_environment_variables,
                system_env_vars,
            )
            .await
    }

    async fn evaluate_component_initializer(
        &self,
        evaluated_definitions: BTreeMap<ComponentDefinitionPath, ComponentDefinitionMetadata>,
        path: ComponentDefinitionPath,
        definition: ModuleConfig,
        args: BTreeMap<Identifier, Resource>,
        name: ComponentName,
    ) -> anyhow::Result<BTreeMap<Identifier, Resource>> {
        self.local
            .evaluate_component_initializer(evaluated_definitions, path, definition, args, name)
            .await
    }

    async fn evaluate_schema(
        &self,
        schema_bundle: ModuleSource,
        source_map: Option<SourceMap>,
        rng_seed: [u8; 32],
        unix_timestamp: UnixTimestamp,
    ) -> anyhow::Result<DatabaseSchema> {
        self.local
            .evaluate_schema(schema_bundle, source_map, rng_seed, unix_timestamp)
            .await
    }

    async fn evaluate_auth_config(
        &self,
        auth_config_bundle: ModuleSource,
        source_map: Option<SourceMap>,
        environment_variables: BTreeMap<EnvVarName, EnvVarValue>,
        explanation: &str,
    ) -> anyhow::Result<AuthConfig> {
        self.local
            .evaluate_auth_config(
                auth_config_bundle,
                source_map,
                environment_variables,
                explanation,
            )
            .await
    }

    fn set_action_callbacks(&self, action_callbacks: Arc<dyn ActionCallbacks>) {
        self.local.set_action_callbacks(action_callbacks);
    }
}

type BodyStream = BoxStream<'static, anyhow::Result<Bytes>>;

struct AttemptFailure {
    stage: FailureStage,
    error: anyhow::Error,
}

/// Runs `request` on a worker, retrying on other workers only when
/// `may_retry` allows it. Each attempt gets `run_timeout`; the gRPC deadline
/// alone only bounds the response headers, which the worker sends at once.
async fn execute_with_retries(
    pool: &Arc<WorkerPool>,
    module: &str,
    udf_type: UdfType,
    request: RunRequest,
    log_line_sender: Option<&mpsc::UnboundedSender<LogLine>>,
    mut http_response: Option<&mut HttpActionResponseStreamer>,
    mut http_body: Option<BodyStream>,
    run_timeout: Duration,
) -> anyhow::Result<RunResult> {
    let module_header =
        AsciiMetadataValue::try_from(module).context("module path is not a valid header")?;
    let mut exclude = BTreeSet::new();
    let mut attempt = 0;
    loop {
        // Once every worker is excluded, fall back to all of them: in proxy
        // mode the only address is a load balancer.
        let Some((addr, client)) = pool
            .choose(module, &exclude)
            .or_else(|| pool.choose(module, &BTreeSet::new()))
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
                udf_type,
                module_header.clone(),
                request.clone(),
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
                "funrun worker {addr} did not finish {udf_type:?} in {module} within \
                 {run_timeout:?}"
            )
        })??;
        let failure = match attempt_result {
            Ok(result) => return Ok(result),
            Err(failure) => failure,
        };
        if !may_retry(
            RequestKind::Run(udf_type),
            failure.stage,
            attempt,
            *FUNRUN_CLIENT_MAX_RETRIES,
        ) {
            return Err(failure.error);
        }
        tracing::warn!(
            "retrying {udf_type:?} in {module}: funrun worker {addr} failed {:?}: {:#}",
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
    udf_type: UdfType,
    module: AsciiMetadataValue,
    request: RunRequest,
    log_line_sender: Option<&mpsc::UnboundedSender<LogLine>>,
    mut http_response: Option<&mut HttpActionResponseStreamer>,
    http_body: &mut Option<BodyStream>,
    run_timeout: Duration,
) -> anyhow::Result<Result<RunResult, AttemptFailure>> {
    // Open the call with an empty request stream and send the RunRequest only
    // once the worker answered with headers (it does so without waiting for
    // the first frame). A failed call therefore never delivered it.
    let (up_tx, up_rx) = mpsc::channel(8);
    let mut req = tonic::Request::new(ReceiverStream::new(up_rx));
    req.metadata_mut().insert(MODULE_HEADER, module);
    req.set_timeout(run_timeout);
    let mut down = match client.execute(req).await {
        Ok(response) => response.into_inner(),
        // The worker never received the RunRequest: it is not sent yet.
        Err(status) if is_transport_failure(&status) => {
            return Ok(Err(AttemptFailure {
                stage: failure_stage(RequestKind::Run(udf_type), Delivery::NotSent),
                error: status.into_anyhow(),
            }));
        },
        Err(status) => return Err(status.into_anyhow()),
    };
    let request = ExecuteUp {
        inner: Some(Up::Request(request)),
    };
    // A failed send means the call already ended and the RunRequest was not
    // handed to it, so nothing was sent.
    if up_tx.send(request).await.is_err() {
        return Ok(Err(AttemptFailure {
            stage: failure_stage(RequestKind::Run(udf_type), Delivery::NotSent),
            error: anyhow::anyhow!("funrun Execute stream closed before the RunRequest was sent"),
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
                            stage: failure_stage(RequestKind::Run(udf_type), lost(started)),
                            error: status.into_anyhow(),
                        }));
                    },
                    Err(status) => return Err(status.into_anyhow()),
                    Ok(None) => {
                        return Ok(Err(AttemptFailure {
                            stage: failure_stage(RequestKind::Run(udf_type), lost(started)),
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
                            stage: failure_stage(RequestKind::Run(udf_type), delivery),
                            error: error.into(),
                        }));
                    },
                    Down::Result(result) => return Ok(Ok(result)),
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

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeSet,
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
        grpc::ConvexGrpcService,
        types::UdfType,
    };
    use errors::ErrorMetadataAnyhowExt;
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
        Overloaded,
        RunRequest,
        RunResult,
        Started,
        WatchLoadRequest,
    };
    use tokio::{
        net::{
            TcpSocket,
            TcpStream,
        },
        sync::mpsc,
    };
    use tonic::{
        Request,
        Response,
        Status,
        Streaming,
    };
    use udf::{
        HttpActionResponsePart,
        HttpActionResponseStreamer,
    };

    use super::execute_with_retries;
    use crate::pool::{
        connect_for_test,
        WorkerPool,
    };

    type Frame = Result<Down, Status>;

    /// Long enough that only the run-timeout test ever hits it.
    const RUN_TIMEOUT: Duration = Duration::from_secs(10);

    enum Step {
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
    type Script = fn(usize) -> Result<Vec<Step>, Status>;

    /// Answers the n-th Execute call (from 0) with `script(n)`, counting calls
    /// and RunRequests received.
    #[derive(Clone)]
    struct FakeWorker {
        calls: Arc<AtomicUsize>,
        requests: Arc<AtomicUsize>,
        early: Arc<AtomicUsize>,
        bodies: Arc<Mutex<Vec<BodyChunk>>>,
        grpc_timeouts: Arc<Mutex<Vec<String>>>,
        script: Script,
    }

    async fn read_request(up: &mut Streaming<ExecuteUp>, requests: &AtomicUsize) {
        if let Ok(Some(ExecuteUp {
            inner: Some(Up::Request(_)),
        })) = up.message().await
        {
            requests.fetch_add(1, Ordering::SeqCst);
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
                        read_request(&mut up, &requests).await;
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

    async fn start_fake(script: Script) -> (String, Arc<AtomicUsize>) {
        let (addr, fake) = start_fake_worker(script).await;
        (addr, fake.calls)
    }

    async fn start_fake_worker(script: Script) -> (String, FakeWorker) {
        let fake = FakeWorker {
            calls: Arc::new(AtomicUsize::new(0)),
            requests: Arc::new(AtomicUsize::new(0)),
            early: Arc::new(AtomicUsize::new(0)),
            bodies: Arc::new(Mutex::new(Vec::new())),
            grpc_timeouts: Arc::new(Mutex::new(Vec::new())),
            script,
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

    fn pool_of(addrs: &[&str]) -> Arc<WorkerPool> {
        let pool = WorkerPool::empty();
        for addr in addrs {
            pool.insert_healthy(addr.to_string(), connect_for_test(addr));
        }
        pool
    }

    async fn run(
        pool: &Arc<WorkerPool>,
        module: &str,
        udf_type: UdfType,
    ) -> anyhow::Result<RunResult> {
        execute_with_retries(
            pool,
            module,
            udf_type,
            RunRequest::default(),
            None,
            None,
            None,
            RUN_TIMEOUT,
        )
        .await
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
            UdfType::Query,
            RunRequest::default(),
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

        let result = execute_with_retries(
            &pool,
            "m.js",
            UdfType::HttpAction,
            RunRequest::default(),
            None,
            Some(&mut streamer),
            Some(body),
            RUN_TIMEOUT,
        )
        .await
        .unwrap();

        assert_eq!(result, RunResult::default());
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
            UdfType::HttpAction,
            RunRequest::default(),
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
