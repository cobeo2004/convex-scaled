//! The `Funrun` gRPC service: `Execute` runs one function on upstream's
//! `FunctionRunnerCore` and streams its progress back, `WatchLoad` reports
//! how busy this worker is.

use std::{
    future::Future,
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
    http::{
        fetch::{
            FetchClient,
            ProxiedFetchClient,
        },
        MakeSocket,
    },
    knobs::{
        FUNRUN_ISOLATE_ACTIVE_THREADS,
        MAX_FUNRUN_RUN_FUNCTION_REQUEST_MESSAGE_SIZE,
        MAX_FUNRUN_RUN_FUNCTION_RESPONSE_MESSAGE_SIZE,
    },
    log_lines::LogLine,
    runtime::tokio_spawn,
};
use errors::ErrorMetadataAnyhowExt;
use function_runner::server::{
    FunctionRunnerCore,
    HttpActionMetadata,
    RunRequestArgs,
};
use funrun_proto::{
    auth::{
        check_bearer,
        check_protocol,
    },
    deploy::{
        DeployCall,
        DeployReturn,
    },
    http::response_part_to_down,
    request::run_request_from_proto,
    transaction::run_result_to_proto,
    FUNRUN_PROTOCOL_VERSION,
};
use futures::{
    stream::BoxStream,
    StreamExt,
};
use isolate::{
    isolate_worker::FunctionRunnerIsolateWorker,
    ConcurrencyLimiter,
    IsolateConfig,
};
use keybroker::{
    DeploymentSecret,
    FunctionRunnerKeyBroker,
    KeyBroker,
};
use node_executor::local::LocalNodeExecutor;
use pb::error_metadata::ErrorMetadataStatusExt;
use pb_funrun::funrun::{
    deploy_result,
    execute_down::Inner as Down,
    execute_up::Inner as Up,
    funrun_server::{
        Funrun,
        FunrunServer,
    },
    BodyChunk,
    DeployRequest,
    DeployResult,
    ExecuteDown,
    ExecuteUp,
    LoadReport,
    NodeRequest,
    NodeResult,
    Overloaded,
    Started,
    WatchLoadRequest,
};
use runtime::prod::ProdRuntime;
use serde_json::Value as JsonValue;
use tokio::sync::{
    mpsc,
    oneshot,
    watch,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{
    Request,
    Response,
    Status,
    Streaming,
};
use udf::{
    HttpActionRequest,
    HttpActionResponsePart,
    HttpActionResponseStreamer,
};

use crate::{
    config::WorkerKind,
    host_client::{
        EagerTableCounts,
        HostChannel,
        RemoteActionCallbacks,
        RemoteIndexReader,
        RemoteTextSnapshot,
    },
    load::{
        effective_load,
        CpuSampler,
        LoadInputs,
        LoadTargets,
    },
    WorkerStorage,
};

const LOAD_REPORT_INTERVAL: Duration = Duration::from_millis(500);
/// The conductor sends the RunRequest right after the response headers.
const FIRST_FRAME_TIMEOUT: Duration = Duration::from_secs(10);

type DownSender = mpsc::Sender<Result<ExecuteDown, Status>>;

#[derive(Clone)]
pub struct FunrunService {
    rt: ProdRuntime,
    core: FunctionRunnerCore<ProdRuntime, WorkerStorage>,
    storage: WorkerStorage,
    host: HostChannel,
    key_broker: FunctionRunnerKeyBroker,
    fetch_client: Arc<dyn FetchClient>,
    instance_name: String,
    token: String,
    kind: WorkerKind,
    /// Most `Execute` streams in flight at once.
    capacity: usize,
    in_flight: Arc<AtomicUsize>,
    draining: Arc<watch::Sender<bool>>,
    /// `Some` exactly on Node workers.
    node: Option<Arc<LocalNodeExecutor>>,
}

impl FunrunService {
    /// Mirrors `InProcessFunctionRunner::new`'s isolate setup.
    pub fn new(
        rt: ProdRuntime,
        host: HostChannel,
        instance_name: &str,
        instance_secret: &str,
        convex_http_proxy: Option<url::Url>,
        kind: WorkerKind,
        capacity: usize,
        node: Option<Arc<LocalNodeExecutor>>,
    ) -> anyhow::Result<Self> {
        let key_broker =
            KeyBroker::new(instance_name, DeploymentSecret::try_from(instance_secret)?)?
                .function_runner_keybroker();
        let fetch_client = Arc::new(ProxiedFetchClient::new(
            convex_http_proxy,
            instance_name.to_string(),
            reqwest::redirect::Policy::none(),
        ));
        let limiter = if *FUNRUN_ISOLATE_ACTIVE_THREADS > 0 {
            ConcurrencyLimiter::new(*FUNRUN_ISOLATE_ACTIVE_THREADS)
        } else {
            ConcurrencyLimiter::unlimited()
        };
        let isolate_worker =
            FunctionRunnerIsolateWorker::new(rt.clone(), IsolateConfig::new("funrun", limiter));
        let storage = WorkerStorage::default();
        let core = FunctionRunnerCore::new(
            rt.clone(),
            storage.clone(),
            // ponytail: one worker serves one deployment, which is the
            // scheduler's only client, like `InProcessFunctionRunner`.
            100,
            isolate_worker,
        )?;
        Ok(Self {
            rt,
            core,
            storage,
            host,
            key_broker,
            fetch_client,
            instance_name: instance_name.to_string(),
            token: funrun_proto::auth::worker_token(instance_secret),
            kind,
            capacity,
            in_flight: Arc::new(AtomicUsize::new(0)),
            draining: Arc::new(watch::Sender::new(false)),
            node,
        })
    }

    /// Ends every `WatchLoad` stream, now and later, so the conductor marks
    /// this worker unhealthy and stops sending it work while it drains.
    pub fn start_draining(&self) {
        self.draining.send_replace(true);
    }

    pub fn in_flight(&self) -> usize {
        self.in_flight.load(Ordering::SeqCst)
    }

    pub async fn shutdown(&self) -> anyhow::Result<()> {
        self.core.shutdown().await
    }

    pub async fn serve(
        self,
        addr: impl MakeSocket,
        shutdown: impl Future<Output = ()> + Send + 'static,
    ) -> anyhow::Result<()> {
        // The conductor sends RunRequests and receives results, the mirror
        // image of the `function_host` limits.
        let service = FunrunServer::new(self)
            .max_decoding_message_size(*MAX_FUNRUN_RUN_FUNCTION_REQUEST_MESSAGE_SIZE)
            .max_encoding_message_size(*MAX_FUNRUN_RUN_FUNCTION_RESPONSE_MESSAGE_SIZE);
        common::grpc::ConvexGrpcService::new()
            .add_service(service)
            .serve(addr, shutdown)
            .await
    }

    /// Runs one Execute stream. Returns when the run finished and its result
    /// was sent, or, for runs and Node actions, as soon as the client goes
    /// away, which drops (cancels) the run future and releases the in-flight
    /// slot. A deploy runs to completion (see `deploy`).
    async fn run(&self, mut up: Streaming<ExecuteUp>, tx: &DownSender) -> Result<(), Status> {
        let first = tokio::time::timeout(FIRST_FRAME_TIMEOUT, up.message())
            .await
            .map_err(|_| {
                Status::deadline_exceeded("no RunRequest within the first-frame timeout")
            })??;
        let Some(ExecuteUp { inner: Some(first) }) = first else {
            return Err(Status::invalid_argument(
                "Execute stream had no first frame",
            ));
        };
        match (self.kind, first) {
            (WorkerKind::Isolate, Up::Request(request)) => self.run_request(request, up, tx).await,
            (WorkerKind::Isolate, Up::Deploy(request)) => self.deploy(request, tx).await,
            (WorkerKind::Node, Up::Node(request)) => self.node(request, tx).await,
            (kind @ WorkerKind::Isolate, other @ Up::Node(_))
            | (kind @ WorkerKind::Node, other @ (Up::Request(_) | Up::Deploy(_))) => {
                Err(Status::failed_precondition(format!(
                    "{kind:?} worker cannot run a {} request",
                    frame_name(&other)
                )))
            },
            (WorkerKind::Isolate | WorkerKind::Node, Up::HttpRequestBody(_)) => Err(
                Status::invalid_argument("first Execute frame must be a request"),
            ),
        }
    }

    /// `None` when the worker is full; the caller answers `Overloaded`.
    fn try_acquire(&self) -> Option<InFlightGuard> {
        InFlightGuard::try_acquire(&self.in_flight, self.capacity)
    }

    async fn overloaded(&self, tx: &DownSender) -> Result<(), Status> {
        let reason = format!("{} requests already in flight", self.capacity);
        send(tx, Down::Overloaded(Overloaded { reason })).await
    }

    /// Deploy-time evaluation: no storage or host, just the isolate. Unlike
    /// `run` and `node`, it has no `tx.closed()` early cancellation: a
    /// client that goes away does not stop it. The isolate's user and system
    /// timeouts bound it instead.
    async fn deploy(&self, request: DeployRequest, tx: &DownSender) -> Result<(), Status> {
        let Some(_in_flight) = self.try_acquire() else {
            return self.overloaded(tx).await;
        };
        let call = DeployCall::try_from(request)
            .map_err(|e| Status::invalid_argument(format!("invalid DeployRequest: {e:#}")))?;
        send(tx, Down::Started(Started {})).await?;
        let name = self.instance_name.clone();
        let result = match call {
            DeployCall::Analyze {
                udf_config,
                modules,
                environment_variables,
            } => self
                .core
                .analyze(udf_config, modules, environment_variables, name)
                .await
                .map(|r| r.map(DeployReturn::Analyze)),
            DeployCall::AppDefinitions {
                app_definition,
                component_definitions,
                dependency_graph,
                user_environment_variables,
                system_env_vars,
            } => self
                .core
                .evaluate_app_definitions(
                    app_definition,
                    component_definitions,
                    dependency_graph,
                    user_environment_variables,
                    system_env_vars,
                    name,
                )
                .await
                .map(|r| Ok(DeployReturn::AppDefinitions(r))),
            DeployCall::ComponentInitializer {
                evaluated_definitions,
                path,
                definition,
                args,
                name: component_name,
            } => self
                .core
                .evaluate_component_initializer(
                    evaluated_definitions,
                    path,
                    definition,
                    args,
                    component_name,
                    name,
                )
                .await
                .map(|r| Ok(DeployReturn::ComponentInitializer(r))),
            DeployCall::Schema {
                bundle,
                source_map,
                rng_seed,
                unix_timestamp,
            } => self
                .core
                .evaluate_schema(bundle, source_map, rng_seed, unix_timestamp, name)
                .await
                .map(|s| Ok(DeployReturn::Schema(s))),
            DeployCall::AuthConfig {
                bundle,
                source_map,
                environment_variables,
                explanation,
            } => self
                .core
                .evaluate_auth_config(
                    bundle,
                    source_map,
                    environment_variables,
                    &explanation,
                    name,
                )
                .await
                .map(|c| Ok(DeployReturn::AuthConfig(c))),
        };
        let result = match result {
            Ok(Ok(ret)) => {
                deploy_result::Result::Json(Vec::try_from(ret).map_err(Status::from_anyhow)?)
            },
            Ok(Err(js)) => {
                deploy_result::Result::JsError(js.try_into().map_err(Status::from_anyhow)?)
            },
            // After `Started`, so the conductor counts an `Overloaded` here
            // as a lost call; user errors keep their `ErrorMetadata`.
            Err(e) => return send(tx, run_error_to_down(e)?).await,
        };
        send(
            tx,
            Down::DeployResult(DeployResult {
                result: Some(result),
            }),
        )
        .await
    }

    /// A `"use node"` action on the local Node executor: `Started`, its log
    /// lines, then `NodeResult`.
    async fn node(&self, request: NodeRequest, tx: &DownSender) -> Result<(), Status> {
        let Some(_in_flight) = self.try_acquire() else {
            return self.overloaded(tx).await;
        };
        let json: JsonValue = serde_json::from_slice(&request.executor_request_json)
            .map_err(|e| Status::invalid_argument(format!("invalid NodeRequest: {e}")))?;
        let node = self
            .node
            .as_ref()
            .ok_or_else(|| Status::internal("node executor missing"))?;
        let (log_tx, log_rx) = mpsc::unbounded_channel();
        // ponytail: reuse `drive` for ordering and cancellation. The executor
        // has no "started" signal, so Started is pre-fired and goes first.
        let (started_tx, started_rx) = oneshot::channel();
        let _ = started_tx.send(());
        let (_resp_tx, resp_rx) = mpsc::unbounded_channel();
        drive(
            node.invoke_json(json, log_tx),
            started_rx,
            log_rx,
            resp_rx,
            tx,
            |resp| {
                Ok(Down::NodeResult(NodeResult {
                    response_json: serde_json::to_vec(&resp.response)
                        .map_err(|e| Status::internal(e.to_string()))?,
                    aws_request_id: resp.aws_request_id,
                }))
            },
        )
        .await
    }

    async fn run_request(
        &self,
        request: pb_funrun::funrun::RunRequest,
        up: Streaming<ExecuteUp>,
        tx: &DownSender,
    ) -> Result<(), Status> {
        let Some(_in_flight) = self.try_acquire() else {
            return self.overloaded(tx).await;
        };
        let parts = run_request_from_proto(request)
            .map_err(|e| Status::invalid_argument(format!("invalid RunRequest: {e:#}")))?;
        ensure_same_instance(&self.instance_name, &parts.instance_name)?;
        self.storage
            .init(self.rt.clone(), &parts.s3_prefix)
            .await
            .map_err(|e| Status::failed_precondition(format!("{e:#}")))?;

        // Unbounded because upstream's `log_line_sender` and
        // `HttpActionResponseStreamer` take `UnboundedSender`s; the loop below
        // drains both into the bounded down stream.
        let (log_tx, log_rx) = mpsc::unbounded_channel();
        let (started_tx, started_rx) = oneshot::channel();
        let (resp_tx, resp_rx) = mpsc::unbounded_channel();
        // Keep the request half open while the run lives, even when it has no
        // body to read.
        let mut up = Some(up);
        let http_meta = parts.http.map(|http| HttpActionMetadata {
            http_response_streamer: HttpActionResponseStreamer::new(resp_tx),
            http_module_path: http.http_module_path,
            routed_path: http.routed_path,
            http_request: HttpActionRequest {
                head: http.head,
                body: if http.has_body {
                    up.take().map(body_stream)
                } else {
                    None
                },
            },
        });
        let args = RunRequestArgs {
            key_broker: self.key_broker.clone(),
            index_reader: Arc::new(RemoteIndexReader::new(self.host.clone(), parts.ts)),
            convex_origin: parts.convex_origin,
            bootstrap_metadata: parts.bootstrap_metadata,
            table_count_snapshot: Arc::new(EagerTableCounts(parts.table_counts)),
            text_index_snapshot: Arc::new(RemoteTextSnapshot::new(self.host.clone(), parts.ts)),
            action_callbacks: Arc::new(RemoteActionCallbacks::new(self.host.clone())),
            fetch_client: self.fetch_client.clone(),
            log_line_sender: Some(log_tx),
            function_started_sender: Some(started_tx),
            udf_type: parts.udf_type,
            identity: parts.identity,
            existing_writes: parts.existing_writes,
            default_system_env_vars: parts.default_system_env_vars,
            in_memory_index_last_modified: parts.in_memory_index_last_modified,
            context: parts.context,
            subfunctions_in_same_isolate: parts.subfunctions_in_same_isolate,
            deployment: parts.deployment,
        };
        let run =
            self.core
                .run_function_no_retention_check(args, parts.function_metadata, http_meta);
        drive(
            run,
            started_rx,
            log_rx,
            resp_rx,
            tx,
            |(transaction, outcome, usage)| {
                run_result_to_proto(transaction, outcome, usage)
                    .map(Down::Result)
                    .map_err(Status::from_anyhow)
            },
        )
        .await
    }
}

/// Streams a run down in order: `Started` once the isolate starts it, log
/// lines and HTTP response parts as they arrive, then the result.
async fn drive<T>(
    run: impl Future<Output = anyhow::Result<T>>,
    mut started_rx: oneshot::Receiver<()>,
    mut log_rx: mpsc::UnboundedReceiver<LogLine>,
    mut resp_rx: mpsc::UnboundedReceiver<HttpActionResponsePart>,
    tx: &DownSender,
    to_result: impl FnOnce(T) -> Result<Down, Status>,
) -> Result<(), Status> {
    tokio::pin!(run);
    // Closed without a send means the scheduler rejected the run, which is
    // still Overloaded: track "sent Started", not "channel done".
    let mut started_open = true;
    let mut started = false;
    loop {
        tokio::select! {
            biased;
            // ponytail: upstream does not stop an HttpAction isolate when
            // its response closes (isolate_worker.rs:196-236); dropping
            // `run` here frees the slot early, same as in process. Stop the
            // isolate too if leaked work shows up in load.
            _ = tx.closed() => return Err(Status::cancelled("Execute stream dropped")),
            signal = &mut started_rx, if started_open => {
                started_open = false;
                if signal.is_ok() {
                    started = true;
                    send(tx, Down::Started(Started {})).await?;
                }
            },
            Some(line) = log_rx.recv() => send(tx, Down::LogLine(line.into())).await?,
            Some(part) = resp_rx.recv() => send_down(tx, response_part_to_down(part)).await?,
            result = &mut run => {
                // The run may have started and finished within one poll.
                if started_open && started_rx.try_recv().is_ok() {
                    started = true;
                    send(tx, Down::Started(Started {})).await?;
                }
                // Flush what the run queued before its result, like
                // `InProcessFunctionRunner::run_http_action`.
                while let Ok(line) = log_rx.try_recv() {
                    send(tx, Down::LogLine(line.into())).await?;
                }
                while let Ok(part) = resp_rx.try_recv() {
                    send_down(tx, response_part_to_down(part)).await?;
                }
                return match result {
                    Ok(value) => send(tx, to_result(value)?).await,
                    // Overloaded only replaces Started: a nested UDF rejected
                    // after Started is a plain failure.
                    Err(e) if !started => send(tx, run_error_to_down(e)?).await,
                    Err(e) => Err(Status::from_anyhow(e)),
                };
            },
        }
    }
}

/// A run the isolate scheduler rejected before executing it (full queue,
/// CoDel expiry, ...) did nothing, so it goes back as `Overloaded` and the
/// conductor retries it elsewhere, whatever its `UdfType`.
fn run_error_to_down(error: anyhow::Error) -> Result<Down, Status> {
    if error.is_rejected_before_execution() {
        Ok(Down::Overloaded(Overloaded {
            reason: format!("{error:#}"),
        }))
    } else {
        Err(Status::from_anyhow(error))
    }
}

fn frame_name(up: &Up) -> &'static str {
    match up {
        Up::Request(_) => "RunRequest",
        Up::HttpRequestBody(_) => "HttpRequestBody",
        Up::Deploy(_) => "DeployRequest",
        Up::Node(_) => "NodeRequest",
    }
}

/// The key broker signs for `serving`, so a request for another instance
/// must not run here. Neither name is echoed back.
fn ensure_same_instance(serving: &str, requested: &str) -> Result<(), Status> {
    if serving == requested {
        Ok(())
    } else {
        Err(Status::failed_precondition(
            "RunRequest is for a different instance than this worker serves",
        ))
    }
}

async fn send(tx: &DownSender, inner: Down) -> Result<(), Status> {
    send_down(tx, ExecuteDown { inner: Some(inner) }).await
}

async fn send_down(tx: &DownSender, down: ExecuteDown) -> Result<(), Status> {
    tx.send(Ok(down))
        .await
        .map_err(|_| Status::cancelled("Execute stream dropped"))
}

/// Request body chunks sent after the RunRequest, up to the `end` chunk.
fn body_stream(up: Streaming<ExecuteUp>) -> BoxStream<'static, anyhow::Result<Bytes>> {
    futures::stream::try_unfold(Some(up), |up| async move {
        let Some(mut up) = up else {
            return Ok(None);
        };
        match up.message().await.map_err(|s| s.into_anyhow())? {
            Some(ExecuteUp {
                inner: Some(Up::HttpRequestBody(BodyChunk { data, end })),
            }) => Ok(Some((Bytes::from(data), (!end).then_some(up)))),
            Some(ExecuteUp {
                inner: Some(Up::Request(_) | Up::Deploy(_) | Up::Node(_)) | None,
            }) => anyhow::bail!("expected an HTTP request body frame"),
            None => anyhow::bail!("Execute stream ended before the request body did"),
        }
    })
    .boxed()
}

/// One in-flight `Execute` slot, released on drop (including cancellation).
struct InFlightGuard(Arc<AtomicUsize>);

impl InFlightGuard {
    fn try_acquire(counter: &Arc<AtomicUsize>, limit: usize) -> Option<Self> {
        counter
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
                (n < limit).then_some(n + 1)
            })
            .ok()
            .map(|_| Self(counter.clone()))
    }
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

#[tonic::async_trait]
impl Funrun for FunrunService {
    type ExecuteStream = ReceiverStream<Result<ExecuteDown, Status>>;
    type WatchLoadStream = ReceiverStream<Result<LoadReport, Status>>;

    async fn execute(
        &self,
        request: Request<Streaming<ExecuteUp>>,
    ) -> Result<Response<Self::ExecuteStream>, Status> {
        check_bearer(request.metadata(), &self.token)?;
        check_protocol(request.metadata())?;
        let (tx, rx) = mpsc::channel(32);
        let this = self.clone();
        // Framing errors go down the stream, so the response headers never
        // wait on the client's first frame.
        tokio_spawn("funrun_execute", async move {
            if let Err(status) = this.run(request.into_inner(), &tx).await {
                // Fails only when the client is already gone.
                let _ = tx.send(Err(status)).await;
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn watch_load(
        &self,
        request: Request<WatchLoadRequest>,
    ) -> Result<Response<Self::WatchLoadStream>, Status> {
        check_bearer(request.metadata(), &self.token)?;
        check_protocol(request.metadata())?;
        let (tx, rx) = mpsc::channel(1);
        let in_flight = self.in_flight.clone();
        let capacity = self.capacity;
        let mut draining = self.draining.subscribe();
        tokio_spawn("funrun_watch_load", async move {
            let targets = LoadTargets::from_knobs();
            let mut sampler = CpuSampler::new();
            let mut interval = tokio::time::interval(LOAD_REPORT_INTERVAL);
            // A slow client or sample must not cause a burst of catch-up
            // reports.
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = interval.tick() => {},
                    // Dropping `tx` ends the stream.
                    _ = draining.wait_for(|draining| *draining) => return,
                }
                // The only place CpuSampler runs: its /proc reads block.
                let cpu = sampler.sample();
                let in_flight = in_flight.load(Ordering::SeqCst);
                let report = LoadReport {
                    effective_load: effective_load(
                        &LoadInputs {
                            in_flight,
                            max_isolate_workers: capacity,
                            cpu_util: cpu.util,
                            cpu_psi: cpu.psi,
                        },
                        &targets,
                    ),
                    in_flight: u32::try_from(in_flight).unwrap_or(u32::MAX),
                    protocol_version: FUNRUN_PROTOCOL_VERSION,
                };
                if tx.send(Ok(report)).await.is_err() {
                    return;
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{
            AtomicUsize,
            Ordering,
        },
        Arc,
    };

    use errors::ErrorMetadata;
    use pb_funrun::funrun::{
        execute_down::Inner as Down,
        Overloaded,
    };
    use tokio::sync::{
        mpsc,
        oneshot,
    };

    use super::{
        drive,
        ensure_same_instance,
        run_error_to_down,
        InFlightGuard,
    };

    fn rejected() -> anyhow::Error {
        anyhow::anyhow!("queue").context(ErrorMetadata::rejected_before_execution(
            "ExpiredInQueue",
            "too long in queue",
        ))
    }

    /// Runs `drive` with a run that may signal Started before failing with
    /// `rejected()`, and returns the frames sent down (or the error status).
    async fn frames(signal_started: bool) -> (Vec<&'static str>, Option<tonic::Code>) {
        let (started_tx, started_rx) = oneshot::channel();
        let (_log_tx, log_rx) = mpsc::unbounded_channel();
        let (_resp_tx, resp_rx) = mpsc::unbounded_channel();
        let (tx, mut rx) = mpsc::channel(8);
        // Like the isolate scheduler: the sender is gone by the time the run
        // resolves, whether or not it was used.
        let run = async move {
            if signal_started {
                started_tx.send(()).unwrap();
            } else {
                drop(started_tx);
            }
            // Resolve on a later poll, so `drive` sees the channel first.
            tokio::task::yield_now().await;
            Err::<(), _>(rejected())
        };
        let status = drive(run, started_rx, log_rx, resp_rx, &tx, |()| unreachable!())
            .await
            .err()
            .map(|s| s.code());
        drop(tx);
        let mut names = vec![];
        while let Some(Ok(down)) = rx.recv().await {
            names.push(match down.inner {
                Some(Down::Started(_)) => "Started",
                Some(Down::Overloaded(_)) => "Overloaded",
                _ => "other",
            });
        }
        (names, status)
    }

    #[tokio::test]
    async fn rejected_before_start_is_overloaded_even_if_started_channel_closes_first() {
        assert_eq!(frames(false).await, (vec!["Overloaded"], None));
    }

    #[tokio::test]
    async fn rejected_after_started_is_a_plain_error() {
        let (names, status) = frames(true).await;
        assert_eq!(names, vec!["Started"]);
        assert!(status.is_some());
    }

    #[test]
    fn admission_rejects_at_limit_and_guard_drop_frees_a_slot() {
        let counter = Arc::new(AtomicUsize::new(0));
        let a = InFlightGuard::try_acquire(&counter, 2).unwrap();
        let _b = InFlightGuard::try_acquire(&counter, 2).unwrap();
        assert!(InFlightGuard::try_acquire(&counter, 2).is_none());
        assert_eq!(counter.load(Ordering::SeqCst), 2);
        drop(a);
        let _c = InFlightGuard::try_acquire(&counter, 2).unwrap();
        assert_eq!(counter.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn request_for_another_instance_is_rejected_without_leaking_names() {
        let status = ensure_same_instance("carnitas", "barbacoa").unwrap_err();
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert!(
            !status.message().contains("carnitas"),
            "{}",
            status.message()
        );
        assert!(
            !status.message().contains("barbacoa"),
            "{}",
            status.message()
        );
        ensure_same_instance("carnitas", "carnitas").unwrap();
    }

    #[test]
    fn rejected_before_execution_is_sent_as_overloaded() {
        let Ok(Down::Overloaded(Overloaded { reason })) = run_error_to_down(rejected()) else {
            panic!("expected Overloaded");
        };
        assert!(reason.contains("too long in queue"), "{reason}");
    }

    #[test]
    fn other_run_errors_stay_errors() {
        let error = anyhow::anyhow!(ErrorMetadata::bad_request("Bad", "nope"));
        assert!(run_error_to_down(error).is_err());
    }

    #[test]
    fn dropped_guards_return_counter_to_zero() {
        let counter = Arc::new(AtomicUsize::new(0));
        let guards: Vec<_> = (0..3)
            .map(|_| InFlightGuard::try_acquire(&counter, 3).unwrap())
            .collect();
        drop(guards);
        assert_eq!(counter.load(Ordering::SeqCst), 0);
    }
}
