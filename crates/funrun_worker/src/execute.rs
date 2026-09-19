//! The `Funrun` gRPC service: `Execute` runs one function on upstream's
//! `FunctionRunnerCore` and streams its progress back, `WatchLoad` reports
//! how busy this worker is.

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
        MAX_ISOLATE_WORKERS,
    },
    runtime::tokio_spawn,
};
use errors::ErrorMetadataAnyhowExt;
use function_runner::server::{
    FunctionRunnerCore,
    HttpActionMetadata,
    RunRequestArgs,
};
use funrun_proto::{
    auth::check_bearer,
    http::response_part_to_down,
    request::run_request_from_proto,
    transaction::run_result_to_proto,
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
use pb::error_metadata::ErrorMetadataStatusExt;
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
    Started,
    WatchLoadRequest,
};
use runtime::prod::ProdRuntime;
use tokio::sync::{
    mpsc,
    oneshot,
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
    HttpActionResponseStreamer,
};

use crate::{
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
    in_flight: Arc<AtomicUsize>,
}

impl FunrunService {
    /// Mirrors `InProcessFunctionRunner::new`'s isolate setup.
    pub fn new(
        rt: ProdRuntime,
        host: HostChannel,
        instance_name: &str,
        instance_secret: &str,
        convex_http_proxy: Option<url::Url>,
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
            in_flight: Arc::new(AtomicUsize::new(0)),
        })
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
    /// was sent, or as soon as the client goes away, which drops (cancels)
    /// the run future and releases the in-flight slot.
    async fn run(&self, mut up: Streaming<ExecuteUp>, tx: &DownSender) -> Result<(), Status> {
        let first = tokio::time::timeout(FIRST_FRAME_TIMEOUT, up.message())
            .await
            .map_err(|_| {
                Status::deadline_exceeded("no RunRequest within the first-frame timeout")
            })??;
        let Some(ExecuteUp {
            inner: Some(Up::Request(request)),
        }) = first
        else {
            return Err(Status::invalid_argument(
                "first Execute frame must be a RunRequest",
            ));
        };
        let Some(_in_flight) = InFlightGuard::try_acquire(&self.in_flight, *MAX_ISOLATE_WORKERS)
        else {
            let reason = format!("{} functions already in flight", *MAX_ISOLATE_WORKERS);
            return send(tx, Down::Overloaded(Overloaded { reason })).await;
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
        let (log_tx, mut log_rx) = mpsc::unbounded_channel();
        let (started_tx, mut started_rx) = oneshot::channel();
        let (resp_tx, mut resp_rx) = mpsc::unbounded_channel();
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
        tokio::pin!(run);
        let mut started_pending = true;
        loop {
            tokio::select! {
                biased;
                // ponytail: upstream does not stop an HttpAction isolate when
                // its response closes (isolate_worker.rs:196-236); dropping
                // `run` here frees the slot early, same as in process. Stop the
                // isolate too if leaked work shows up in load.
                _ = tx.closed() => return Err(Status::cancelled("Execute stream dropped")),
                started = &mut started_rx, if started_pending => {
                    started_pending = false;
                    if started.is_ok() {
                        send(tx, Down::Started(Started {})).await?;
                    }
                },
                Some(line) = log_rx.recv() => send(tx, Down::LogLine(line.into())).await?,
                Some(part) = resp_rx.recv() => send_down(tx, response_part_to_down(part)).await?,
                result = &mut run => {
                    // Flush what the run queued before its result, like
                    // `InProcessFunctionRunner::run_http_action`.
                    while let Ok(line) = log_rx.try_recv() {
                        send(tx, Down::LogLine(line.into())).await?;
                    }
                    while let Ok(part) = resp_rx.try_recv() {
                        send_down(tx, response_part_to_down(part)).await?;
                    }
                    let (transaction, outcome, usage) = match result {
                        Ok(run) => run,
                        Err(e) => return send(tx, run_error_to_down(e)?).await,
                    };
                    let result = run_result_to_proto(transaction, outcome, usage)
                        .map_err(Status::from_anyhow)?;
                    return send(tx, Down::Result(result)).await;
                },
            }
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
                inner: Some(Up::Request(_)) | None,
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
        let (tx, rx) = mpsc::channel(1);
        let in_flight = self.in_flight.clone();
        tokio_spawn("funrun_watch_load", async move {
            let targets = LoadTargets::from_knobs();
            let mut sampler = CpuSampler::new();
            let mut interval = tokio::time::interval(LOAD_REPORT_INTERVAL);
            // A slow client or sample must not cause a burst of catch-up reports.
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                interval.tick().await;
                // The only place CpuSampler runs: its /proc reads block.
                let cpu = sampler.sample();
                let in_flight = in_flight.load(Ordering::SeqCst);
                let report = LoadReport {
                    effective_load: effective_load(
                        &LoadInputs {
                            in_flight,
                            max_isolate_workers: *MAX_ISOLATE_WORKERS,
                            cpu_util: cpu.util,
                            cpu_psi: cpu.psi,
                        },
                        &targets,
                    ),
                    in_flight: u32::try_from(in_flight).unwrap_or(u32::MAX),
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

    use super::{
        ensure_same_instance,
        run_error_to_down,
        InFlightGuard,
    };

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
        let error = anyhow::anyhow!("queue").context(ErrorMetadata::rejected_before_execution(
            "ExpiredInQueue",
            "too long in queue",
        ));
        let Ok(Down::Overloaded(Overloaded { reason })) = run_error_to_down(error) else {
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
