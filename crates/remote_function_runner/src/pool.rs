//! The set of funrun workers the conductor can send functions to, with each
//! worker's load as reported over `WatchLoad`.

use std::{
    collections::{
        BTreeMap,
        BTreeSet,
    },
    sync::Arc,
    time::Duration,
};

use common::{
    knobs::{
        FUNRUN_CLIENT_MAX_REQUESTS_PER_UPSTREAM,
        MAX_FUNRUN_RUN_FUNCTION_REQUEST_MESSAGE_SIZE,
        MAX_FUNRUN_RUN_FUNCTION_RESPONSE_MESSAGE_SIZE,
    },
    runtime::{
        Runtime,
        SpawnHandle,
    },
};
use funrun_proto::auth::BearerInterceptor;
use parking_lot::Mutex;
use pb_funrun::funrun::{
    funrun_client::FunrunClient,
    WatchLoadRequest,
};
use tonic::{
    service::interceptor::InterceptedService,
    transport::Channel,
};

use crate::pick::{
    pick,
    WorkerState,
};

pub type FunrunChannel = FunrunClient<InterceptedService<Channel, BearerInterceptor>>;

const DNS_REFRESH_INTERVAL: Duration = Duration::from_secs(5);
const RECONNECT_DELAY: Duration = Duration::from_secs(1);

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum RoutingMode {
    /// `target` is a DNS name resolving to every worker (e.g. a headless
    /// service); the conductor routes by module affinity and load.
    Direct,
    /// `target` is a single load-balanced address.
    Proxy,
}

struct Worker {
    client: FunrunChannel,
    state: WorkerState,
    // Aborts the worker's WatchLoad task when the worker leaves the pool.
    _watch: Option<Box<dyn SpawnHandle>>,
}

pub struct WorkerPool {
    workers: Mutex<BTreeMap<String, Worker>>,
}

impl WorkerPool {
    /// `target` is `host:port`. `token` is the funrun bearer token.
    pub fn start<RT: Runtime>(
        rt: RT,
        target: String,
        mode: RoutingMode,
        token: String,
    ) -> anyhow::Result<Arc<Self>> {
        let pool = Self::empty();
        match mode {
            RoutingMode::Proxy => {
                let client = connect(&target, token)?;
                pool.insert_healthy(target, client);
            },
            RoutingMode::Direct => {
                rt.clone().spawn_background(
                    "funrun_pool_refresh",
                    pool.clone().refresh_loop(rt, target, token),
                );
            },
        }
        Ok(pool)
    }

    /// Picks a worker for `module`, avoiding `exclude`, and returns its
    /// address and a client for it.
    pub fn choose(
        &self,
        module: &str,
        exclude: &BTreeSet<String>,
    ) -> Option<(String, FunrunChannel)> {
        let workers = self.workers.lock();
        let states: Vec<WorkerState> = workers.values().map(|w| w.state.clone()).collect();
        let addr = &pick(
            &states,
            module,
            exclude,
            *FUNRUN_CLIENT_MAX_REQUESTS_PER_UPSTREAM,
            rand::random_range(0..usize::MAX),
        )?
        .addr;
        let worker = workers.get(addr)?;
        Some((addr.clone(), worker.client.clone()))
    }

    /// Counts a request against `addr` until the guard drops.
    pub fn begin(self: &Arc<Self>, addr: &str) -> InFlightGuard {
        if let Some(w) = self.workers.lock().get_mut(addr) {
            w.state.in_flight += 1;
        }
        InFlightGuard {
            pool: self.clone(),
            addr: addr.to_string(),
        }
    }

    /// Adds an always-healthy worker. Proxy mode, and tests.
    pub(crate) fn insert_healthy(&self, addr: String, client: FunrunChannel) {
        self.workers.lock().insert(
            addr.clone(),
            Worker {
                client,
                state: WorkerState {
                    addr,
                    load: 0.0,
                    in_flight: 0,
                    healthy: true,
                },
                _watch: None,
            },
        );
    }

    pub(crate) fn empty() -> Arc<Self> {
        Arc::new(Self {
            workers: Mutex::new(BTreeMap::new()),
        })
    }

    async fn refresh_loop<RT: Runtime>(self: Arc<Self>, rt: RT, target: String, token: String) {
        loop {
            match tokio::net::lookup_host(&target).await {
                Ok(addrs) => {
                    let addrs: BTreeSet<String> = addrs.map(|a| a.to_string()).collect();
                    self.sync_workers(&rt, &addrs, &token);
                },
                Err(e) => tracing::warn!("funrun worker lookup of {target} failed: {e}"),
            }
            rt.wait(DNS_REFRESH_INTERVAL).await;
        }
    }

    fn sync_workers<RT: Runtime>(self: &Arc<Self>, rt: &RT, addrs: &BTreeSet<String>, token: &str) {
        let mut workers = self.workers.lock();
        // Dropping a worker aborts its WatchLoad task.
        workers.retain(|addr, _| addrs.contains(addr));
        for addr in addrs {
            if workers.contains_key(addr) {
                continue;
            }
            let client = match connect(addr, token.to_string()) {
                Ok(client) => client,
                Err(e) => {
                    tracing::warn!("bad funrun worker address {addr}: {e:#}");
                    continue;
                },
            };
            let watch = rt.spawn(
                "funrun_watch_load",
                self.clone()
                    .watch_load(rt.clone(), addr.clone(), client.clone()),
            );
            workers.insert(
                addr.clone(),
                Worker {
                    client,
                    // Unhealthy until its first load report proves it is up.
                    state: WorkerState {
                        addr: addr.clone(),
                        load: 0.0,
                        in_flight: 0,
                        healthy: false,
                    },
                    _watch: Some(watch),
                },
            );
        }
    }

    async fn watch_load<RT: Runtime>(
        self: Arc<Self>,
        rt: RT,
        addr: String,
        mut client: FunrunChannel,
    ) {
        loop {
            match client.watch_load(WatchLoadRequest {}).await {
                Ok(response) => {
                    let mut reports = response.into_inner();
                    loop {
                        match reports.message().await {
                            Ok(Some(report)) => self.update(&addr, |s| {
                                s.load = report.effective_load;
                                s.healthy = true;
                            }),
                            Ok(None) => {
                                tracing::warn!("funrun worker {addr} ended WatchLoad");
                                break;
                            },
                            Err(status) => {
                                tracing::warn!("funrun worker {addr} WatchLoad failed: {status}");
                                break;
                            },
                        }
                    }
                },
                Err(status) => tracing::warn!("funrun worker {addr} unreachable: {status}"),
            }
            self.update(&addr, |s| s.healthy = false);
            rt.wait(RECONNECT_DELAY).await;
        }
    }

    fn update(&self, addr: &str, f: impl FnOnce(&mut WorkerState)) {
        if let Some(w) = self.workers.lock().get_mut(addr) {
            f(&mut w.state);
        }
    }
}

pub struct InFlightGuard {
    pool: Arc<WorkerPool>,
    addr: String,
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.pool
            .update(&self.addr, |s| s.in_flight = s.in_flight.saturating_sub(1));
    }
}

/// Same channel settings as the worker's `connect_host`.
fn connect(addr: &str, token: String) -> anyhow::Result<FunrunChannel> {
    let channel = Channel::from_shared(format!("http://{addr}"))?
        .connect_timeout(Duration::from_secs(5))
        .http2_keep_alive_interval(Duration::from_secs(30))
        .keep_alive_timeout(Duration::from_secs(20))
        .connect_lazy();
    Ok(
        FunrunClient::with_interceptor(channel, BearerInterceptor { token })
            .max_encoding_message_size(*MAX_FUNRUN_RUN_FUNCTION_REQUEST_MESSAGE_SIZE)
            .max_decoding_message_size(*MAX_FUNRUN_RUN_FUNCTION_RESPONSE_MESSAGE_SIZE),
    )
}

#[cfg(test)]
pub(crate) fn connect_for_test(addr: &str) -> FunrunChannel {
    connect(addr, "test-token".to_string()).expect("valid address")
}
