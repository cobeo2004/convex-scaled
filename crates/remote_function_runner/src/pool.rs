//! The set of funrun workers the conductor can send functions to, with each
//! worker's load as reported over `WatchLoad`.

use std::{
    collections::{
        BTreeMap,
        BTreeSet,
    },
    net::SocketAddr,
    sync::{
        atomic::{
            AtomicBool,
            Ordering,
        },
        Arc,
    },
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
use funrun_proto::{
    auth::BearerInterceptor,
    FUNRUN_PROTOCOL_VERSION,
};
use parking_lot::Mutex;
use pb_funrun::funrun::{
    funrun_client::FunrunClient,
    LoadReport,
    WatchLoadRequest,
};
use tonic::{
    service::interceptor::InterceptedService,
    transport::Channel,
};

use crate::{
    pick::{
        pick,
        WorkerState,
    },
    retry::is_transport_failure,
};

pub type FunrunChannel = FunrunClient<InterceptedService<Channel, BearerInterceptor>>;

const DNS_REFRESH_INTERVAL: Duration = Duration::from_secs(5);
const RECONNECT_DELAY: Duration = Duration::from_secs(1);
const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(30);

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
    // Family of the first address DNS returned; see `prefer_family`.
    prefer_ipv6: AtomicBool,
    /// `"isolate"` or `"node"`; labels this pool's metrics.
    #[allow(dead_code)] // ponytail: read by the pool gauges (Task 8).
    name: &'static str,
}

impl WorkerPool {
    /// `target` is `host:port`. `token` is the funrun bearer token. `name`
    /// labels the pool's metrics.
    pub fn start<RT: Runtime>(
        rt: RT,
        target: String,
        mode: RoutingMode,
        token: String,
        name: &'static str,
    ) -> anyhow::Result<Arc<Self>> {
        let pool = Self::named(name);
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
        let states = prefer_family(
            workers.values().map(|w| w.state.clone()).collect(),
            self.prefer_ipv6.load(Ordering::Relaxed),
        );
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

    pub fn has_healthy(&self) -> bool {
        self.workers.lock().values().any(|w| w.state.healthy)
    }

    #[cfg(test)]
    pub(crate) fn empty() -> Arc<Self> {
        Self::named("test")
    }

    fn named(name: &'static str) -> Arc<Self> {
        Arc::new(Self {
            workers: Mutex::new(BTreeMap::new()),
            prefer_ipv6: AtomicBool::new(false),
            name,
        })
    }

    async fn refresh_loop<RT: Runtime>(self: Arc<Self>, rt: RT, target: String, token: String) {
        loop {
            match tokio::net::lookup_host(&target).await {
                Ok(addrs) => {
                    let addrs: Vec<SocketAddr> = addrs.collect();
                    if let Some(first) = addrs.first() {
                        self.prefer_ipv6.store(first.is_ipv6(), Ordering::Relaxed);
                    }
                    let addrs: BTreeSet<String> = addrs.iter().map(|a| a.to_string()).collect();
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
        let mut delay = RECONNECT_DELAY;
        loop {
            match client.watch_load(WatchLoadRequest {}).await {
                Ok(response) => {
                    let mut reports = response.into_inner();
                    loop {
                        match reports.message().await {
                            Ok(Some(report)) => {
                                if let Err(e) = check_protocol_version(&report) {
                                    // Backs off like a rejected call, which
                                    // rate-limits this log line.
                                    tracing::error!("funrun worker {addr} unusable: {e:#}");
                                    delay = (delay * 2).min(MAX_RECONNECT_DELAY);
                                    break;
                                }
                                delay = RECONNECT_DELAY;
                                self.update(&addr, |s| {
                                    s.load = report.effective_load;
                                    s.healthy = true;
                                });
                            },
                            Ok(None) => {
                                tracing::warn!("funrun worker {addr} ended WatchLoad");
                                break;
                            },
                            Err(status) => {
                                tracing::warn!("funrun worker {addr} WatchLoad failed: {status}");
                                delay = reconnect_delay(delay, &status);
                                break;
                            },
                        }
                    }
                },
                Err(status) => {
                    tracing::warn!("funrun worker {addr} unreachable: {status}");
                    delay = reconnect_delay(delay, &status);
                },
            }
            self.update(&addr, |s| s.healthy = false);
            rt.wait(delay).await;
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

/// A worker from another build may silently drop fields it doesn't know, so
/// it never gets work.
fn check_protocol_version(report: &LoadReport) -> anyhow::Result<()> {
    anyhow::ensure!(
        report.protocol_version == FUNRUN_PROTOCOL_VERSION,
        "worker speaks funrun protocol version {}, this conductor {FUNRUN_PROTOCOL_VERSION}; run \
         the same build on conductor and workers",
        report.protocol_version
    );
    Ok(())
}

/// Reconnects quickly after a lost connection, but backs off exponentially
/// when the worker keeps rejecting the call (e.g. a wrong token).
fn reconnect_delay(delay: Duration, status: &tonic::Status) -> Duration {
    if is_transport_failure(status) {
        RECONNECT_DELAY
    } else {
        (delay * 2).min(MAX_RECONNECT_DELAY)
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

/// Dual-stack DNS lists each worker once per family. Route within the family
/// the resolver put first (RFC 6724 order) while any of its workers is
/// healthy, so each worker counts once. The other family stays in the pool as
/// a fallback: that order is a preference, not proof the route works, and
/// workers may listen on IPv4 only.
fn prefer_family(states: Vec<WorkerState>, prefer_ipv6: bool) -> Vec<WorkerState> {
    let preferred = |s: &WorkerState| s.addr.starts_with('[') == prefer_ipv6;
    if states.iter().any(|s| s.healthy && preferred(s)) {
        states.into_iter().filter(preferred).collect()
    } else {
        states
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use funrun_proto::FUNRUN_PROTOCOL_VERSION;
    use pb_funrun::funrun::LoadReport;
    use tonic::Status;

    use super::{
        check_protocol_version,
        prefer_family,
        reconnect_delay,
        WorkerState,
        MAX_RECONNECT_DELAY,
        RECONNECT_DELAY,
    };

    #[test]
    fn dual_stack_workers_route_within_the_preferred_family() {
        let state = |addr: &str, healthy| WorkerState {
            addr: addr.to_string(),
            load: 0.0,
            in_flight: 0,
            healthy,
        };
        let addrs = |states: Vec<WorkerState>| -> Vec<String> {
            states.into_iter().map(|s| s.addr).collect()
        };
        let pool = || {
            vec![
                state("10.0.0.1:7400", true),
                state("[fd12::1]:7400", true),
                state("[fd12::2]:7400", false),
            ]
        };
        assert_eq!(
            addrs(prefer_family(pool(), true)),
            ["[fd12::1]:7400", "[fd12::2]:7400"]
        );
        assert_eq!(addrs(prefer_family(pool(), false)), ["10.0.0.1:7400"]);
        // No healthy IPv6 worker (broken route, IPv4-only listeners): fall
        // back.
        let v6_down = vec![state("10.0.0.1:7400", true), state("[fd12::1]:7400", false)];
        assert_eq!(addrs(prefer_family(v6_down, true)).len(), 2);
    }

    #[test]
    fn rejected_watch_load_backs_off_up_to_the_cap() {
        let rejected = Status::unauthenticated("bad token");
        let mut delay = RECONNECT_DELAY;
        let mut delays = vec![];
        for _ in 0..7 {
            delay = reconnect_delay(delay, &rejected);
            delays.push(delay.as_secs());
        }
        assert_eq!(delays, [2, 4, 8, 16, 30, 30, 30]);
        assert_eq!(MAX_RECONNECT_DELAY, Duration::from_secs(30));
    }

    #[test]
    fn report_from_another_protocol_version_is_rejected() {
        let report = |protocol_version| LoadReport {
            effective_load: 0.0,
            in_flight: 0,
            protocol_version,
        };
        check_protocol_version(&report(FUNRUN_PROTOCOL_VERSION)).unwrap();
        let err = check_protocol_version(&report(FUNRUN_PROTOCOL_VERSION + 1)).unwrap_err();
        assert!(format!("{err:#}").contains("protocol version"), "{err:#}");
        // A worker from before the field existed reports 0.
        check_protocol_version(&report(0)).unwrap_err();
    }

    #[test]
    fn lost_connection_reconnects_quickly() {
        let lost = Status::unavailable("connection reset");
        assert_eq!(reconnect_delay(MAX_RECONNECT_DELAY, &lost), RECONNECT_DELAY);
    }
}
