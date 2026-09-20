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
    time::{
        Duration,
        Instant,
    },
};

use common::{
    knobs::{
        FUNRUN_CLIENT_MAX_REQUESTS_PER_UPSTREAM,
        MAX_FUNRUN_RUN_FUNCTION_RESPONSE_MESSAGE_SIZE,
    },
    runtime::{
        Runtime,
        SpawnHandle,
    },
};
use funrun_proto::{
    auth::BearerInterceptor,
    max_up_message_size,
    FUNRUN_PROTOCOL_VERSION,
};
use metrics::{
    log_gauge_with_labels,
    StaticMetricLabel,
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
    metrics::{
        FUNRUN_POOL_HEALTHY_INFO,
        FUNRUN_WORKER_LOAD_INFO,
    },
    pick::{
        pick,
        WorkerState,
    },
    retry::is_transport_failure,
    status::WorkerStatus,
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
    // Set on every `WatchLoad` report; `None` until the first one arrives.
    last_report: Option<Instant>,
}

pub struct WorkerPool {
    workers: Mutex<BTreeMap<String, Worker>>,
    // Family of the first address DNS returned; see `prefer_family`.
    prefer_ipv6: AtomicBool,
    /// `"isolate"` or `"node"`; labels this pool's metrics.
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
                // The proxy is one endpoint standing in for the whole pool, so
                // it gets the same WatchLoad treatment as a direct worker:
                // unhealthy until it reports, unhealthy again the moment the
                // stream breaks. Without this, `has_healthy()` would stay true
                // through an Envoy outage and `FUNRUN_FALLBACK=local` would
                // never trigger. `effective_load` is whichever backend Envoy
                // picked, but `pick()` has nothing to choose between here.
                let watch = rt.spawn(
                    "funrun_watch_load",
                    pool.clone()
                        .watch_load(rt.clone(), target.clone(), client.clone()),
                );
                pool.insert_watched(target, client, watch);
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

    /// Adds a worker whose health a `WatchLoad` task owns. Unhealthy until
    /// that task sees the first report, exactly like a direct worker.
    fn insert_watched(&self, addr: String, client: FunrunChannel, watch: Box<dyn SpawnHandle>) {
        self.workers.lock().insert(
            addr.clone(),
            Worker {
                client,
                state: WorkerState {
                    addr,
                    load: 0.0,
                    in_flight: 0,
                    healthy: false,
                },
                _watch: Some(watch),
                last_report: None,
            },
        );
    }

    /// Adds an always-healthy worker, bypassing `WatchLoad`. Tests only —
    /// production health always comes from a report.
    #[cfg(test)]
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
                last_report: None,
            },
        );
        self.report_healthy();
    }

    pub fn has_healthy(&self) -> bool {
        self.workers.lock().values().any(|w| w.state.healthy)
    }

    /// A point-in-time view of every worker in the pool, for `/funrun/status`.
    pub fn snapshot(&self) -> Vec<WorkerStatus> {
        self.workers
            .lock()
            .values()
            .map(|w| WorkerStatus {
                addr: w.state.addr.clone(),
                healthy: w.state.healthy,
                load: w.state.load,
                in_flight: w.state.in_flight,
                last_report_ms: w.last_report.map(|t| t.elapsed().as_millis() as u64),
            })
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn empty() -> Arc<Self> {
        Self::named("test")
    }

    #[cfg(test)]
    pub(crate) fn set_last_report_for_test(&self, addr: &str, at: Instant) {
        if let Some(w) = self.workers.lock().get_mut(addr) {
            w.last_report = Some(at);
        }
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
        let before = workers.len();
        // Dropping a worker aborts its WatchLoad task. Its load series goes
        // too, so DNS churn does not grow the gauge's cardinality.
        workers.retain(|addr, _| {
            let keep = addrs.contains(addr);
            if !keep {
                // Err only when the series was never set.
                let _ = FUNRUN_WORKER_LOAD_INFO.remove_label_values(&[self.name, addr]);
            }
            keep
        });
        let worker_removed = workers.len() < before;
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
                    last_report: None,
                },
            );
        }
        drop(workers);
        // New workers start unhealthy, so only a removal can change the
        // healthy count here.
        if worker_removed {
            self.report_healthy();
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
                                // The gauge is set under the workers lock, so
                                // it cannot outlive `sync_workers` removing
                                // this worker's series.
                                self.update(&addr, |w| {
                                    w.state.load = report.effective_load;
                                    w.state.healthy = true;
                                    w.last_report = Some(Instant::now());
                                    log_gauge_with_labels(
                                        &FUNRUN_WORKER_LOAD_INFO,
                                        report.effective_load,
                                        vec![
                                            StaticMetricLabel::new("pool", self.name),
                                            StaticMetricLabel::new("addr", addr.clone()),
                                        ],
                                    );
                                });
                                self.report_healthy();
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
            self.update(&addr, |w| w.state.healthy = false);
            self.report_healthy();
            rt.wait(delay).await;
        }
    }

    /// Applies `f` to the worker at `addr`. Callers that change a worker's
    /// health must also call `report_healthy`; this alone does not, so it
    /// stays cheap to call from a hot path like the in-flight counter.
    fn update(&self, addr: &str, f: impl FnOnce(&mut Worker)) {
        let mut workers = self.workers.lock();
        if let Some(w) = workers.get_mut(addr) {
            f(w);
        }
    }

    /// Reports the pool's healthy worker count (labelled with this pool's
    /// `name`). Call this only from the paths that actually change a
    /// worker's health (`insert_healthy`, worker removal, and the
    /// `watch_load` success/failure transitions) — not from the in-flight
    /// counter, which does not affect health.
    fn report_healthy(&self) {
        let healthy = self
            .workers
            .lock()
            .values()
            .filter(|w| w.state.healthy)
            .count();
        log_gauge_with_labels(
            &FUNRUN_POOL_HEALTHY_INFO,
            healthy as f64,
            vec![StaticMetricLabel::new("pool", self.name)],
        );
    }
}

pub struct InFlightGuard {
    pool: Arc<WorkerPool>,
    addr: String,
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.pool.update(&self.addr, |w| {
            w.state.in_flight = w.state.in_flight.saturating_sub(1)
        });
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
            // One channel carries both Run and Deploy frames; Deploy is the
            // larger of the two because it embeds the push's module source.
            .max_encoding_message_size(max_up_message_size())
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
        connect_for_test,
        prefer_family,
        reconnect_delay,
        WorkerPool,
        WorkerState,
        MAX_RECONNECT_DELAY,
        RECONNECT_DELAY,
    };
    use crate::metrics::FUNRUN_POOL_HEALTHY_INFO;

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

    // ponytail: `connect_for_test` lazily connects, which needs a Tokio
    // reactor even though nothing here awaits; `#[tokio::test]` supplies one.
    #[tokio::test]
    async fn in_flight_decrement_does_not_change_the_healthy_gauge() {
        // Unique pool name: FUNRUN_POOL_HEALTHY_INFO is a process-global
        // metric, and other tests reuse WorkerPool::empty()'s "test" name.
        let pool = WorkerPool::named("gauge_test_in_flight_decrement");
        pool.insert_healthy("w:1".to_string(), connect_for_test("w:1"));
        pool.insert_healthy("w:2".to_string(), connect_for_test("w:2"));
        let healthy_gauge = || {
            FUNRUN_POOL_HEALTHY_INFO
                .with_label_values(&["gauge_test_in_flight_decrement"])
                .get()
        };
        assert_eq!(healthy_gauge(), 2.0);

        // Completing a request changes only `in_flight`, not health, so the
        // healthy gauge must still read correctly afterwards.
        drop(pool.begin("w:1"));
        assert_eq!(healthy_gauge(), 2.0);
        assert!(pool.snapshot().iter().all(|w| w.healthy));
    }
}
