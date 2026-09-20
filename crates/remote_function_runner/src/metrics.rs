use std::sync::atomic::{
    AtomicU64,
    Ordering,
};

use metrics::{
    log_counter_with_labels,
    register_convex_counter,
    register_convex_gauge,
    StaticMetricLabel,
};

register_convex_counter!(
    FUNRUN_FALLBACK_TOTAL,
    "Requests run in-process because a funrun pool had no healthy worker",
    &["kind"]
);

register_convex_gauge!(
    pub FUNRUN_WORKER_LOAD_INFO,
    "Load reported by a funrun worker's most recent WatchLoad message",
    &["pool", "addr"]
);

register_convex_gauge!(
    pub FUNRUN_POOL_HEALTHY_INFO,
    "Number of healthy workers in a funrun worker pool",
    &["pool"]
);

#[derive(Clone, Copy, Debug)]
pub enum FallbackKind {
    Isolate,
    Deploy,
    Node,
}

impl FallbackKind {
    fn label(self) -> &'static str {
        match self {
            Self::Isolate => "isolate",
            Self::Deploy => "deploy",
            Self::Node => "node",
        }
    }

    // ponytail: 3 fixed variants, so 3 fixed counters rather than a map --
    // simplest thing that lets `status.rs` read `fallbackTotal` without a
    // lock.
    fn counter(self) -> &'static AtomicU64 {
        match self {
            Self::Isolate => &FALLBACK_TOTAL_ISOLATE,
            Self::Deploy => &FALLBACK_TOTAL_DEPLOY,
            Self::Node => &FALLBACK_TOTAL_NODE,
        }
    }
}

static FALLBACK_TOTAL_ISOLATE: AtomicU64 = AtomicU64::new(0);
static FALLBACK_TOTAL_DEPLOY: AtomicU64 = AtomicU64::new(0);
static FALLBACK_TOTAL_NODE: AtomicU64 = AtomicU64::new(0);

pub fn log_fallback(kind: FallbackKind) {
    tracing::warn!(
        "funrun: no healthy {} worker, running in-process",
        kind.label()
    );
    log_counter_with_labels(
        &FUNRUN_FALLBACK_TOTAL,
        1,
        vec![StaticMetricLabel::new("kind", kind.label())],
    );
    kind.counter().fetch_add(1, Ordering::Relaxed);
}

/// The fallback counts for `/funrun/status`'s `conductor.fallbackTotal`.
pub fn fallback_total(kind: FallbackKind) -> u64 {
    kind.counter().load(Ordering::Relaxed)
}
