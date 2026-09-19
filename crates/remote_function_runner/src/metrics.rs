use metrics::{
    log_counter_with_labels,
    register_convex_counter,
    StaticMetricLabel,
};

register_convex_counter!(
    FUNRUN_FALLBACK_TOTAL,
    "Requests run in-process because a funrun pool had no healthy worker",
    &["kind"]
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
}

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
}
