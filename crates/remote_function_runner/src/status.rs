//! A point-in-time snapshot of the conductor's worker pools, served as JSON
//! at `/api/funrun/status` and rendered by `/funrun/status`.

use std::{
    sync::Arc,
    time::Instant,
};

use serde::Serialize;

use crate::{
    metrics::{
        fallback_total,
        FallbackKind,
    },
    pool::WorkerPool,
};

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerStatus {
    pub addr: String,
    pub healthy: bool,
    pub load: f64,
    pub in_flight: usize,
    pub last_report_ms: Option<u64>,
}

pub struct FunrunStatus {
    isolate: Arc<WorkerPool>,
    node: Option<Arc<WorkerPool>>,
    fallback: &'static str,
    started: Instant,
}

impl FunrunStatus {
    pub fn new(
        isolate: Arc<WorkerPool>,
        node: Option<Arc<WorkerPool>>,
        fallback: &'static str,
    ) -> Self {
        Self {
            isolate,
            node,
            fallback,
            started: Instant::now(),
        }
    }

    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "conductor": {
                "version": metrics::SERVER_VERSION_STR.clone(),
                "uptimeS": self.started.elapsed().as_secs(),
                "fallback": self.fallback,
                "fallbackTotal": {
                    "isolate": fallback_total(FallbackKind::Isolate),
                    "deploy": fallback_total(FallbackKind::Deploy),
                    "node": fallback_total(FallbackKind::Node),
                },
            },
            "pools": {
                "isolate": self.isolate.snapshot(),
                "node": self.node.as_ref().map(|p| p.snapshot()),
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use std::time::{
        Duration,
        Instant,
    };

    use super::FunrunStatus;
    use crate::pool::{
        connect_for_test,
        WorkerPool,
    };

    // ponytail: `connect_for_test` lazily connects, which needs a Tokio
    // reactor even though nothing here awaits; `#[tokio::test]` (not
    // `#[test]`, as the brief had it) supplies one.
    #[tokio::test]
    async fn snapshot_reports_stale_worker() {
        let pool = WorkerPool::empty();
        pool.insert_healthy("a:1".into(), connect_for_test("http://a:1"));
        pool.set_last_report_for_test("a:1", Instant::now() - Duration::from_secs(5));
        let s = pool.snapshot();
        assert_eq!(s.len(), 1);
        assert!(s[0].last_report_ms.unwrap() >= 5000);
    }

    #[test]
    fn status_json_has_null_node_pool_when_unset() {
        let st = FunrunStatus::new(WorkerPool::empty(), None, "fail");
        let v = st.json();
        assert!(v["pools"]["node"].is_null());
        assert_eq!(v["conductor"]["fallback"], "fail");
    }
}
