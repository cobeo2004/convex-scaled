//! Conductor side of Node action execution: `RemoteNodeExecutor` sends
//! `NodeExecutor` requests to the Node worker pool over the same `Execute`
//! call funrun and deploy-time evaluation use.

use std::{
    sync::Arc,
    time::Duration,
};

use anyhow::Context;
use async_trait::async_trait;
use common::{
    knobs::NODE_ACTION_USER_TIMEOUT,
    log_lines::LogLine,
};
use node_executor::{
    ExecutorRequest,
    InvokeResponse,
    NodeExecutor,
};
use pb_funrun::funrun::{
    execute_up::Inner as Up,
    NodeRequest,
};
use serde_json::Value as JsonValue;
use tokio::sync::mpsc;

use crate::{
    execute_with_retries,
    metrics::{
        log_fallback,
        FallbackKind,
    },
    pool::WorkerPool,
    retry::RequestKind,
    Terminal,
};

/// Runs `"use node"` actions on the Node worker pool. `fallback` is set when
/// `FUNRUN_FALLBACK=local`, in which case Node code runs in process (via
/// `LocalNodeExecutor`) while the pool has no healthy worker.
pub struct RemoteNodeExecutor {
    pool: Arc<WorkerPool>,
    fallback: Option<Arc<dyn NodeExecutor>>,
}

impl RemoteNodeExecutor {
    pub fn new(pool: Arc<WorkerPool>, fallback: Option<Arc<dyn NodeExecutor>>) -> Self {
        Self { pool, fallback }
    }
}

#[async_trait]
impl NodeExecutor for RemoteNodeExecutor {
    fn enable(&self) -> anyhow::Result<()> {
        match &self.fallback {
            Some(f) => f.enable(),
            None => Ok(()),
        }
    }

    async fn invoke(
        &self,
        request: ExecutorRequest,
        log_line_sender: mpsc::UnboundedSender<LogLine>,
    ) -> anyhow::Result<InvokeResponse> {
        if let Some(f) = &self.fallback {
            if !self.pool.has_healthy() {
                log_fallback(FallbackKind::Node);
                return f.invoke(request, log_line_sender).await;
            }
        }
        let kind = match &request {
            ExecutorRequest::Execute { .. } => RequestKind::NodeExecute,
            ExecutorRequest::Analyze(_) | ExecutorRequest::BuildDeps(_) => RequestKind::NodePure,
        };
        let json = JsonValue::try_from(request)?;
        let up = Up::Node(NodeRequest {
            executor_request_json: serde_json::to_vec(&json)?,
        });
        let Terminal::Node(r) = execute_with_retries(
            &self.pool,
            "_node",
            kind,
            up,
            Some(&log_line_sender),
            None,
            None,
            *NODE_ACTION_USER_TIMEOUT + Duration::from_secs(30),
        )
        .await?
        else {
            anyhow::bail!("worker answered a node request with a non-node frame");
        };
        Ok(InvokeResponse {
            response: serde_json::from_slice(&r.response_json)
                .context("node worker returned invalid response JSON")?,
            aws_request_id: r.aws_request_id,
        })
    }

    fn shutdown(&self) {
        if let Some(f) = &self.fallback {
            f.shutdown()
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        sync::{
            atomic::{
                AtomicUsize,
                Ordering,
            },
            Arc,
        },
    };

    use common::{
        log_lines::{
            LogLevel,
            LogLine,
        },
        runtime::UnixTimestamp,
        sha256::Sha256Digest,
        types::ObjectKey,
    };
    use node_executor::{
        AnalyzeRequest,
        ExecutorRequest,
        InvokeResponse,
        NodeExecutor,
        Package,
        SourcePackage,
    };
    use pb_funrun::funrun::{
        execute_down::Inner as Down,
        NodeResult,
    };
    use tokio::sync::mpsc;

    use super::RemoteNodeExecutor;
    use crate::{
        pool::WorkerPool,
        test_util::{
            down,
            pool_of,
            start_fake_worker,
            Step,
        },
    };

    // ponytail: no shared helper for this exists yet in the crate (the brief
    // expected one), so these are local to this test module.
    fn log_line_proto(text: &str) -> pb::outcome::LogLine {
        LogLine::new_developer_log_line(
            LogLevel::Log,
            vec![text.to_string()],
            UnixTimestamp::from_nanos(0),
        )
        .into()
    }

    fn log_text(line: &LogLine) -> String {
        match line {
            LogLine::Structured(s) => s.messages.join(" "),
            LogLine::SubFunction { .. } => panic!("unexpected sub function log line"),
        }
    }

    fn analyze_request() -> ExecutorRequest {
        ExecutorRequest::Analyze(AnalyzeRequest {
            source_package: SourcePackage {
                bundled_source: Package {
                    uri: "http://example.com/pkg.zip".to_string(),
                    key: ObjectKey::try_from("pkg.zip").unwrap(),
                    sha256: Sha256Digest::from([0u8; 32]),
                },
                external_deps: None,
            },
            environment_variables: BTreeMap::new(),
        })
    }

    #[derive(Default)]
    struct StubExecutor {
        calls: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl NodeExecutor for StubExecutor {
        fn enable(&self) -> anyhow::Result<()> {
            Ok(())
        }

        async fn invoke(
            &self,
            _request: ExecutorRequest,
            _log_line_sender: mpsc::UnboundedSender<LogLine>,
        ) -> anyhow::Result<InvokeResponse> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(InvokeResponse {
                response: serde_json::json!({}),
                aws_request_id: None,
            })
        }

        fn shutdown(&self) {}
    }

    #[tokio::test]
    async fn forwards_logs_in_order_then_response() -> anyhow::Result<()> {
        let (addr, _fake) = start_fake_worker(|_| {
            Ok(vec![
                Step::Send(down(Down::Started(Default::default()))),
                Step::Send(down(Down::LogLine(log_line_proto("one")))),
                Step::Send(down(Down::LogLine(log_line_proto("two")))),
                Step::Send(down(Down::NodeResult(NodeResult {
                    response_json: br#"{"type":"success"}"#.to_vec(),
                    aws_request_id: Some("r1".into()),
                }))),
            ])
        })
        .await;
        let exec = RemoteNodeExecutor::new(pool_of(&[&addr]), None);
        let (tx, mut rx) = mpsc::unbounded_channel();
        let resp = exec.invoke(analyze_request(), tx).await?;
        assert_eq!(resp.response, serde_json::json!({"type": "success"}));
        assert_eq!(resp.aws_request_id.as_deref(), Some("r1"));
        let lines: Vec<_> = std::iter::from_fn(|| rx.try_recv().ok())
            .map(|l| log_text(&l))
            .collect();
        assert_eq!(lines, ["one", "two"]);
        Ok(())
    }

    #[tokio::test]
    async fn falls_back_when_pool_has_no_healthy_worker() -> anyhow::Result<()> {
        let fallback = Arc::new(StubExecutor::default());
        let exec = RemoteNodeExecutor::new(WorkerPool::empty(), Some(fallback.clone()));
        exec.invoke(analyze_request(), mpsc::unbounded_channel().0)
            .await?;
        assert_eq!(fallback.calls.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn fails_without_fallback_when_pool_is_empty() {
        let exec = RemoteNodeExecutor::new(WorkerPool::empty(), None);
        assert!(exec
            .invoke(analyze_request(), mpsc::unbounded_channel().0)
            .await
            .is_err());
    }
}
