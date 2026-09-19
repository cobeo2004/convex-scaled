//! Stateless remote function runner worker. It has no database: reads and
//! action callbacks go back to the conductor's `function_host` over gRPC.

use std::{
    sync::Arc,
    time::Duration,
};

use anyhow::Context;
use async_trait::async_trait;
use aws_s3::storage::S3Storage;
use common::{
    knobs::BACKEND_REQUEST_DRAIN_TIMEOUT,
    runtime::{
        tokio_spawn,
        Runtime,
    },
};
use database::Transaction;
use function_runner::server::{
    DeploymentStorage,
    StorageForDeployment,
};
use funrun_proto::auth::host_token;
use runtime::prod::ProdRuntime;
use storage::{
    Storage,
    StorageUseCase,
};
use tokio::{
    signal::unix::{
        signal,
        SignalKind,
    },
    sync::{
        oneshot,
        OnceCell,
    },
};

use crate::{
    config::WorkerConfig,
    execute::FunrunService,
    host_client::connect_host,
};

pub mod config;
pub mod execute;
pub mod host_client;
pub mod load;
mod metrics;

/// Files and modules storage for the one deployment this worker serves, in
/// the same S3 buckets as the conductor (`S3_STORAGE_{FILES,MODULES}_BUCKET`).
/// `FunctionRunnerCore` takes its storage at construction, but the S3 prefix
/// only arrives with the first request, so this fills in on first use and
/// then hands out upstream `DeploymentStorage`.
#[derive(Clone, Debug, Default)]
pub struct WorkerStorage(Arc<OnceCell<(String, DeploymentStorage)>>);

impl WorkerStorage {
    /// Builds the storage from the first request's prefix. Later requests
    /// must carry the same prefix: one worker serves one deployment.
    pub async fn init(&self, rt: ProdRuntime, s3_prefix: &str) -> anyhow::Result<()> {
        let (serving, _) = self
            .0
            .get_or_try_init(|| async {
                let storage = DeploymentStorage {
                    files_storage: Arc::new(
                        S3Storage::for_use_case(
                            StorageUseCase::Files,
                            s3_prefix.to_string(),
                            rt.clone(),
                        )
                        .await?,
                    ),
                    modules_storage: Arc::new(
                        S3Storage::for_use_case(StorageUseCase::Modules, s3_prefix.to_string(), rt)
                            .await?,
                    ),
                };
                anyhow::Ok((s3_prefix.to_string(), storage))
            })
            .await?;
        ensure_same_deployment(serving, s3_prefix)
    }
}

// The prefix embeds a per-deployment secret, so the error doesn't print it.
fn ensure_same_deployment(serving: &str, requested: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        serving == requested,
        "request's s3_prefix is not the deployment this worker serves (one worker serves one \
         deployment)"
    );
    Ok(())
}

#[async_trait]
impl<RT: Runtime> StorageForDeployment<RT> for WorkerStorage {
    async fn storage_for_deployment(
        &self,
        transaction: &mut Transaction<RT>,
        use_case: StorageUseCase,
    ) -> anyhow::Result<Arc<dyn Storage>> {
        let (_, storage) = self
            .0
            .get()
            .context("WorkerStorage used before a request set its s3_prefix")?;
        StorageForDeployment::<RT>::storage_for_deployment(storage, transaction, use_case).await
    }
}

/// Serves `Funrun` until SIGTERM/ctrl-c, then stops accepting and waits up to
/// `BACKEND_REQUEST_DRAIN_TIMEOUT` for in-flight runs to finish.
pub async fn run_worker(rt: ProdRuntime, config: WorkerConfig) -> anyhow::Result<()> {
    let host = connect_host(
        &config.function_host_url,
        host_token(&config.instance_secret),
    )?;
    let service = FunrunService::new(
        rt,
        host,
        &config.instance_name,
        &config.instance_secret,
        config.convex_http_proxy.clone(),
    )?;
    let mut sigterm = signal(SignalKind::terminate())?;
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let mut server = tokio_spawn(
        "funrun_grpc",
        service.clone().serve(config.listen, async {
            let _ = shutdown_rx.await;
        }),
    );
    tokio::select! {
        result = &mut server => {
            result??;
            anyhow::bail!("funrun gRPC server stopped unexpectedly");
        },
        _ = sigterm.recv() => {},
        _ = tokio::signal::ctrl_c() => {},
    }
    tracing::info!(
        "Shutting down, draining {} in-flight functions",
        service.in_flight()
    );
    let _ = shutdown_tx.send(());
    let drain = async {
        while service.in_flight() > 0 {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    };
    if tokio::time::timeout(*BACKEND_REQUEST_DRAIN_TIMEOUT, drain)
        .await
        .is_err()
    {
        tracing::warn!(
            "Drain timed out with {} functions still in flight",
            service.in_flight()
        );
    }
    service.shutdown().await
}

#[cfg(test)]
mod tests {
    use super::ensure_same_deployment;

    #[test]
    fn same_prefix_is_accepted() {
        assert!(ensure_same_deployment("dep-1/", "dep-1/").is_ok());
    }

    #[test]
    fn different_prefix_is_rejected_without_leaking_it() {
        let err = ensure_same_deployment("dep-1/", "dep-2/").unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("one worker serves one deployment"));
        assert!(!msg.contains("dep-"));
    }
}
