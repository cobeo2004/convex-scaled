//! Stateless remote function runner worker. It has no database: reads and
//! action callbacks go back to the conductor's `function_host` over gRPC.

use std::{
    sync::Arc,
    time::Duration,
};

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
use funrun_proto::auth::funrun_token;
use model::database_globals::{
    types::StorageType,
    DatabaseGlobalsModel,
};
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
/// The key prefix is a per-deployment secret that lives only in the
/// conductor's `_db` globals, so the first run reads it through its own
/// (host-backed) transaction, which is what `StorageForDeployment` takes the
/// transaction for.
#[derive(Clone, Debug, Default)]
pub struct WorkerStorage(Arc<OnceCell<DeploymentStorage>>);

#[async_trait]
impl<RT: Runtime> StorageForDeployment<RT> for WorkerStorage {
    async fn storage_for_deployment(
        &self,
        transaction: &mut Transaction<RT>,
        use_case: StorageUseCase,
    ) -> anyhow::Result<Arc<dyn Storage>> {
        let storage = self
            .0
            .get_or_try_init(|| async {
                let rt = transaction.runtime().clone();
                let globals = DatabaseGlobalsModel::new(transaction)
                    .database_globals()
                    .await?;
                let s3_prefix = match globals.storage_type.clone() {
                    Some(StorageType::S3 { s3_prefix }) => s3_prefix,
                    Some(StorageType::Local { .. }) | None => anyhow::bail!(
                        "remote function runner workers need the conductor on S3 storage"
                    ),
                };
                anyhow::Ok(DeploymentStorage {
                    files_storage: Arc::new(
                        S3Storage::for_use_case(
                            StorageUseCase::Files,
                            s3_prefix.clone(),
                            rt.clone(),
                        )
                        .await?,
                    ),
                    modules_storage: Arc::new(
                        S3Storage::for_use_case(StorageUseCase::Modules, s3_prefix, rt).await?,
                    ),
                })
            })
            .await?;
        StorageForDeployment::<RT>::storage_for_deployment(storage, transaction, use_case).await
    }
}

/// Serves `Funrun` until SIGTERM/ctrl-c, then stops accepting and waits up to
/// `BACKEND_REQUEST_DRAIN_TIMEOUT` for in-flight runs to finish.
pub async fn run_worker(rt: ProdRuntime, config: WorkerConfig) -> anyhow::Result<()> {
    let host = connect_host(
        &config.function_host_url,
        funrun_token(&config.instance_secret),
    )
    .await?;
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
