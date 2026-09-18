use clap::Parser;
use cmd_util::env::config_service;
use common::errors::MainError;
use funrun_worker::{
    config::WorkerConfig,
    run_worker,
};
use runtime::prod::ProdRuntime;

fn main() -> Result<(), MainError> {
    let config = WorkerConfig::parse();
    let _guard = config_service();
    let tokio = ProdRuntime::init_tokio()?;
    let runtime = ProdRuntime::new(&tokio);
    let rt = runtime.clone();
    runtime.block_on("funrun_worker", run_worker(rt, config))?;
    Ok(())
}
