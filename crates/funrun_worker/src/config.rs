use clap::Parser;

// No `Debug`: it would print `instance_secret`.
#[derive(Parser, Clone)]
#[clap(name = "funrun_worker")]
pub struct WorkerConfig {
    #[clap(long, env = "FUNRUN_LISTEN", default_value = "0.0.0.0:7400")]
    pub listen: std::net::SocketAddr,
    #[clap(long, env = "FUNCTION_HOST_URL")]
    pub function_host_url: String,
    #[clap(long, env = "INSTANCE_NAME")]
    pub instance_name: String,
    #[clap(long, env = "INSTANCE_SECRET")]
    pub instance_secret: String,
    #[clap(long, env = "CONVEX_HTTP_PROXY")]
    pub convex_http_proxy: Option<url::Url>,
    /// If set, serve Prometheus metrics on `http://<addr>/metrics`.
    #[clap(long, env = "FUNRUN_METRICS_LISTEN")]
    pub metrics_listen: Option<std::net::SocketAddr>,
    /// Which requests this worker runs: V8 functions and deploy-time
    /// evaluation (`isolate`), or `"use node"` actions (`node`).
    #[clap(long, env = "FUNRUN_KIND", value_enum, default_value = "isolate")]
    pub kind: WorkerKind,
    /// In-flight Node actions a `node` worker accepts.
    #[clap(long, env = "FUNRUN_NODE_MAX_CONCURRENT", default_value_t = 4)]
    pub node_max_concurrent: usize,
    /// How long a `node` worker waits for in-flight actions on shutdown.
    #[clap(long, env = "FUNRUN_NODE_DRAIN_TIMEOUT_SECS")]
    pub node_drain_timeout_secs: Option<u64>,
}

#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkerKind {
    Isolate,
    Node,
}
