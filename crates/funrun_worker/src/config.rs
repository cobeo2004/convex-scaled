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
}
