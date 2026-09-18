use clap::Parser;

#[derive(Parser, Debug, Clone)]
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
}
