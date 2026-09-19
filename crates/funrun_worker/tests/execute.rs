//! Execute/WatchLoad framing and auth over loopback gRPC. Nothing here runs
//! a user function (deploy-time evaluation needs no host), so the host
//! channel points nowhere and is never dialled.

use std::{
    collections::BTreeMap,
    future::Future,
    net::SocketAddr,
    time::Duration,
};

use funrun_proto::{
    auth::{
        host_token,
        worker_token,
        BearerInterceptor,
    },
    deploy::{
        decode_return,
        DeployCall,
        DeployReturn,
    },
};
use funrun_worker::{
    config::WorkerKind,
    execute::FunrunService,
    host_client::connect_host,
};
use model::modules::module_versions::ModuleSource;
use pb_funrun::funrun::{
    deploy_result,
    execute_down,
    execute_up,
    funrun_client::FunrunClient,
    BodyChunk,
    ExecuteUp,
    NodeRequest,
    RunRequest,
    WatchLoadRequest,
};
use runtime::prod::ProdRuntime;
use tokio::net::{
    TcpSocket,
    TcpStream,
};
use tonic::{
    codegen::InterceptedService,
    transport::Channel,
};

const SECRET: &str = "4361726e697461732c206c69746572616c6c79206d65616e696e6720226c6974";

fn with_worker<F, Fut>(test: F)
where
    F: FnOnce(SocketAddr, FunrunService) -> Fut,
    Fut: Future<Output = ()>,
{
    with_kind_worker(WorkerKind::Isolate, test)
}

fn with_kind_worker<F, Fut>(kind: WorkerKind, test: F)
where
    F: FnOnce(SocketAddr, FunrunService) -> Fut,
    Fut: Future<Output = ()>,
{
    let tokio = ProdRuntime::init_tokio().unwrap();
    let rt = ProdRuntime::new(&tokio);
    rt.clone().block_on("test", async move {
        let host = connect_host("http://127.0.0.1:9", host_token(SECRET)).unwrap();
        let service = FunrunService::new(rt, host, "carnitas", SECRET, None, kind, 4).unwrap();
        let socket = TcpSocket::new_v4().unwrap();
        socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = socket.local_addr().unwrap();
        tokio::spawn(service.clone().serve(socket, std::future::pending()));
        tokio::time::timeout(Duration::from_secs(5), async {
            while TcpStream::connect(addr).await.is_err() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        test(addr, service).await;
    });
}

async fn authed_client(
    addr: SocketAddr,
) -> FunrunClient<InterceptedService<Channel, BearerInterceptor>> {
    let channel = Channel::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    FunrunClient::with_interceptor(
        channel,
        BearerInterceptor {
            token: worker_token(SECRET),
        },
    )
}

fn request_frame() -> ExecuteUp {
    ExecuteUp {
        inner: Some(execute_up::Inner::Request(RunRequest::default())),
    }
}

#[test]
fn execute_without_token_is_rejected() {
    with_worker(|addr, _| async move {
        let mut client = FunrunClient::connect(format!("http://{addr}"))
            .await
            .unwrap();
        let err = client
            .execute(tokio_stream::iter(vec![request_frame()]))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    });
}

#[test]
fn watch_load_without_token_is_rejected() {
    with_worker(|addr, _| async move {
        let mut client = FunrunClient::connect(format!("http://{addr}"))
            .await
            .unwrap();
        let err = client.watch_load(WatchLoadRequest {}).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    });
}

/// Proxy mode has no WatchLoad, so this header is the only version check a
/// worker gets from a conductor on another build.
#[test]
fn calls_without_protocol_version_are_rejected() {
    with_worker(|addr, _| async move {
        let channel = Channel::from_shared(format!("http://{addr}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let bearer: tonic::metadata::MetadataValue<_> =
            format!("Bearer {}", worker_token(SECRET)).parse().unwrap();
        let mut client =
            FunrunClient::with_interceptor(channel, move |mut req: tonic::Request<()>| {
                req.metadata_mut().insert("authorization", bearer.clone());
                Ok(req)
            });
        let err = client
            .execute(tokio_stream::iter(vec![request_frame()]))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        let err = client.watch_load(WatchLoadRequest {}).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    });
}

#[test]
fn first_frame_must_be_request() {
    with_worker(|addr, _| async move {
        let mut client = authed_client(addr).await;
        let body_first = ExecuteUp {
            inner: Some(execute_up::Inner::HttpRequestBody(BodyChunk {
                data: vec![],
                end: true,
            })),
        };
        let mut down = client
            .execute(tokio_stream::iter(vec![body_first]))
            .await
            .unwrap()
            .into_inner();
        let err = down.message().await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    });
}

#[test]
fn undecodable_request_is_invalid_argument() {
    with_worker(|addr, _| async move {
        let mut client = authed_client(addr).await;
        let mut down = client
            .execute(tokio_stream::iter(vec![request_frame()]))
            .await
            .unwrap()
            .into_inner();
        let err = down.message().await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    });
}

#[test]
fn watch_load_emits_reports() {
    with_worker(|addr, _| async move {
        let mut client = authed_client(addr).await;
        let mut stream = client
            .watch_load(WatchLoadRequest {})
            .await
            .unwrap()
            .into_inner();
        let report = stream.message().await.unwrap().unwrap();
        assert!((0.0..=1.0).contains(&report.effective_load));
        assert_eq!(report.in_flight, 0);
        assert_eq!(
            report.protocol_version,
            funrun_proto::FUNRUN_PROTOCOL_VERSION
        );
    });
}

#[test]
fn draining_ends_watch_load_streams() {
    with_worker(|addr, service| async move {
        let mut client = authed_client(addr).await;
        let mut stream = client
            .watch_load(WatchLoadRequest {})
            .await
            .unwrap()
            .into_inner();
        stream.message().await.unwrap().unwrap();
        service.start_draining();
        // The conductor sees the stream end and marks the worker unhealthy.
        let end = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if stream.message().await.unwrap().is_none() {
                    return;
                }
            }
        });
        end.await.expect("WatchLoad should end once draining");
    });
}

#[test]
fn isolate_worker_rejects_node_request() {
    with_worker(|addr, _| async move {
        let mut client = authed_client(addr).await;
        let node = ExecuteUp {
            inner: Some(execute_up::Inner::Node(NodeRequest {
                executor_request_json: b"{}".to_vec(),
            })),
        };
        let mut down = client
            .execute(tokio_stream::iter(vec![node]))
            .await
            .unwrap()
            .into_inner();
        let err = down.message().await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    });
}

#[test]
fn node_worker_rejects_deploy_request() {
    with_kind_worker(WorkerKind::Node, |addr, _| async move {
        let mut client = authed_client(addr).await;
        let mut down = client
            .execute(tokio_stream::iter(vec![auth_config_frame()]))
            .await
            .unwrap()
            .into_inner();
        let err = down.message().await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    });
}

fn auth_config_frame() -> ExecuteUp {
    let call = DeployCall::AuthConfig {
        bundle: ModuleSource::new("export default { providers: [] };"),
        source_map: None,
        environment_variables: BTreeMap::new(),
        explanation: "test".into(),
    };
    ExecuteUp {
        inner: Some(execute_up::Inner::Deploy(call.try_into().unwrap())),
    }
}

#[test]
fn isolate_worker_evaluates_auth_config() {
    with_worker(|addr, _| async move {
        let mut client = authed_client(addr).await;
        let mut down = client
            .execute(tokio_stream::iter(vec![auth_config_frame()]))
            .await
            .unwrap()
            .into_inner();
        let mut frames = vec![];
        while let Some(frame) = down.message().await.unwrap() {
            frames.push(frame.inner.unwrap());
        }
        let [execute_down::Inner::Started(_), execute_down::Inner::DeployResult(result)] =
            &frames[..]
        else {
            panic!("expected Started then DeployResult, got {frames:?}");
        };
        let Some(deploy_result::Result::Json(b)) = &result.result else {
            panic!("no json result: {result:?}");
        };
        assert!(matches!(
            decode_return(b).unwrap(),
            DeployReturn::AuthConfig(c) if c.providers.is_empty()
        ));
    });
}
