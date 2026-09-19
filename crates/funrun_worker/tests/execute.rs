//! Execute/WatchLoad framing and auth over loopback gRPC. Nothing here runs
//! a function, so the host channel points nowhere and is never dialled.

use std::{
    future::Future,
    net::SocketAddr,
    time::Duration,
};

use funrun_proto::auth::{
    host_token,
    worker_token,
    BearerInterceptor,
};
use funrun_worker::{
    execute::FunrunService,
    host_client::connect_host,
};
use pb_funrun::funrun::{
    execute_up,
    funrun_client::FunrunClient,
    BodyChunk,
    ExecuteUp,
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
    let tokio = ProdRuntime::init_tokio().unwrap();
    let rt = ProdRuntime::new(&tokio);
    rt.clone().block_on("test", async move {
        let host = connect_host("http://127.0.0.1:9", host_token(SECRET)).unwrap();
        let service = FunrunService::new(rt, host, "carnitas", SECRET, None).unwrap();
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
