use aws_lc_rs::{
    constant_time::verify_slices_are_equal,
    hmac,
};
use tonic::{
    metadata::MetadataMap,
    service::Interceptor,
    Request,
    Status,
};

use crate::FUNRUN_PROTOCOL_VERSION;

pub const AUTHORIZATION: &str = "authorization";
pub const MODULE_HEADER: &str = "x-convex-module";
pub const PROTOCOL_HEADER: &str = "x-funrun-protocol";

/// Bearer token for conductor -> worker calls (`Execute`, `WatchLoad`).
pub fn worker_token(instance_secret: &str) -> String {
    derive_token(instance_secret, b"funrun-worker")
}

/// Bearer token for worker -> conductor `FunctionHost` calls. Distinct from
/// `worker_token`, so whatever answers a worker address (DNS says so) cannot
/// replay the conductor's token against the `FunctionHost`.
pub fn host_token(instance_secret: &str) -> String {
    derive_token(instance_secret, b"funrun-host")
}

/// base64(HMAC-SHA256(INSTANCE_SECRET, label)), derived so no extra secret
/// has to be distributed.
fn derive_token(instance_secret: &str, label: &[u8]) -> String {
    let key = hmac::Key::new(hmac::HMAC_SHA256, instance_secret.as_bytes());
    base64::encode(hmac::sign(&key, label).as_ref())
}

/// Logs rejections: otherwise a peer with the wrong INSTANCE_SECRET fails
/// silently on the rejecting side.
pub fn check_bearer(metadata: &MetadataMap, expected: &str) -> Result<(), Status> {
    verify_bearer(metadata, expected).inspect_err(|status| {
        tracing::warn!("Unauthenticated funrun call rejected: {}", status.message())
    })
}

fn verify_bearer(metadata: &MetadataMap, expected: &str) -> Result<(), Status> {
    let presented = metadata
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .ok_or_else(|| Status::unauthenticated("missing funrun bearer token"))?;
    verify_slices_are_equal(presented.as_bytes(), expected.as_bytes())
        .map_err(|_| Status::unauthenticated("invalid funrun bearer token"))
}

/// Rejects a peer built with another `FUNRUN_PROTOCOL_VERSION`.
pub fn check_protocol(metadata: &MetadataMap) -> Result<(), Status> {
    let presented = metadata.get(PROTOCOL_HEADER).and_then(|v| v.to_str().ok());
    if presented == Some(FUNRUN_PROTOCOL_VERSION.to_string().as_str()) {
        return Ok(());
    }
    let status = Status::failed_precondition(format!(
        "funrun protocol version mismatch: peer sent {presented:?}, this build speaks \
         {FUNRUN_PROTOCOL_VERSION}; run the same build on conductor and workers"
    ));
    tracing::error!("{}", status.message());
    Err(status)
}

/// Adds the bearer token and `x-funrun-protocol` to every call.
#[derive(Clone)]
pub struct BearerInterceptor {
    pub token: String,
}

impl Interceptor for BearerInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        let value = format!("Bearer {}", self.token)
            .parse()
            .map_err(|_| Status::internal("bad funrun token"))?;
        request.metadata_mut().insert(AUTHORIZATION, value);
        request
            .metadata_mut()
            .insert(PROTOCOL_HEADER, FUNRUN_PROTOCOL_VERSION.into());
        Ok(request)
    }
}

#[cfg(test)]
mod tests {
    use tonic::metadata::MetadataMap;

    use super::*;

    #[test]
    fn tokens_are_deterministic_and_secret_dependent() {
        assert_eq!(worker_token("a"), worker_token("a"));
        assert_ne!(worker_token("a"), worker_token("b"));
        assert_eq!(host_token("a"), host_token("a"));
        assert_ne!(host_token("a"), host_token("b"));
    }

    #[test]
    fn a_token_for_one_direction_is_rejected_in_the_other() {
        let mut md = MetadataMap::new();
        md.insert(
            AUTHORIZATION,
            format!("Bearer {}", worker_token("s")).parse().unwrap(),
        );
        verify_bearer(&md, &worker_token("s")).unwrap();
        assert_eq!(
            verify_bearer(&md, &host_token("s")).unwrap_err().code(),
            tonic::Code::Unauthenticated
        );
    }

    #[test]
    fn check_bearer_accepts_matching_and_rejects_others() {
        let token = worker_token("secret");
        let mut md = MetadataMap::new();
        assert_eq!(
            check_bearer(&md, &token).unwrap_err().code(),
            tonic::Code::Unauthenticated
        );
        md.insert(
            AUTHORIZATION,
            format!("Bearer {}", worker_token("other")).parse().unwrap(),
        );
        assert_eq!(
            check_bearer(&md, &token).unwrap_err().code(),
            tonic::Code::Unauthenticated
        );
        md.insert(AUTHORIZATION, format!("Bearer {token}").parse().unwrap());
        check_bearer(&md, &token).unwrap();
    }

    #[test]
    fn interceptor_adds_header() {
        let mut interceptor = BearerInterceptor {
            token: worker_token("s"),
        };
        let req =
            tonic::service::Interceptor::call(&mut interceptor, tonic::Request::new(())).unwrap();
        check_protocol(req.metadata()).unwrap();
        check_bearer(req.metadata(), &worker_token("s")).unwrap();
    }
}
