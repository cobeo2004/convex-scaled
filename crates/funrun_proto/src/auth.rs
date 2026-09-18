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

pub const AUTHORIZATION: &str = "authorization";
pub const MODULE_HEADER: &str = "x-convex-module";

/// base64(HMAC-SHA256(INSTANCE_SECRET, "funrun")). Shared by conductor and
/// workers; derived so no extra secret has to be distributed.
pub fn funrun_token(instance_secret: &str) -> String {
    let key = hmac::Key::new(hmac::HMAC_SHA256, instance_secret.as_bytes());
    base64::encode(hmac::sign(&key, b"funrun").as_ref())
}

pub fn check_bearer(metadata: &MetadataMap, expected: &str) -> Result<(), Status> {
    let presented = metadata
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .ok_or_else(|| Status::unauthenticated("missing funrun bearer token"))?;
    verify_slices_are_equal(presented.as_bytes(), expected.as_bytes())
        .map_err(|_| Status::unauthenticated("invalid funrun bearer token"))
}

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
        Ok(request)
    }
}

#[cfg(test)]
mod tests {
    use tonic::metadata::MetadataMap;

    use super::*;

    #[test]
    fn token_is_deterministic_and_secret_dependent() {
        assert_eq!(funrun_token("a"), funrun_token("a"));
        assert_ne!(funrun_token("a"), funrun_token("b"));
    }

    #[test]
    fn check_bearer_accepts_matching_and_rejects_others() {
        let token = funrun_token("secret");
        let mut md = MetadataMap::new();
        assert_eq!(
            check_bearer(&md, &token).unwrap_err().code(),
            tonic::Code::Unauthenticated
        );
        md.insert(
            AUTHORIZATION,
            format!("Bearer {}", funrun_token("other")).parse().unwrap(),
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
            token: funrun_token("s"),
        };
        let req =
            tonic::service::Interceptor::call(&mut interceptor, tonic::Request::new(())).unwrap();
        check_bearer(req.metadata(), &funrun_token("s")).unwrap();
    }
}
