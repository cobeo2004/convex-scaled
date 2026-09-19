use pb_funrun::funrun::{
    execute_down::Inner,
    ExecuteDown,
};
use udf::HttpActionResponsePart;

pub fn response_part_to_down(part: HttpActionResponsePart) -> ExecuteDown {
    let inner = match part {
        HttpActionResponsePart::Head(head) => Inner::HttpResponseHead(head.into()),
        HttpActionResponsePart::BodyChunk(bytes) => Inner::HttpResponseBody(bytes.into()),
    };
    ExecuteDown { inner: Some(inner) }
}

/// `Ok(None)` for frames that are not HTTP response parts.
pub fn down_to_response_part(down: ExecuteDown) -> anyhow::Result<Option<HttpActionResponsePart>> {
    Ok(match down.inner {
        Some(Inner::HttpResponseHead(head)) => Some(HttpActionResponsePart::Head(head.try_into()?)),
        Some(Inner::HttpResponseBody(bytes)) => {
            Some(HttpActionResponsePart::BodyChunk(bytes.into()))
        },
        Some(
            Inner::Started(_)
            | Inner::LogLine(_)
            | Inner::Result(_)
            | Inner::Overloaded(_)
            | Inner::DeployResult(_)
            | Inner::NodeResult(_),
        )
        | None => None,
    })
}

#[cfg(test)]
mod tests {
    use ::http::{
        HeaderMap,
        HeaderValue,
        StatusCode,
    };
    use udf::HttpActionResponseHead;

    use super::*;

    fn assert_round_trips(part: HttpActionResponsePart) {
        let down = response_part_to_down(part);
        let back = down_to_response_part(down.clone()).unwrap().unwrap();
        assert_eq!(response_part_to_down(back), down);
    }

    #[test]
    fn head_round_trips() {
        let mut headers = HeaderMap::new();
        headers.append("x-multi", HeaderValue::from_static("a"));
        headers.append("x-multi", HeaderValue::from_static("b"));
        assert_round_trips(HttpActionResponsePart::Head(HttpActionResponseHead {
            status: StatusCode::CREATED,
            headers,
        }));
    }

    #[test]
    fn body_chunk_round_trips() {
        assert_round_trips(HttpActionResponsePart::BodyChunk(b"hello".to_vec().into()));
    }

    #[test]
    fn non_http_frame_is_none() {
        let down = pb_funrun::funrun::ExecuteDown {
            inner: Some(pb_funrun::funrun::execute_down::Inner::Started(
                pb_funrun::funrun::Started {},
            )),
        };
        assert!(down_to_response_part(down).unwrap().is_none());
    }
}
