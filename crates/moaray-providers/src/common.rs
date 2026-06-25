//! Helpers shared by provider adapters: status mapping and stream relay.

use bytes::Bytes;
use futures_util::StreamExt;
use moaray_core::error::Error;
use moaray_core::provider::{ByteStream, RawResponse};

/// Map an upstream HTTP status into the canonical error matrix. `Ok(())` means
/// the status is a success the caller should relay as-is.
pub fn map_upstream_status(status: u16) -> Result<(), Error> {
    match status {
        s if (200..300).contains(&s) => Ok(()),
        429 => Err(Error::UpstreamRateLimited),
        _ => Err(Error::UpstreamError),
    }
}

/// Map a `reqwest::Error` (transport-level) into the canonical error.
pub fn map_reqwest_error(e: &reqwest::Error) -> Error {
    if e.is_timeout() {
        Error::UpstreamTimeout
    } else {
        Error::UpstreamError
    }
}

/// Collect a non-streaming body into a single-chunk [`RawResponse`].
pub async fn collect_response(resp: reqwest::Response) -> Result<RawResponse, Error> {
    let status = resp.status().as_u16();
    let content_type = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let bytes = resp.bytes().await.map_err(|e| map_reqwest_error(&e))?;
    let body: ByteStream = Box::pin(futures_util::stream::once(async move { Ok(bytes) }));
    Ok(RawResponse {
        status,
        content_type,
        body,
    })
}

/// Build a streaming [`RawResponse`] from an upstream response, relaying frames.
pub fn stream_response(resp: reqwest::Response) -> RawResponse {
    let status = resp.status().as_u16();
    let content_type = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .or_else(|| Some("text/event-stream".to_string()));
    RawResponse {
        status,
        content_type,
        body: relay_stream_inner(resp),
    }
}

fn relay_stream_inner(resp: reqwest::Response) -> ByteStream {
    let stream = resp
        .bytes_stream()
        .map(|item: Result<Bytes, reqwest::Error>| {
            item.map_err(|e| {
                if e.is_timeout() {
                    Error::UpstreamTimeout
                } else {
                    Error::UpstreamError
                }
            })
        });
    Box::pin(stream)
}
