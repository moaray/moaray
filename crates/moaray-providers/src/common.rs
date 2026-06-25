//! Helpers shared by provider adapters: status mapping and stream relay.

use bytes::Bytes;
use futures_util::StreamExt;
use moaray_core::error::Error;
use moaray_core::provider::{ByteStream, RawResponse};

/// Correlation header forwarded to every upstream so the request id minted by
/// the gateway (see `moaray::app`) is propagated end-to-end (DESIGN P1-6).
pub const REQUEST_ID_HEADER: &str = "x-request-id";

/// Map an upstream HTTP status into the canonical error matrix. `Ok(())` means
/// the status is a success the caller should relay as-is.
///
/// The 4xx-vs-5xx split is load-bearing for the circuit breaker (plan P3-2): a
/// 4xx means the upstream is up and answering (the fault is the request's or the
/// upstream credential/config), so it maps to the breaker-neutral
/// [`Error::UpstreamClientError`]; a 429 maps to [`Error::UpstreamRateLimited`]
/// (throttling, also breaker-neutral); only a server-class 5xx maps to
/// [`Error::UpstreamError`], which counts against the breaker. The client-facing
/// envelope for 4xx/5xx is identical (502 `upstream_error`) so upstream
/// internals never leak — only the breaker classification differs (see
/// [`Error::counts_against_breaker`]).
pub fn map_upstream_status(status: u16) -> Result<(), Error> {
    match status {
        s if (200..300).contains(&s) => Ok(()),
        429 => Err(Error::UpstreamRateLimited),
        s if (400..500).contains(&s) => Err(Error::UpstreamClientError),
        _ => Err(Error::UpstreamError),
    }
}

/// Map a `reqwest::Error` (transport-level) into the canonical error.
///
/// The connect-vs-sent distinction is load-bearing for the retry policy (plan
/// P3-2): a `connect`/DNS/TLS failure means the request **never left the
/// gateway**, so it maps to the retry-safe [`Error::UpstreamUnavailable`].
/// Anything else (the request was sent, then failed mid-flight) maps to
/// [`Error::UpstreamError`], which is NOT retried by default to avoid
/// double-charging a generation call.
pub fn map_reqwest_error(e: &reqwest::Error) -> Error {
    if e.is_timeout() {
        Error::UpstreamTimeout
    } else if e.is_connect() {
        Error::UpstreamUnavailable
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
