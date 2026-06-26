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
/// The breaker classification is load-bearing (plan P3-2): only a server-class
/// **5xx** maps to the breaker-counting [`Error::UpstreamError`]. A 429 maps to
/// [`Error::UpstreamRateLimited`] (throttling), and *every other* non-2xx status
/// — 4xx client errors and any 1xx/3xx the gateway does not relay (e.g. a
/// redirect from a base-URL mismatch) — maps to the breaker-neutral
/// [`Error::UpstreamClientError`]: the upstream is reachable and answering, so it
/// must not trip the shared per-upstream circuit. The client-facing envelope for
/// 4xx/5xx/redirect is identical (502 `upstream_error`) so upstream internals
/// never leak; only the breaker classification differs (see
/// [`Error::counts_against_breaker`]).
pub fn map_upstream_status(status: u16) -> Result<(), Error> {
    match status {
        s if (200..300).contains(&s) => Ok(()),
        429 => Err(Error::UpstreamRateLimited),
        s if s >= 500 => Err(Error::UpstreamError),
        // 4xx, and any 1xx/3xx we don't relay: upstream is reachable -> neutral.
        _ => Err(Error::UpstreamClientError),
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
///
/// Taps the upstream `usage` object **here, while the bytes are still held** (the
/// caller has already moved them into the relay stream by the time it returns, so
/// parsing in the caller would force a second drain — forbidden by the
/// `streaming-passthrough`/no-double-drain contract). The parse is best-effort
/// and read-only: the body is relayed byte-for-byte verbatim regardless, and a
/// non-JSON or usage-less body simply yields `usage: None`.
pub async fn collect_response(resp: reqwest::Response) -> Result<RawResponse, Error> {
    let status = resp.status().as_u16();
    let content_type = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let bytes = resp.bytes().await.map_err(|e| map_reqwest_error(&e))?;
    // Non-stream usage tap: peek the JSON `usage` object before the bytes move
    // into the relay stream. Read-only; never mutates the relayed body.
    let usage = parse_usage_tokens(&bytes);
    let body: ByteStream = Box::pin(futures_util::stream::once(async move { Ok(bytes) }));
    Ok(RawResponse {
        status,
        content_type,
        body,
        usage,
    })
}

/// Best-effort, read-only extraction of `(prompt_tokens, completion_tokens)` from
/// an OpenAI-shaped response body's `usage` object. Returns `None` when the body
/// is not JSON, has no `usage` object, or lacks both token fields. A
/// genuinely-zero usage (`{"prompt_tokens":0,"completion_tokens":0}`) returns
/// `Some((0,0))` — it is the caller's job (not this helper's) to map zeros.
pub fn parse_usage_tokens(bytes: &Bytes) -> Option<(i64, i64)> {
    let v: serde_json::Value = serde_json::from_slice(bytes).ok()?;
    let usage = v.get("usage")?;
    if !usage.is_object() {
        return None;
    }
    let p = usage.get("prompt_tokens").and_then(|x| x.as_i64());
    let c = usage.get("completion_tokens").and_then(|x| x.as_i64());
    match (p, c) {
        (None, None) => None,
        (p, c) => Some((p.unwrap_or(0), c.unwrap_or(0))),
    }
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
        // Streaming path never taps usage (would require buffering the SSE
        // stream); the gap is made observable by a counter at the app layer.
        usage: None,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn map_upstream_status_classifies_for_breaker() {
        // 2xx is success.
        assert!(map_upstream_status(200).is_ok());
        assert!(map_upstream_status(204).is_ok());
        // 429 -> throttling (breaker-neutral).
        assert!(matches!(
            map_upstream_status(429),
            Err(Error::UpstreamRateLimited)
        ));
        // 4xx -> client error (breaker-neutral).
        for s in [400u16, 401, 403, 404, 422] {
            assert!(
                matches!(map_upstream_status(s), Err(Error::UpstreamClientError)),
                "status {s} should be UpstreamClientError"
            );
        }
        // 3xx redirects we don't relay are also breaker-neutral (upstream is up).
        assert!(matches!(
            map_upstream_status(301),
            Err(Error::UpstreamClientError)
        ));
        assert!(matches!(
            map_upstream_status(308),
            Err(Error::UpstreamClientError)
        ));
        // 5xx -> genuine upstream-health fault (counts against breaker).
        for s in [500u16, 502, 503, 504] {
            assert!(
                matches!(map_upstream_status(s), Err(Error::UpstreamError)),
                "status {s} should be UpstreamError"
            );
        }
        // Breaker classification matches the policy.
        assert!(!Error::UpstreamClientError.counts_against_breaker());
        assert!(!Error::UpstreamRateLimited.counts_against_breaker());
        assert!(Error::UpstreamError.counts_against_breaker());
    }
}
