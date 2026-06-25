//! Unified gateway error model.
//!
//! `moaray-core` owns the canonical `Error`; the HTTP layer maps it into the
//! OpenAI-compatible envelope `{"error":{"message","type","code","param"}}`.
//! Keeping the mapping data here (status, type, code) means the matrix in the
//! plan is asserted in one place and the bin only renders it.

use thiserror::Error;

/// OpenAI-style error `type` discriminants used by clients/SDKs.
pub const TYPE_INVALID_REQUEST: &str = "invalid_request_error";
pub const TYPE_API_ERROR: &str = "api_error";
pub const TYPE_RATE_LIMIT: &str = "rate_limit_error";

/// Canonical error for the whole gateway.
///
/// Variants carry only non-secret, client-safe context. Upstream credentials and
/// inbound tokens must never be embedded here (see no-secret-logging rule).
#[derive(Debug, Error)]
pub enum Error {
    /// Missing or malformed bearer token. -> 401 invalid_api_key
    #[error("missing or invalid API key")]
    InvalidApiKey,

    /// Authenticated, but the model is not in the caller's allowlist. -> 403
    #[error("model `{model}` is not allowed for this key")]
    ModelNotAllowed { model: String },

    /// Model name maps to no configured route. -> 404
    #[error("model `{model}` not found")]
    ModelNotFound { model: String },

    /// Inbound body exceeded the configured limit. -> 413
    #[error("request body too large")]
    PayloadTooLarge,

    /// Inbound JSON could not be parsed. -> 400
    #[error("invalid request: {0}")]
    BadRequest(String),

    /// Upstream did not answer within the deadline. -> 504
    #[error("upstream request timed out")]
    UpstreamTimeout,

    /// Upstream returned a non-429 server-class error status (5xx). This is a
    /// genuine upstream-health fault and counts against the circuit breaker (see
    /// [`Error::counts_against_breaker`]). -> 502
    #[error("upstream returned an error")]
    UpstreamError,

    /// Upstream returned a 4xx client-class status (e.g. 400/401/403/404). The
    /// upstream is reachable and answering — the failure is attributable to the
    /// request itself or to upstream credential/config, NOT to upstream health —
    /// so it is **breaker-neutral** (see [`Error::counts_against_breaker`]). The
    /// client-facing envelope is identical to [`Error::UpstreamError`] (502
    /// `upstream_error`) so provider internals are never leaked. -> 502
    #[error("upstream returned a client error")]
    UpstreamClientError,

    /// Upstream returned 429. The upstream is up but throttling, so this is
    /// breaker-neutral (see [`Error::counts_against_breaker`]). -> 429
    #[error("upstream rate limited")]
    UpstreamRateLimited,

    /// Upstream could not be reached: the connection failed before the request
    /// was sent (DNS / connect / TLS handshake). Client-facing envelope is
    /// identical to [`Error::UpstreamError`] (502), but this variant is the only
    /// **retry-safe** upstream failure — the generation request never left the
    /// gateway, so retrying cannot double-charge or duplicate a side effect (see
    /// [`Error::is_retryable`]). -> 502
    #[error("upstream unavailable")]
    UpstreamUnavailable,

    /// The gateway's own per-key or per-upstream limiter rejected the request,
    /// distinct from [`Error::UpstreamRateLimited`] (the upstream's own 429).
    /// -> 429 rate_limit_error / rate_limited
    #[error("rate limit exceeded")]
    RateLimited,

    /// The per-upstream circuit breaker is open after repeated failures; the
    /// request fails fast instead of hammering a known-bad upstream. -> 503
    #[error("upstream circuit breaker is open")]
    CircuitOpen,

    /// A capability the v1 structured path does not support (e.g. tool_use).
    /// -> 400 invalid_request_error / unsupported
    #[error("unsupported: {0}")]
    Unsupported(String),

    /// MoA mode does not support streaming in v1 (`model: moa/*` + `stream:true`).
    /// -> 400 invalid_request_error / moa_streaming_unsupported
    #[error("MoA mode does not support streaming responses")]
    MoaStreamingUnsupported,

    /// Too few proposer arms succeeded to meet the recipe quorum. -> 503
    #[error("MoA quorum not met: {succeeded}/{required} proposer arms succeeded")]
    MoaQuorumFailed { succeeded: usize, required: usize },

    /// Anything else not safe to attribute to the client. -> 500
    #[error("internal error")]
    Internal,
}

/// Client-facing rendering of an [`Error`]: the exact tuple the HTTP envelope
/// needs. `status` is the numeric HTTP code; `error_type`/`code` are the OpenAI
/// discriminants.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErrorEnvelope {
    pub status: u16,
    pub error_type: &'static str,
    pub code: &'static str,
    pub message: String,
}

impl Error {
    /// Map to the OpenAI-compatible envelope tuple. Single source of truth for
    /// the error-code matrix.
    pub fn envelope(&self) -> ErrorEnvelope {
        let (status, error_type, code) = match self {
            Error::InvalidApiKey => (401, TYPE_INVALID_REQUEST, "invalid_api_key"),
            Error::ModelNotAllowed { .. } => (403, TYPE_INVALID_REQUEST, "model_not_allowed"),
            Error::ModelNotFound { .. } => (404, TYPE_INVALID_REQUEST, "model_not_found"),
            Error::PayloadTooLarge => (413, TYPE_INVALID_REQUEST, "payload_too_large"),
            Error::BadRequest(_) => (400, TYPE_INVALID_REQUEST, "invalid_request"),
            Error::UpstreamTimeout => (504, TYPE_API_ERROR, "upstream_timeout"),
            Error::UpstreamError => (502, TYPE_API_ERROR, "upstream_error"),
            Error::UpstreamClientError => (502, TYPE_API_ERROR, "upstream_error"),
            Error::UpstreamRateLimited => (429, TYPE_RATE_LIMIT, "upstream_rate_limited"),
            Error::UpstreamUnavailable => (502, TYPE_API_ERROR, "upstream_error"),
            Error::RateLimited => (429, TYPE_RATE_LIMIT, "rate_limited"),
            Error::CircuitOpen => (503, TYPE_API_ERROR, "circuit_open"),
            Error::Unsupported(_) => (400, TYPE_INVALID_REQUEST, "unsupported"),
            Error::MoaStreamingUnsupported => {
                (400, TYPE_INVALID_REQUEST, "moa_streaming_unsupported")
            }
            Error::MoaQuorumFailed { .. } => (503, TYPE_API_ERROR, "moa_quorum_failed"),
            Error::Internal => (500, TYPE_API_ERROR, "internal_error"),
        };
        ErrorEnvelope {
            status,
            error_type,
            code,
            message: self.to_string(),
        }
    }

    /// Whether retrying the request that produced this error is safe.
    ///
    /// **Only connection failures that happened before the request was sent are
    /// retry-safe** ([`Error::UpstreamUnavailable`]). A generation request that
    /// already reached the upstream ([`Error::UpstreamError`],
    /// [`Error::UpstreamTimeout`], [`Error::UpstreamRateLimited`]) is NOT
    /// retried by default: the upstream may have produced output and charged for
    /// it, so a retry risks double-billing or a divergent answer (plan P3-2,
    /// codex P1). The retry policy in the bin gates on this; streaming requests
    /// are never retried regardless.
    pub fn is_retryable(&self) -> bool {
        matches!(self, Error::UpstreamUnavailable)
    }

    /// Whether this error should count as an *upstream-health* failure against
    /// the per-upstream circuit breaker.
    ///
    /// Only genuine upstream faults count: a server-class 5xx
    /// ([`Error::UpstreamError`]), a request that timed out
    /// ([`Error::UpstreamTimeout`]), or a connect/transport failure
    /// ([`Error::UpstreamUnavailable`]). These mean the upstream is unhealthy and
    /// hammering it should stop.
    ///
    /// Breaker-**neutral** outcomes (return `false`):
    /// - [`Error::UpstreamClientError`] — a 4xx means the upstream is up and
    ///   answering; the fault is the request's or the upstream credential/config.
    ///   Counting it would let one caller's malformed requests, or a single
    ///   misconfigured key (persistent 401/403), trip the *shared* per-upstream
    ///   breaker and fail-fast every other caller/model on that upstream while
    ///   masking the real cause.
    /// - [`Error::UpstreamRateLimited`] — a 429 means the upstream is up but
    ///   throttling; opening the breaker on throttling would amplify, not relieve.
    /// - the gateway's own errors (rate limit, circuit open, bad request, …),
    ///   which never reflect upstream health.
    pub fn counts_against_breaker(&self) -> bool {
        matches!(
            self,
            Error::UpstreamError | Error::UpstreamTimeout | Error::UpstreamUnavailable
        )
    }
}

/// Convenience result alias.
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_matrix_maps_to_spec() {
        let cases = [
            (Error::InvalidApiKey, 401, "invalid_api_key"),
            (
                Error::ModelNotAllowed { model: "x".into() },
                403,
                "model_not_allowed",
            ),
            (
                Error::ModelNotFound { model: "x".into() },
                404,
                "model_not_found",
            ),
            (Error::PayloadTooLarge, 413, "payload_too_large"),
            (Error::UpstreamTimeout, 504, "upstream_timeout"),
            (Error::UpstreamError, 502, "upstream_error"),
            (Error::UpstreamClientError, 502, "upstream_error"),
            (Error::UpstreamRateLimited, 429, "upstream_rate_limited"),
            (Error::UpstreamUnavailable, 502, "upstream_error"),
            (Error::RateLimited, 429, "rate_limited"),
            (Error::CircuitOpen, 503, "circuit_open"),
            (
                Error::MoaStreamingUnsupported,
                400,
                "moa_streaming_unsupported",
            ),
            (
                Error::MoaQuorumFailed {
                    succeeded: 1,
                    required: 3,
                },
                503,
                "moa_quorum_failed",
            ),
        ];
        for (err, status, code) in cases {
            let env = err.envelope();
            assert_eq!(env.status, status, "status for {err:?}");
            assert_eq!(env.code, code, "code for {err:?}");
        }
    }

    #[test]
    fn rate_limit_uses_rate_limit_type() {
        assert_eq!(
            Error::UpstreamRateLimited.envelope().error_type,
            TYPE_RATE_LIMIT
        );
        assert_eq!(Error::RateLimited.envelope().error_type, TYPE_RATE_LIMIT);
    }

    #[test]
    fn only_connect_failures_are_retryable() {
        // Retry-safe: the request never left the gateway.
        assert!(Error::UpstreamUnavailable.is_retryable());
        // NOT retry-safe: the generation request may have reached the upstream
        // and produced (billed) output — retrying risks double-charge.
        assert!(!Error::UpstreamError.is_retryable());
        assert!(!Error::UpstreamTimeout.is_retryable());
        assert!(!Error::UpstreamRateLimited.is_retryable());
        assert!(!Error::UpstreamClientError.is_retryable());
        assert!(!Error::CircuitOpen.is_retryable());
        assert!(!Error::Internal.is_retryable());
    }

    #[test]
    fn only_upstream_health_faults_count_against_breaker() {
        // Genuine upstream-health faults trip the breaker.
        assert!(Error::UpstreamError.counts_against_breaker());
        assert!(Error::UpstreamTimeout.counts_against_breaker());
        assert!(Error::UpstreamUnavailable.counts_against_breaker());
        // Breaker-neutral: upstream is up and answering / throttling.
        assert!(!Error::UpstreamClientError.counts_against_breaker());
        assert!(!Error::UpstreamRateLimited.counts_against_breaker());
        // Gateway-own errors never reflect upstream health.
        assert!(!Error::RateLimited.counts_against_breaker());
        assert!(!Error::CircuitOpen.counts_against_breaker());
        assert!(!Error::BadRequest("x".into()).counts_against_breaker());
        assert!(!Error::Internal.counts_against_breaker());
    }
}
