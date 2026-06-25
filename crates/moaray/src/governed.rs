//! `GovernedProvider` — the per-upstream protection decorator.
//!
//! Wraps any `Arc<dyn Provider>` and enforces, in order:
//! 1. **circuit breaker** (fail fast with 503 `circuit_open` when the upstream
//!    is known-bad),
//! 2. **per-upstream token bucket** (429 `rate_limited` when over rate),
//! 3. **per-upstream concurrency cap** (a semaphore permit held for the call),
//! 4. **conservative retry** (only `UpstreamUnavailable` connect failures, only
//!    when explicitly enabled, never for streaming — plan P3-2).
//!
//! Because the bin wraps every concrete provider in this decorator and the MoA
//! orchestrator shares the *same* `Arc<dyn Provider>` instances as passthrough,
//! a MoA arm and a passthrough call to the same `upstream_id` go through the
//! **same** breaker/bucket/semaphore. MoA fan-out therefore cannot amplify
//! traffic past the per-upstream cap (acceptance: MoA traffic also trips the
//! upstream's 429).

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::StreamExt;
use moaray_config::RetryConfig;
use moaray_core::error::Result;
use moaray_core::provider::{ByteStream, Provider, RawResponse, ReqCtx};
use moaray_core::types::{ChatRequest, ChatResponse};
use tokio::sync::OwnedSemaphorePermit;

use crate::runtime::UpstreamState;

/// A provider wrapped with per-upstream breaker + limiter + concurrency + retry.
pub struct GovernedProvider {
    inner: Arc<dyn Provider>,
    state: Arc<UpstreamState>,
    retry: RetryConfig,
}

impl GovernedProvider {
    /// Wrap `inner` with the shared per-upstream `state` and the retry policy.
    pub fn new(inner: Arc<dyn Provider>, state: Arc<UpstreamState>, retry: RetryConfig) -> Self {
        Self {
            inner,
            state,
            retry,
        }
    }

    /// Run the breaker + limiter admission checks. Returns a held concurrency
    /// permit (or `None` when unbounded) once admitted.
    async fn admit(&self) -> Result<Option<OwnedSemaphorePermit>> {
        // 1. circuit breaker — fail fast if open.
        self.state.breaker.check()?;
        // 2. per-upstream token bucket.
        self.state.check_limit()?;
        // 3. concurrency cap (permit released on drop / cancellation).
        self.state.concurrency.acquire().await
    }

    /// Record an outcome against the breaker. Every provider error here is an
    /// upstream/transport failure (the gateway's own 4xx never reaches a
    /// provider), so any `Err` counts against the breaker.
    fn record(&self, ok: bool) {
        if ok {
            self.state.breaker.on_success();
        } else {
            self.state.breaker.on_failure();
        }
    }

    /// Number of additional attempts permitted for a retry-safe, non-stream call.
    fn max_retries(&self) -> u32 {
        if self.retry.enabled {
            self.retry.max_retries
        } else {
            0
        }
    }
}

#[async_trait]
impl Provider for GovernedProvider {
    fn upstream_id(&self) -> &str {
        self.inner.upstream_id()
    }

    async fn passthrough(&self, ctx: &ReqCtx, raw_body: Bytes) -> Result<RawResponse> {
        let _permit = self.admit().await?;
        let mut attempt = 0;
        loop {
            let result = self.inner.passthrough(ctx, raw_body.clone()).await;
            match result {
                Ok(resp) => {
                    self.record(true);
                    return Ok(resp);
                }
                Err(e) => {
                    self.record(false);
                    // Retry ONLY connect failures (request never sent), only when
                    // enabled, and never for streaming (this is the non-stream path).
                    if attempt < self.max_retries() && e.is_retryable() {
                        attempt += 1;
                        let backoff = self.retry.backoff_ms.saturating_mul(1u64 << (attempt - 1));
                        tokio::time::sleep(Duration::from_millis(backoff)).await;
                        continue;
                    }
                    return Err(e);
                }
            }
        }
    }

    async fn passthrough_stream(&self, ctx: &ReqCtx, raw_body: Bytes) -> Result<RawResponse> {
        let permit = self.admit().await?;
        // Streaming is NEVER retried (a partially-streamed generation cannot be
        // safely replayed — plan P3-2).
        match self.inner.passthrough_stream(ctx, raw_body).await {
            Ok(resp) => {
                self.record(true);
                // Hold the concurrency permit for the lifetime of the stream so
                // the cap reflects truly in-flight upstream work, not just the
                // connect handshake.
                Ok(attach_permit(resp, permit))
            }
            Err(e) => {
                self.record(false);
                Err(e)
            }
        }
    }

    async fn chat(&self, ctx: &ReqCtx, req: ChatRequest) -> Result<ChatResponse> {
        let _permit = self.admit().await?;
        let mut attempt = 0;
        loop {
            match self.inner.chat(ctx, req.clone()).await {
                Ok(resp) => {
                    self.record(true);
                    return Ok(resp);
                }
                Err(e) => {
                    self.record(false);
                    if attempt < self.max_retries() && e.is_retryable() {
                        attempt += 1;
                        let backoff = self.retry.backoff_ms.saturating_mul(1u64 << (attempt - 1));
                        tokio::time::sleep(Duration::from_millis(backoff)).await;
                        continue;
                    }
                    return Err(e);
                }
            }
        }
    }
}

/// Move a held semaphore permit into the response body stream so it is released
/// only when the stream is fully consumed (or dropped on client disconnect).
fn attach_permit(mut resp: RawResponse, permit: Option<OwnedSemaphorePermit>) -> RawResponse {
    let Some(permit) = permit else {
        return resp; // unbounded: nothing to hold
    };
    let body = std::mem::replace(&mut resp.body, empty_stream());
    // Keep `permit` alive for as long as the stream yields; `scan` threads the
    // permit through and drops it when the stream ends.
    let guarded = body.scan(Some(permit), |permit_slot, item| {
        // hold the permit; only drop it when the stream terminates.
        let _ = &permit_slot;
        std::future::ready(Some(item))
    });
    resp.body = Box::pin(guarded);
    resp
}

fn empty_stream() -> ByteStream {
    Box::pin(futures_util::stream::empty::<Result<Bytes>>())
}

#[cfg(test)]
mod tests {
    use super::*;
    use moaray_config::{BreakerConfig, RateLimit};
    use moaray_core::error::Error;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Instant;

    fn ctx() -> ReqCtx {
        ReqCtx {
            request_id: "rid".into(),
            deadline: Instant::now() + Duration::from_secs(5),
            caller_key_id: "k".into(),
            model: "m".into(),
        }
    }

    fn breaker_cfg() -> BreakerConfig {
        BreakerConfig {
            failure_threshold: 100,
            open_ms: 1000,
            half_open_successes: 1,
        }
    }

    fn retry_on(max: u32) -> RetryConfig {
        RetryConfig {
            enabled: true,
            max_retries: max,
            backoff_ms: 0,
        }
    }
    fn retry_off() -> RetryConfig {
        RetryConfig {
            enabled: false,
            max_retries: 5,
            backoff_ms: 0,
        }
    }

    /// A mock provider counting calls and returning a scripted error/ok.
    struct CountingProvider {
        calls: AtomicUsize,
        mode: Mode,
    }
    enum Mode {
        AlwaysOk,
        AlwaysErr(fn() -> Error),
    }

    #[async_trait]
    impl Provider for CountingProvider {
        fn upstream_id(&self) -> &str {
            "up"
        }
        async fn passthrough(&self, _c: &ReqCtx, _b: Bytes) -> Result<RawResponse> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            match self.mode {
                Mode::AlwaysOk => Ok(RawResponse {
                    status: 200,
                    content_type: None,
                    body: empty_stream(),
                }),
                Mode::AlwaysErr(f) => Err(f()),
            }
        }
        async fn passthrough_stream(&self, _c: &ReqCtx, _b: Bytes) -> Result<RawResponse> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            match self.mode {
                Mode::AlwaysOk => Ok(RawResponse {
                    status: 200,
                    content_type: Some("text/event-stream".into()),
                    body: empty_stream(),
                }),
                Mode::AlwaysErr(f) => Err(f()),
            }
        }
        async fn chat(&self, _c: &ReqCtx, _r: ChatRequest) -> Result<ChatResponse> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            match self.mode {
                Mode::AlwaysOk => Err(Error::Internal),
                Mode::AlwaysErr(f) => Err(f()),
            }
        }
    }

    fn state() -> Arc<UpstreamState> {
        Arc::new(UpstreamState {
            limiter: None,
            concurrency: crate::limit::Concurrency::new(None),
            breaker: crate::breaker::CircuitBreaker::new(breaker_cfg()),
        })
    }

    #[tokio::test]
    async fn generation_error_is_not_retried() {
        // A *sent* generation request that errors must hit the upstream once.
        let inner = Arc::new(CountingProvider {
            calls: AtomicUsize::new(0),
            mode: Mode::AlwaysErr(|| Error::UpstreamError),
        });
        let g = GovernedProvider::new(inner.clone(), state(), retry_on(3));
        let _ = g.passthrough(&ctx(), Bytes::new()).await;
        assert_eq!(
            inner.calls.load(Ordering::SeqCst),
            1,
            "must not retry a sent request"
        );
    }

    #[tokio::test]
    async fn connect_failure_is_retried_when_enabled() {
        let inner = Arc::new(CountingProvider {
            calls: AtomicUsize::new(0),
            mode: Mode::AlwaysErr(|| Error::UpstreamUnavailable),
        });
        let g = GovernedProvider::new(inner.clone(), state(), retry_on(2));
        let _ = g.passthrough(&ctx(), Bytes::new()).await;
        // 1 initial + 2 retries = 3 attempts.
        assert_eq!(inner.calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn connect_failure_not_retried_when_disabled() {
        let inner = Arc::new(CountingProvider {
            calls: AtomicUsize::new(0),
            mode: Mode::AlwaysErr(|| Error::UpstreamUnavailable),
        });
        let g = GovernedProvider::new(inner.clone(), state(), retry_off());
        let _ = g.passthrough(&ctx(), Bytes::new()).await;
        assert_eq!(inner.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn streaming_never_retried_even_on_connect_failure() {
        let inner = Arc::new(CountingProvider {
            calls: AtomicUsize::new(0),
            mode: Mode::AlwaysErr(|| Error::UpstreamUnavailable),
        });
        let g = GovernedProvider::new(inner.clone(), state(), retry_on(5));
        let _ = g.passthrough_stream(&ctx(), Bytes::new()).await;
        assert_eq!(
            inner.calls.load(Ordering::SeqCst),
            1,
            "streaming is never retried"
        );
    }

    #[tokio::test]
    async fn rate_limit_blocks_before_calling_upstream() {
        let inner = Arc::new(CountingProvider {
            calls: AtomicUsize::new(0),
            mode: Mode::AlwaysOk,
        });
        let st = Arc::new(UpstreamState {
            limiter: Some(crate::limit::TokenBucket::new(RateLimit {
                rps: 1,
                burst: 1,
            })),
            concurrency: crate::limit::Concurrency::new(None),
            breaker: crate::breaker::CircuitBreaker::new(breaker_cfg()),
        });
        let g = GovernedProvider::new(inner.clone(), st, retry_off());
        assert!(g.passthrough(&ctx(), Bytes::new()).await.is_ok());
        // second call over the bucket -> rate limited, upstream not called again.
        assert!(matches!(
            g.passthrough(&ctx(), Bytes::new()).await,
            Err(Error::RateLimited)
        ));
        assert_eq!(inner.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn open_breaker_fails_fast_without_calling_upstream() {
        let inner = Arc::new(CountingProvider {
            calls: AtomicUsize::new(0),
            mode: Mode::AlwaysOk,
        });
        let st = Arc::new(UpstreamState {
            limiter: None,
            concurrency: crate::limit::Concurrency::new(None),
            breaker: crate::breaker::CircuitBreaker::new(BreakerConfig {
                failure_threshold: 1,
                open_ms: 60_000,
                half_open_successes: 1,
            }),
        });
        // Trip the breaker.
        st.breaker.on_failure();
        let g = GovernedProvider::new(inner.clone(), st, retry_off());
        assert!(matches!(
            g.passthrough(&ctx(), Bytes::new()).await,
            Err(Error::CircuitOpen)
        ));
        assert_eq!(inner.calls.load(Ordering::SeqCst), 0);
    }
}
