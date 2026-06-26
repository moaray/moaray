//! Token-bucket rate limiting + concurrency caps.
//!
//! Two limiter scopes, both built from validated config and both living in the
//! reload-surviving [`crate::runtime::StatefulState`] (keyed by a stable id so a
//! future config reload can preserve in-flight bucket state):
//!
//! - **per-key** (inbound): keyed by `caller_key_id`, checked in the handler.
//! - **per-upstream**: keyed by `upstream_id`, enforced inside
//!   [`crate::governed::GovernedProvider`] so passthrough and MoA arms that
//!   resolve to the same upstream share *one* bucket (plan §1.4) and MoA fan-out
//!   cannot amplify traffic past the cap.
//!
//! Over-limit is a fast, allocation-free `check()` returning
//! [`moaray_core::Error::RateLimited`] (429). Concurrency uses a Tokio
//! [`Semaphore`]; a held permit is released on drop, including on cancellation.

use std::num::NonZeroU32;
use std::sync::Arc;

use governor::clock::DefaultClock;
use governor::state::{InMemoryState, NotKeyed};
use governor::{Quota, RateLimiter as GovRateLimiter};
use moaray_config::RateLimit;
use moaray_core::error::{Error, Result};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// A direct (single-bucket) governor limiter.
type Direct = GovRateLimiter<NotKeyed, InMemoryState, DefaultClock>;

/// One token-bucket limiter wrapping a validated [`RateLimit`].
pub struct TokenBucket {
    inner: Direct,
    /// The validated limit this bucket was built from. Kept so a config reload can
    /// detect whether a per-key / per-upstream limit *value* changed (rebuild) or
    /// is byte-identical (preserve the live bucket).
    limit: RateLimit,
}

impl TokenBucket {
    /// Build from a validated [`RateLimit`] (rps >= 1, burst >= rps guaranteed
    /// by config validation).
    pub fn new(limit: RateLimit) -> Self {
        let rps = NonZeroU32::new(limit.rps.max(1)).expect("rps >= 1");
        let burst = NonZeroU32::new(limit.burst.max(limit.rps).max(1)).expect("burst >= 1");
        let quota = Quota::per_second(rps).allow_burst(burst);
        Self {
            inner: GovRateLimiter::direct(quota),
            limit,
        }
    }

    /// The validated limit this bucket enforces (for reload diffing).
    pub fn limit(&self) -> RateLimit {
        self.limit
    }

    /// Try to consume one token. `Err(RateLimited)` when the bucket is empty.
    pub fn check(&self) -> Result<()> {
        match self.inner.check() {
            Ok(()) => Ok(()),
            Err(_) => Err(Error::RateLimited),
        }
    }
}

/// Per-upstream concurrency cap backed by a Tokio semaphore.
///
/// `None` (unconfigured) means unbounded; `acquire` then returns `None` and the
/// call proceeds without a permit.
pub struct Concurrency {
    sem: Option<Arc<Semaphore>>,
}

impl Concurrency {
    /// Build a cap of `max` in-flight, or unbounded when `max` is `None`/0.
    pub fn new(max: Option<u32>) -> Self {
        let sem = match max {
            Some(n) if n > 0 => Some(Arc::new(Semaphore::new(n as usize))),
            _ => None,
        };
        Self { sem }
    }

    /// Acquire a permit if a cap is configured. The returned guard releases the
    /// permit on drop (including cancellation/timeout). `Ok(None)` = unbounded.
    pub async fn acquire(&self) -> Result<Option<OwnedSemaphorePermit>> {
        match &self.sem {
            None => Ok(None),
            Some(sem) => sem
                .clone()
                .acquire_owned()
                .await
                .map(Some)
                .map_err(|_| Error::Internal),
        }
    }

    /// Currently available permits (for tests/metrics). `None` = unbounded.
    pub fn available(&self) -> Option<usize> {
        self.sem.as_ref().map(|s| s.available_permits())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucket_allows_burst_then_rejects() {
        let b = TokenBucket::new(RateLimit { rps: 1, burst: 2 });
        // burst of 2 -> first two pass, third rejected.
        assert!(b.check().is_ok());
        assert!(b.check().is_ok());
        assert!(matches!(b.check(), Err(Error::RateLimited)));
    }

    #[tokio::test]
    async fn concurrency_caps_in_flight() {
        let c = Concurrency::new(Some(1));
        assert_eq!(c.available(), Some(1));
        let p = c.acquire().await.unwrap();
        assert!(p.is_some());
        assert_eq!(c.available(), Some(0));
        drop(p);
        assert_eq!(c.available(), Some(1));
    }

    #[tokio::test]
    async fn unbounded_concurrency_returns_no_permit() {
        let c = Concurrency::new(None);
        assert_eq!(c.available(), None);
        assert!(c.acquire().await.unwrap().is_none());
    }
}
