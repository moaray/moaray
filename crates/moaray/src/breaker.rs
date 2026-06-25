//! Per-upstream circuit breaker (closed -> open -> half-open -> closed).
//!
//! State lives in the reload-surviving [`crate::runtime::StatefulState`] keyed by
//! `upstream_id`, so a config reload that does not change an upstream keeps its
//! breaker exactly where it was (plan §1.3 state-preserving foundation). The
//! breaker is shared by passthrough and MoA arms hitting the same upstream.
//!
//! Transitions:
//! - **Closed**: requests flow. `failure_threshold` consecutive failures -> Open.
//! - **Open**: requests fail fast with [`Error::CircuitOpen`] until `open_ms`
//!   elapses, then the next request is allowed as a half-open probe.
//! - **HalfOpen**: a probe is in flight. `half_open_successes` consecutive
//!   successes -> Closed; any failure -> Open again.
//!
//! Only an *upstream* failure (5xx/timeout/connect) counts against the breaker;
//! client errors (4xx, e.g. a bad request) are not the upstream's fault and are
//! reported as success to the breaker by the caller.

use std::sync::Mutex;
use std::time::{Duration, Instant};

use moaray_config::BreakerConfig;
use moaray_core::error::{Error, Result};

/// Public breaker state label — low-cardinality, safe as a metric/log value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BreakerState {
    Closed,
    Open,
    HalfOpen,
}

impl BreakerState {
    pub fn as_str(&self) -> &'static str {
        match self {
            BreakerState::Closed => "closed",
            BreakerState::Open => "open",
            BreakerState::HalfOpen => "half_open",
        }
    }
}

struct Inner {
    state: BreakerState,
    consecutive_failures: u32,
    consecutive_successes: u32,
    /// When the breaker opened; used to time the half-open probe window.
    opened_at: Option<Instant>,
}

/// A per-upstream circuit breaker.
pub struct CircuitBreaker {
    cfg: BreakerConfig,
    inner: Mutex<Inner>,
}

impl CircuitBreaker {
    /// Build a breaker from validated thresholds, starting closed.
    pub fn new(cfg: BreakerConfig) -> Self {
        Self {
            cfg,
            inner: Mutex::new(Inner {
                state: BreakerState::Closed,
                consecutive_failures: 0,
                consecutive_successes: 0,
                opened_at: None,
            }),
        }
    }

    /// Gate a request. `Ok(())` to proceed (closed, or a half-open probe);
    /// `Err(CircuitOpen)` to fail fast while open. Uses `now` for testability.
    pub fn check_at(&self, now: Instant) -> Result<()> {
        let mut g = self.inner.lock().expect("breaker mutex");
        match g.state {
            BreakerState::Closed | BreakerState::HalfOpen => Ok(()),
            BreakerState::Open => {
                let open_for = g.opened_at.map(|t| now.duration_since(t));
                if matches!(open_for, Some(d) if d >= Duration::from_millis(self.cfg.open_ms)) {
                    // Cooldown elapsed: allow a single half-open probe.
                    g.state = BreakerState::HalfOpen;
                    g.consecutive_successes = 0;
                    Ok(())
                } else {
                    Err(Error::CircuitOpen)
                }
            }
        }
    }

    /// Convenience wrapper over [`Self::check_at`] using the real clock.
    pub fn check(&self) -> Result<()> {
        self.check_at(Instant::now())
    }

    /// Record an upstream success, advancing recovery.
    pub fn on_success_at(&self, now: Instant) {
        let _ = now;
        let mut g = self.inner.lock().expect("breaker mutex");
        match g.state {
            BreakerState::Closed => {
                g.consecutive_failures = 0;
            }
            BreakerState::HalfOpen => {
                g.consecutive_successes += 1;
                if g.consecutive_successes >= self.cfg.half_open_successes {
                    g.state = BreakerState::Closed;
                    g.consecutive_failures = 0;
                    g.consecutive_successes = 0;
                    g.opened_at = None;
                }
            }
            BreakerState::Open => { /* a stray late success while open: ignore */ }
        }
    }

    /// Record an upstream failure, possibly tripping the breaker.
    pub fn on_failure_at(&self, now: Instant) {
        let mut g = self.inner.lock().expect("breaker mutex");
        match g.state {
            BreakerState::Closed => {
                g.consecutive_failures += 1;
                if g.consecutive_failures >= self.cfg.failure_threshold {
                    g.state = BreakerState::Open;
                    g.opened_at = Some(now);
                }
            }
            BreakerState::HalfOpen => {
                // Probe failed: re-open immediately.
                g.state = BreakerState::Open;
                g.opened_at = Some(now);
                g.consecutive_successes = 0;
            }
            BreakerState::Open => {
                g.opened_at = Some(now);
            }
        }
    }

    pub fn on_success(&self) {
        self.on_success_at(Instant::now());
    }

    pub fn on_failure(&self) {
        self.on_failure_at(Instant::now());
    }

    /// Current state (for metrics/tests).
    pub fn state(&self) -> BreakerState {
        self.inner.lock().expect("breaker mutex").state
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> BreakerConfig {
        BreakerConfig {
            failure_threshold: 3,
            open_ms: 1000,
            half_open_successes: 2,
        }
    }

    #[test]
    fn opens_after_threshold_failures() {
        let b = CircuitBreaker::new(cfg());
        let t = Instant::now();
        assert!(b.check_at(t).is_ok());
        b.on_failure_at(t);
        b.on_failure_at(t);
        assert_eq!(b.state(), BreakerState::Closed);
        b.on_failure_at(t);
        assert_eq!(b.state(), BreakerState::Open);
        // fail fast while open
        assert!(matches!(b.check_at(t), Err(Error::CircuitOpen)));
    }

    #[test]
    fn half_open_probe_recovers_to_closed() {
        let b = CircuitBreaker::new(cfg());
        let t0 = Instant::now();
        for _ in 0..3 {
            b.on_failure_at(t0);
        }
        assert_eq!(b.state(), BreakerState::Open);
        // before cooldown: still open
        let before = t0 + Duration::from_millis(500);
        assert!(matches!(b.check_at(before), Err(Error::CircuitOpen)));
        // after cooldown: a probe is allowed -> half-open
        let after = t0 + Duration::from_millis(1001);
        assert!(b.check_at(after).is_ok());
        assert_eq!(b.state(), BreakerState::HalfOpen);
        // need 2 consecutive successes to close
        b.on_success_at(after);
        assert_eq!(b.state(), BreakerState::HalfOpen);
        b.on_success_at(after);
        assert_eq!(b.state(), BreakerState::Closed);
    }

    #[test]
    fn half_open_failure_reopens() {
        let b = CircuitBreaker::new(cfg());
        let t0 = Instant::now();
        for _ in 0..3 {
            b.on_failure_at(t0);
        }
        let after = t0 + Duration::from_millis(1001);
        assert!(b.check_at(after).is_ok());
        assert_eq!(b.state(), BreakerState::HalfOpen);
        b.on_failure_at(after);
        assert_eq!(b.state(), BreakerState::Open);
        // still fails fast immediately after re-opening
        assert!(matches!(b.check_at(after), Err(Error::CircuitOpen)));
    }

    #[test]
    fn success_resets_failure_streak_while_closed() {
        let b = CircuitBreaker::new(cfg());
        let t = Instant::now();
        b.on_failure_at(t);
        b.on_failure_at(t);
        b.on_success_at(t); // resets streak
        b.on_failure_at(t);
        b.on_failure_at(t);
        assert_eq!(b.state(), BreakerState::Closed); // only 2 since reset
    }
}
