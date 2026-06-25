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
//! - **HalfOpen**: a *single* probe is allowed in flight at a time; concurrent
//!   requests while a probe is outstanding fail fast with [`Error::CircuitOpen`]
//!   so a recovering (possibly still-bad) upstream is not stampeded.
//!   `half_open_successes` consecutive probe successes -> Closed; any probe
//!   failure -> Open again. Each admission is tagged ([`Admission::Normal`] vs
//!   [`Admission::Probe`]) and the outcome must be reported with the same token,
//!   so a call admitted while Closed that completes *after* the breaker has gone
//!   half-open cannot be mistaken for the probe (no recovery-logic race).
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

/// Which kind of admission a call was granted by [`CircuitBreaker::check_at`].
///
/// A call MUST report its outcome ([`CircuitBreaker::on_success_at`] /
/// [`CircuitBreaker::on_failure_at`]) carrying the **same** kind it was admitted
/// under. This is the fix for the half-open probe race: without a token, a call
/// admitted while `Closed` that finishes *after* the breaker has since
/// transitioned to half-open would be misread by the half-open arm as the probe
/// — closing the breaker (or freeing the single probe slot so a second probe
/// slips in while the real one is still outstanding) even though the recovering,
/// possibly-still-bad upstream had only been tested by the actual probe. Tagging
/// the admission keeps a stale `Normal` completion from ever touching half-open
/// recovery bookkeeping.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admission {
    /// Admitted while `Closed` (a non-probe call). Its outcome only advances the
    /// Closed-state failure streak; it is a **no-op** on half-open recovery, so a
    /// completion straddling the open→half-open transition cannot corrupt the
    /// single-probe state machine.
    Normal,
    /// Admitted as the single half-open probe. Its outcome drives recovery (close
    /// after enough consecutive successes, re-open on failure) and frees the
    /// probe slot.
    Probe,
}

struct Inner {
    state: BreakerState,
    consecutive_failures: u32,
    consecutive_successes: u32,
    /// When the breaker opened; used to time the half-open probe window.
    opened_at: Option<Instant>,
    /// Whether a half-open probe is currently outstanding. While `true`, further
    /// half-open admissions fail fast so only one probe at a time tests a
    /// recovering upstream (no thundering herd). Cleared when the probe records
    /// its outcome via [`CircuitBreaker::on_success_at`] /
    /// [`CircuitBreaker::on_failure_at`].
    probe_in_flight: bool,
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
                probe_in_flight: false,
            }),
        }
    }

    /// Gate a request. `Ok(Admission)` to proceed, tagging whether the call was
    /// admitted as a normal (Closed) call or the single half-open probe;
    /// `Err(CircuitOpen)` to fail fast while open. Uses `now` for testability.
    ///
    /// The returned [`Admission`] MUST be threaded back into
    /// [`Self::on_success_at`] / [`Self::on_failure_at`] so a completion can be
    /// attributed to the admission it was granted under — only a `Probe` may
    /// advance or clear half-open recovery state.
    pub fn check_at(&self, now: Instant) -> Result<Admission> {
        let mut g = self.inner.lock().expect("breaker mutex");
        match g.state {
            BreakerState::Closed => Ok(Admission::Normal),
            BreakerState::HalfOpen => {
                // Only one probe at a time may test a recovering upstream.
                if g.probe_in_flight {
                    Err(Error::CircuitOpen)
                } else {
                    g.probe_in_flight = true;
                    Ok(Admission::Probe)
                }
            }
            BreakerState::Open => {
                let open_for = g.opened_at.map(|t| now.duration_since(t));
                if matches!(open_for, Some(d) if d >= Duration::from_millis(self.cfg.open_ms)) {
                    // Cooldown elapsed: admit exactly one half-open probe and mark
                    // it in flight so concurrent callers still fail fast.
                    g.state = BreakerState::HalfOpen;
                    g.consecutive_successes = 0;
                    g.probe_in_flight = true;
                    Ok(Admission::Probe)
                } else {
                    Err(Error::CircuitOpen)
                }
            }
        }
    }

    /// Convenience wrapper over [`Self::check_at`] using the real clock.
    pub fn check(&self) -> Result<Admission> {
        self.check_at(Instant::now())
    }

    /// Record an outcome for a call admitted as `admission`, advancing recovery.
    ///
    /// `admission` is the token [`Self::check_at`] returned for this call. Only a
    /// [`Admission::Probe`] may touch half-open recovery; a stale
    /// [`Admission::Normal`] completing while half-open is a no-op on the probe
    /// machinery (it cannot close the breaker or free the probe slot).
    pub fn on_success_at(&self, now: Instant, admission: Admission) {
        let _ = now;
        let mut g = self.inner.lock().expect("breaker mutex");
        match g.state {
            BreakerState::Closed => {
                g.consecutive_failures = 0;
            }
            BreakerState::HalfOpen => {
                // Only the real probe advances recovery. A Normal call admitted
                // back in Closed that happens to finish now must NOT be mistaken
                // for the probe (that was the race).
                if admission == Admission::Probe {
                    g.probe_in_flight = false;
                    g.consecutive_successes += 1;
                    if g.consecutive_successes >= self.cfg.half_open_successes {
                        g.state = BreakerState::Closed;
                        g.consecutive_failures = 0;
                        g.consecutive_successes = 0;
                        g.opened_at = None;
                    }
                }
            }
            BreakerState::Open => { /* a stray late success while open: ignore */ }
        }
    }

    /// Record a failure for a call admitted as `admission`, possibly tripping or
    /// re-opening the breaker. See [`Self::on_success_at`] for why the admission
    /// token matters.
    pub fn on_failure_at(&self, now: Instant, admission: Admission) {
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
                // Only the real probe failing re-opens the breaker and frees the
                // slot. A stale Normal failure while half-open is ignored so it
                // cannot discard the in-flight probe's slot.
                if admission == Admission::Probe {
                    g.state = BreakerState::Open;
                    g.opened_at = Some(now);
                    g.consecutive_successes = 0;
                    g.probe_in_flight = false;
                }
            }
            BreakerState::Open => {
                g.opened_at = Some(now);
            }
        }
    }

    pub fn on_success(&self, admission: Admission) {
        self.on_success_at(Instant::now(), admission);
    }

    pub fn on_failure(&self, admission: Admission) {
        self.on_failure_at(Instant::now(), admission);
    }

    /// Release a half-open probe slot reserved by [`Self::check_at`] without
    /// recording an outcome. Used when a *later* admission gate (rate limit /
    /// concurrency) rejects the request after the breaker already admitted it, so
    /// the reserved probe never actually reaches the upstream. Without this the
    /// `probe_in_flight` flag would stay set forever and the breaker could never
    /// probe again. No-op outside HalfOpen / when no probe is outstanding.
    pub fn release_probe(&self) {
        let mut g = self.inner.lock().expect("breaker mutex");
        if g.state == BreakerState::HalfOpen {
            g.probe_in_flight = false;
        }
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
        assert_eq!(b.check_at(t).unwrap(), Admission::Normal);
        b.on_failure_at(t, Admission::Normal);
        b.on_failure_at(t, Admission::Normal);
        assert_eq!(b.state(), BreakerState::Closed);
        b.on_failure_at(t, Admission::Normal);
        assert_eq!(b.state(), BreakerState::Open);
        // fail fast while open
        assert!(matches!(b.check_at(t), Err(Error::CircuitOpen)));
    }

    #[test]
    fn half_open_probe_recovers_to_closed() {
        let b = CircuitBreaker::new(cfg());
        let t0 = Instant::now();
        for _ in 0..3 {
            b.on_failure_at(t0, Admission::Normal);
        }
        assert_eq!(b.state(), BreakerState::Open);
        // before cooldown: still open
        let before = t0 + Duration::from_millis(500);
        assert!(matches!(b.check_at(before), Err(Error::CircuitOpen)));
        // after cooldown: a probe is allowed -> half-open
        let after = t0 + Duration::from_millis(1001);
        assert_eq!(b.check_at(after).unwrap(), Admission::Probe);
        assert_eq!(b.state(), BreakerState::HalfOpen);
        // need 2 consecutive successes to close
        b.on_success_at(after, Admission::Probe);
        assert_eq!(b.state(), BreakerState::HalfOpen);
        // second probe admitted + succeeds
        assert_eq!(b.check_at(after).unwrap(), Admission::Probe);
        b.on_success_at(after, Admission::Probe);
        assert_eq!(b.state(), BreakerState::Closed);
    }

    #[test]
    fn half_open_failure_reopens() {
        let b = CircuitBreaker::new(cfg());
        let t0 = Instant::now();
        for _ in 0..3 {
            b.on_failure_at(t0, Admission::Normal);
        }
        let after = t0 + Duration::from_millis(1001);
        assert_eq!(b.check_at(after).unwrap(), Admission::Probe);
        assert_eq!(b.state(), BreakerState::HalfOpen);
        b.on_failure_at(after, Admission::Probe);
        assert_eq!(b.state(), BreakerState::Open);
        // still fails fast immediately after re-opening
        assert!(matches!(b.check_at(after), Err(Error::CircuitOpen)));
    }

    #[test]
    fn success_resets_failure_streak_while_closed() {
        let b = CircuitBreaker::new(cfg());
        let t = Instant::now();
        b.on_failure_at(t, Admission::Normal);
        b.on_failure_at(t, Admission::Normal);
        b.on_success_at(t, Admission::Normal); // resets streak
        b.on_failure_at(t, Admission::Normal);
        b.on_failure_at(t, Admission::Normal);
        assert_eq!(b.state(), BreakerState::Closed); // only 2 since reset
    }

    #[test]
    fn half_open_admits_only_a_single_concurrent_probe() {
        let b = CircuitBreaker::new(cfg());
        let t0 = Instant::now();
        for _ in 0..3 {
            b.on_failure_at(t0, Admission::Normal);
        }
        assert_eq!(b.state(), BreakerState::Open);
        let after = t0 + Duration::from_millis(1001);
        // First admission after cooldown becomes the half-open probe.
        assert_eq!(b.check_at(after).unwrap(), Admission::Probe);
        assert_eq!(b.state(), BreakerState::HalfOpen);
        // A *concurrent* second request while the probe is in flight must fail
        // fast — no thundering herd onto a recovering upstream.
        assert!(matches!(b.check_at(after), Err(Error::CircuitOpen)));
        assert!(matches!(b.check_at(after), Err(Error::CircuitOpen)));
        // The probe succeeds (needs 2 successes to close); the breaker stays
        // half-open and now admits exactly one more probe, not a flood.
        b.on_success_at(after, Admission::Probe);
        assert_eq!(b.state(), BreakerState::HalfOpen);
        assert_eq!(
            b.check_at(after).unwrap(),
            Admission::Probe,
            "next single probe admitted"
        );
        assert!(
            matches!(b.check_at(after), Err(Error::CircuitOpen)),
            "still only one probe at a time"
        );
    }

    #[test]
    fn release_probe_lets_a_new_probe_in_after_a_rejected_admission() {
        let b = CircuitBreaker::new(cfg());
        let t0 = Instant::now();
        for _ in 0..3 {
            b.on_failure_at(t0, Admission::Normal);
        }
        let after = t0 + Duration::from_millis(1001);
        // Reserve the probe, then a downstream gate rejects: release it.
        assert_eq!(b.check_at(after).unwrap(), Admission::Probe);
        assert_eq!(b.state(), BreakerState::HalfOpen);
        b.release_probe();
        // The freed slot allows the next request to probe.
        assert_eq!(
            b.check_at(after).unwrap(),
            Admission::Probe,
            "released probe slot must be reusable"
        );
        assert!(matches!(b.check_at(after), Err(Error::CircuitOpen)));
    }

    #[test]
    fn half_open_probe_failure_clears_flag_and_reopens() {
        let b = CircuitBreaker::new(cfg());
        let t0 = Instant::now();
        for _ in 0..3 {
            b.on_failure_at(t0, Admission::Normal);
        }
        let after = t0 + Duration::from_millis(1001);
        assert_eq!(b.check_at(after).unwrap(), Admission::Probe);
        assert_eq!(b.state(), BreakerState::HalfOpen);
        // Probe fails -> Open; a fresh cooldown later admits exactly one probe
        // again (the in-flight flag did not leak).
        b.on_failure_at(after, Admission::Probe);
        assert_eq!(b.state(), BreakerState::Open);
        let after2 = after + Duration::from_millis(1001);
        assert_eq!(b.check_at(after2).unwrap(), Admission::Probe);
        assert!(matches!(b.check_at(after2), Err(Error::CircuitOpen)));
    }

    /// P1 (rework R2): a call admitted while **Closed** that finishes *after* the
    /// breaker has transitioned to half-open with a probe outstanding must NOT be
    /// mistaken for the probe. Its stale `Normal`-tagged completion may neither
    /// close the breaker (success) nor free/discard the probe slot — only the
    /// real `Probe` admission drives recovery. Without the admission token this
    /// straddling completion corrupted the single-probe state machine.
    #[test]
    fn closed_admitted_call_completing_after_half_open_does_not_touch_probe() {
        // half_open_successes=1 so a single mistaken success would close the breaker.
        let b = CircuitBreaker::new(BreakerConfig {
            failure_threshold: 3,
            open_ms: 1000,
            half_open_successes: 1,
        });
        let t0 = Instant::now();

        // 1. Request A admitted while Closed (still in flight) — token is Normal.
        let a_admission = b.check_at(t0).expect("closed admits");
        assert_eq!(a_admission, Admission::Normal);

        // 2. Other failures open the breaker.
        for _ in 0..3 {
            b.on_failure_at(t0, Admission::Normal);
        }
        assert_eq!(b.state(), BreakerState::Open);

        // 3. Cooldown elapses; probe P is admitted as the single half-open probe.
        let after = t0 + Duration::from_millis(1001);
        let p_admission = b.check_at(after).expect("probe admitted after cooldown");
        assert_eq!(p_admission, Admission::Probe);
        assert_eq!(b.state(), BreakerState::HalfOpen);

        // 4. NOW A completes — success path. With its Normal token this must be a
        // no-op on probe bookkeeping: breaker stays half-open, probe slot stays
        // taken, so no second probe can slip in while P is still in flight.
        b.on_success_at(after, a_admission);
        assert_eq!(
            b.state(),
            BreakerState::HalfOpen,
            "a Closed-era success must not close the breaker"
        );
        assert!(
            matches!(b.check_at(after), Err(Error::CircuitOpen)),
            "probe slot must remain held by P; no second probe admitted"
        );

        // 5. The real probe P now succeeds and legitimately closes the breaker.
        b.on_success_at(after, p_admission);
        assert_eq!(b.state(), BreakerState::Closed);
    }

    /// P1 sibling: a Closed-admitted call's *failure* arriving while half-open
    /// must not re-open the breaker nor discard the in-flight probe's slot.
    #[test]
    fn closed_admitted_failure_after_half_open_does_not_discard_probe() {
        let b = CircuitBreaker::new(BreakerConfig {
            failure_threshold: 3,
            open_ms: 1000,
            half_open_successes: 1,
        });
        let t0 = Instant::now();
        let a_admission = b.check_at(t0).expect("closed admits");
        for _ in 0..3 {
            b.on_failure_at(t0, Admission::Normal);
        }
        let after = t0 + Duration::from_millis(1001);
        let p_admission = b.check_at(after).expect("probe admitted");
        assert_eq!(b.state(), BreakerState::HalfOpen);

        // Stale Closed-era failure arrives while half-open: ignored.
        b.on_failure_at(after, a_admission);
        assert_eq!(
            b.state(),
            BreakerState::HalfOpen,
            "a Closed-era failure must not re-open the breaker"
        );
        assert!(
            matches!(b.check_at(after), Err(Error::CircuitOpen)),
            "probe slot must still be held by P"
        );

        // The real probe still owns recovery.
        b.on_success_at(after, p_admission);
        assert_eq!(b.state(), BreakerState::Closed);
    }
}
