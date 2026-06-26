//! Runtime state, split into two deliberately decoupled layers.
//!
//! - [`Runtime`] holds the **hot-swappable** config-derived data: the resolved
//!   models/recipes and the built provider registry. It lives behind an
//!   `ArcSwap` so a config reload can atomically replace it without touching
//!   in-flight requests.
//! - [`StatefulState`] holds **per-upstream / per-key** state (rate limiters,
//!   concurrency semaphores, circuit breakers) keyed by the internal `state_key`
//!   (`provider_type|base_url|api_key_env`) / inbound key id. It must survive a
//!   config reload, so it is intentionally NOT part of `Runtime`. It is backed by
//!   a `DashMap` so a reload can insert new slots and GC orphaned ones in place
//!   while requests are in flight; each entry is an `Arc`, preserved verbatim for
//!   any upstream/key that still exists. This is the state-preserving foundation
//!   the P3-3 hot reload ([`crate::reload`]) builds on.

use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwap;
use dashmap::DashMap;
use moaray_config::RuntimeConfig;
use moaray_core::error::Result;
use moaray_core::provider::Provider;
use moaray_moa::{MapResolver, Orchestrator};

use crate::breaker::CircuitBreaker;
use crate::limit::{Concurrency, TokenBucket};

/// Hot-swappable, config-derived runtime data.
pub struct Runtime {
    pub config: RuntimeConfig,
    /// model name -> provider instance.
    pub providers: HashMap<String, Arc<dyn Provider>>,
    /// MoA orchestrator, sharing the same provider instances as passthrough.
    pub orchestrator: Orchestrator<MapResolver>,
}

impl Runtime {
    /// Resolve a model name to its provider.
    pub fn provider(&self, model: &str) -> Option<Arc<dyn Provider>> {
        self.providers.get(model).cloned()
    }
}

/// Per-upstream stateful data that must persist across config reloads, keyed by
/// the internal `state_key` (`provider_type|base_url|api_key_env`). Holds the
/// per-upstream token bucket, concurrency cap, and circuit breaker — all shared
/// by passthrough and MoA arms hitting the same upstream identity (plan §1.4).
pub struct UpstreamState {
    /// Per-upstream token bucket. `None` when no rate limit is configured.
    pub limiter: Option<TokenBucket>,
    /// Per-upstream concurrency cap (unbounded when unconfigured).
    pub concurrency: Concurrency,
    /// Per-upstream circuit breaker.
    pub breaker: CircuitBreaker,
    /// The governance inputs this slot was built from (rate_limit /
    /// max_concurrency / breaker thresholds). A reload that keeps the same
    /// `state_key` but changes any of these must REBUILD the slot so the new
    /// safety limits take effect — preserving live state is only correct when the
    /// governance is byte-identical. See [`StatefulState::ensure_for_config`].
    governance: UpstreamGovernance,
}

/// The governance knobs that define an `UpstreamState`'s configuration. Equality
/// decides whether a reload may preserve the live limiter/breaker state (governance
/// unchanged) or must rebuild it (governance changed → new limits must apply).
#[derive(Clone, Copy, PartialEq, Eq)]
struct UpstreamGovernance {
    rate_limit: Option<moaray_config::RateLimit>,
    max_concurrency: Option<u32>,
    breaker: moaray_config::BreakerConfig,
}

impl UpstreamGovernance {
    fn of(model: &moaray_config::ModelConfig, breaker_cfg: moaray_config::BreakerConfig) -> Self {
        Self {
            rate_limit: model.rate_limit,
            max_concurrency: model.max_concurrency,
            breaker: breaker_cfg,
        }
    }
}

impl UpstreamState {
    fn build(
        model: &moaray_config::ModelConfig,
        breaker_cfg: moaray_config::BreakerConfig,
    ) -> Self {
        Self {
            limiter: model.rate_limit.map(TokenBucket::new),
            concurrency: Concurrency::new(model.max_concurrency),
            breaker: CircuitBreaker::new(breaker_cfg),
            governance: UpstreamGovernance::of(model, breaker_cfg),
        }
    }

    /// Enforce the per-upstream limiter (429 `rate_limited` when empty).
    pub fn check_limit(&self) -> Result<()> {
        match &self.limiter {
            Some(b) => b.check(),
            None => Ok(()),
        }
    }

    /// Construct a slot directly from its parts (no config). Intended for unit
    /// tests of the `GovernedProvider` decorator that drive a limiter/breaker
    /// directly. The reload governance fingerprint is left empty (these instances
    /// are never reconciled), so it must not be used on the reload path.
    #[doc(hidden)]
    pub fn from_parts(
        limiter: Option<TokenBucket>,
        concurrency: Concurrency,
        breaker: CircuitBreaker,
    ) -> Self {
        Self {
            limiter,
            concurrency,
            breaker,
            governance: UpstreamGovernance {
                rate_limit: None,
                max_concurrency: None,
                breaker: moaray_config::BreakerConfig {
                    failure_threshold: 0,
                    open_ms: 0,
                    half_open_successes: 0,
                },
            },
        }
    }
}

/// Per-upstream and per-key stateful slots that survive config reloads.
///
/// Backed by [`DashMap`] (not a plain `HashMap`) so a config hot-reload can
/// insert new slots and remove orphaned ones **concurrently and in place** while
/// requests are in flight, without rebuilding the whole layer or swapping the
/// `Arc<StatefulState>` (the handlers hold). The per-upstream map is keyed by the
/// internal `state_key`; the per-key map by `caller_key_id`.
///
/// Reload publish order (P3-3, pinned in [`crate::reload`]): added slots are
/// inserted **before** the new `Runtime` is published ([`Self::ensure_for_config`]
/// → providers built → `ArcSwap.store`), so a freshly routable upstream always
/// has its bucket — there is never a "provider without limiter" window. Removed
/// slots are GC'd **after** a drain window ([`Self::retain_for_config`]); because
/// each live provider holds an `Arc<UpstreamState>`, an in-flight request that
/// resolved the old `Runtime` keeps its state alive until it completes regardless
/// of the map entry being gone.
#[derive(Default)]
pub struct StatefulState {
    /// Per-upstream slots, keyed by the internal `state_key`.
    per_upstream: DashMap<String, Arc<UpstreamState>>,
    /// Per-key inbound limiters, keyed by `caller_key_id`.
    per_key: DashMap<String, Arc<TokenBucket>>,
}

impl StatefulState {
    /// Build a fresh stateful layer from validated config (per-upstream slots +
    /// per-key inbound limiters).
    pub fn from_config(config: &RuntimeConfig) -> Self {
        let state = StatefulState::default();
        state.ensure_for_config(config);
        state.install_key_limiters(config);
        state
    }

    /// **Reload step 1 (before swap):** ensure every upstream identity / inbound
    /// key referenced by `config` has a live slot. The existing `Arc` (and thus
    /// the live bucket/breaker state) is **preserved** for any slot whose
    /// governance is byte-identical; a slot whose governance *changed* (the same
    /// `state_key` but a different rate_limit / max_concurrency / breaker) is
    /// **rebuilt** so the new safety limits take effect. New ids get fresh state.
    /// Safe to call concurrently with traffic.
    ///
    /// This handles **per-upstream** slots only; per-key inbound limiters are
    /// reconciled by [`Self::install_key_limiters`] (before the swap) and
    /// [`Self::retain_key_limiters`] (after it) — they need not exist before
    /// provider build, and splitting install/remove around the swap keeps the
    /// reload all-or-nothing while never leaving a tightened limit unenforced.
    ///
    /// This is the state-preserving seam P3-3 needs: a reload that does not change
    /// an upstream's governance must not reset its limiter remaining or breaker
    /// state; a reload that *does* change it must not silently keep the old limits.
    /// It also pins the invariant "a routable upstream always has a bucket":
    /// callers run this, then build the new providers (which look up these slots),
    /// then publish the new `Runtime` — so the slot is in place before routable.
    pub fn ensure_for_config(&self, config: &RuntimeConfig) {
        for m in config.models.values() {
            let key = m.state_key.as_str();
            let want = UpstreamGovernance::of(m, config.server.breaker);
            let preserve = self
                .per_upstream
                .get(key)
                .is_some_and(|e| e.value().governance == want);
            if !preserve {
                // Absent, or governance changed -> (re)build so new limits apply.
                self.per_upstream.insert(
                    m.state_key.clone(),
                    Arc::new(UpstreamState::build(m, config.server.breaker)),
                );
            }
        }
    }

    /// **Reload step (before swap):** install/refresh per-key inbound limiters for
    /// every key that declares a `rate_limit`, adding a bucket when new and
    /// rebuilding it when the limit value changed (byte-identical → preserved).
    /// Returns the number of buckets added/changed.
    ///
    /// Runs **before** the `Runtime` swap so that any request which can observe the
    /// new runtime also observes the new/tightened per-key bucket — closing the
    /// window where a newly-limited key would be authenticated against the new
    /// config but skip its 429 because the bucket wasn't installed yet. It is
    /// called only after the (fallible) provider build has succeeded, so the reload
    /// stays all-or-nothing: a build failure returns before this runs and leaves
    /// per-key state untouched. Removals are deferred to [`Self::retain_key_limiters`]
    /// after the swap (relaxing a limit is the safe direction to apply last).
    pub fn install_key_limiters(&self, config: &RuntimeConfig) -> usize {
        let mut updated = 0usize;
        for k in &config.keys {
            if let Some(limit) = k.rate_limit {
                let preserve = self
                    .per_key
                    .get(&k.id)
                    .is_some_and(|e| e.value().limit() == limit);
                if !preserve {
                    self.per_key
                        .insert(k.id.clone(), Arc::new(TokenBucket::new(limit)));
                    updated += 1;
                }
            }
        }
        updated
    }

    /// **Reload step 2 (after the drain window):** drop **per-upstream** slots no
    /// longer referenced by `config` (orphans from removed upstream identities).
    /// Returns the number of removed entries. Must run only *after* the new
    /// `Runtime` is published and a drain window has elapsed; dropping the map
    /// entry does not free the state while any in-flight request still holds an
    /// `Arc<UpstreamState>` (Arc refcount), so this is safe even under load — it
    /// only reclaims the lookup index, not the live state. (Per-key buckets are
    /// reclaimed immediately via [`Self::retain_key_limiters`], not here.)
    ///
    /// Pass the **current live** config (not a captured snapshot): if an upstream
    /// was removed and then re-added by a newer reload before this runs, retaining
    /// against the live config keeps the now-live slot instead of deleting it.
    pub fn retain_for_config(&self, config: &RuntimeConfig) -> usize {
        let live_upstreams: std::collections::HashSet<&str> = config
            .models
            .values()
            .map(|m| m.state_key.as_str())
            .collect();

        let before = self.per_upstream.len();
        self.per_upstream
            .retain(|k, _| live_upstreams.contains(k.as_str()));
        before - self.per_upstream.len()
    }

    /// Reclaim per-key inbound limiters not referenced by `config`, **immediately**.
    /// Unlike per-upstream slots, a per-key bucket is looked up fresh on every
    /// request and never held across one, so removing it has no in-flight hazard
    /// and should take effect at once: a key whose `rate_limit` was dropped must
    /// stop being 429'd as soon as the reload is published, not after the drain
    /// window. Returns the number of reclaimed entries.
    pub fn retain_key_limiters(&self, config: &RuntimeConfig) -> usize {
        let live_keys: std::collections::HashSet<&str> = config
            .keys
            .iter()
            .filter(|k| k.rate_limit.is_some())
            .map(|k| k.id.as_str())
            .collect();
        let before = self.per_key.len();
        self.per_key.retain(|k, _| live_keys.contains(k.as_str()));
        before - self.per_key.len()
    }

    /// Look up the per-upstream stateful slot by internal `state_key`.
    pub fn upstream(&self, state_key: &str) -> Option<Arc<UpstreamState>> {
        self.per_upstream.get(state_key).map(|e| e.value().clone())
    }

    /// Number of live per-upstream slots (for tests / introspection).
    pub fn upstream_count(&self) -> usize {
        self.per_upstream.len()
    }

    /// Enforce the inbound per-key limiter, if one is configured for this key.
    /// `Ok(())` when no limit applies or a token is available.
    pub fn check_key_limit(&self, caller_key_id: &str) -> Result<()> {
        match self.per_key.get(caller_key_id) {
            Some(b) => b.value().check(),
            None => Ok(()),
        }
    }
}

/// The whole shared application state handed to handlers.
#[derive(Clone)]
pub struct AppState {
    pub runtime: Arc<ArcSwap<Runtime>>,
    /// Persisted across config hot-swaps; holds the limiter/breaker state.
    pub stateful: Arc<StatefulState>,
    /// The usage-accounting sink (process-lifetime, like `stateful`; NOT in the
    /// hot-swappable `Runtime`). `record`-only and non-blocking; defaults to a
    /// `NullSink` when no `usage_store` is configured. The matching
    /// `UsageWriterHandle` for shutdown flushing is held by `main()`, OUT of state.
    pub usage_sink: Arc<dyn moaray_core::usage::UsageSink>,
}

impl AppState {
    /// Build app state from an initial runtime, deriving the stateful layer from
    /// the same config. Convenience for tests; production wiring builds the
    /// stateful layer first (so providers can be wrapped) via
    /// [`AppState::with_stateful`]. Accounting is disabled (`NullSink`).
    pub fn new(runtime: Runtime) -> Self {
        let stateful = Arc::new(StatefulState::from_config(&runtime.config));
        Self {
            runtime: Arc::new(ArcSwap::from_pointee(runtime)),
            stateful,
            usage_sink: Arc::new(moaray_store::NullSink),
        }
    }

    /// Build app state from a runtime and a pre-built stateful layer. Used by the
    /// bin so the provider registry can be wrapped against the same stateful
    /// slots the handlers will read. Accounting is disabled (`NullSink`).
    pub fn with_stateful(runtime: Runtime, stateful: Arc<StatefulState>) -> Self {
        Self {
            runtime: Arc::new(ArcSwap::from_pointee(runtime)),
            stateful,
            usage_sink: Arc::new(moaray_store::NullSink),
        }
    }

    /// Build app state with an injected usage sink. The bin uses this to wire the
    /// real `SqliteSink`; tests use it to inject a `VecSink` and read back the
    /// rows a request booked — the seam that makes the accounting gates
    /// non-vacuous (without it `NullSink.record` is a no-op and assertions would
    /// pass on an empty set).
    pub fn with_sink(
        runtime: Runtime,
        stateful: Arc<StatefulState>,
        usage_sink: Arc<dyn moaray_core::usage::UsageSink>,
    ) -> Self {
        Self {
            runtime: Arc::new(ArcSwap::from_pointee(runtime)),
            stateful,
            usage_sink,
        }
    }

    /// Load the current runtime snapshot.
    pub fn runtime(&self) -> arc_swap::Guard<Arc<Runtime>> {
        self.runtime.load()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(yaml: &str) -> RuntimeConfig {
        struct E;
        impl moaray_config::EnvSource for E {
            fn get(&self, k: &str) -> Option<String> {
                Some(format!("env-{k}"))
            }
        }
        moaray_config::load_yaml_with_env(yaml, &E).expect("valid config")
    }

    const YAML: &str = r#"
auth:
  keys:
    - id: team-a
      key_env: INBOUND
      allow_models: [gpt]
      rate_limit: {rps: 1, burst: 2}
models:
  - name: gpt
    provider_type: openai-compat
    base_url: https://x
    api_key_env: UP
    upstream_id: shared
    rate_limit: {rps: 1, burst: 2}
"#;

    /// state_key for the single model in `YAML` (provider|base_url|api_key_env).
    const SHARED_KEY: &str = "openai-compat|https://x|UP";

    #[test]
    fn reconcile_preserves_unchanged_upstream_state() {
        let config = cfg(YAML);
        let s1 = StatefulState::from_config(&config);
        // Drain the per-upstream bucket (burst=2 -> two allowed, third rejected).
        let up = s1.upstream(SHARED_KEY).unwrap();
        assert!(up.check_limit().is_ok());
        assert!(up.check_limit().is_ok());
        assert!(up.check_limit().is_err());

        // Reload with an unchanged upstream: state must be preserved (still empty).
        s1.ensure_for_config(&config);
        let up2 = s1.upstream(SHARED_KEY).unwrap();
        assert!(
            up2.check_limit().is_err(),
            "preserved bucket must stay drained across reconcile"
        );
        // And it is literally the same Arc (state object), not a rebuilt one.
        assert!(Arc::ptr_eq(&up, &up2));
    }

    #[test]
    fn reconcile_adds_new_and_keeps_per_key() {
        let config = cfg(YAML);
        let s1 = StatefulState::from_config(&config);
        assert!(s1.check_key_limit("team-a").is_ok());
        assert!(s1.check_key_limit("team-a").is_ok());
        assert!(s1.check_key_limit("team-a").is_err());
        // unknown key has no limiter -> always ok
        assert!(s1.check_key_limit("ghost").is_ok());

        s1.ensure_for_config(&config);
        assert!(
            s1.check_key_limit("team-a").is_err(),
            "per-key bucket preserved across reconcile"
        );
    }

    /// P3-3 (P0-2 / F4): `retain_for_config` drops orphaned slots but only reclaims
    /// the lookup index — an `Arc<UpstreamState>` already cloned out (an in-flight
    /// request's view) keeps the live state alive after the map entry is gone.
    #[test]
    fn retain_removes_orphans_but_inflight_arc_survives() {
        let config = cfg(YAML);
        let state = StatefulState::from_config(&config);
        assert_eq!(state.upstream_count(), 1);
        // An in-flight request resolved its provider, which holds this Arc.
        let inflight = state.upstream(SHARED_KEY).unwrap();
        inflight.check_limit().ok();

        // Reload to an empty-models config is impossible (validate rejects no
        // models), so simulate "this upstream removed" with a config whose model
        // has a different identity triple.
        let other = cfg(r#"
auth:
  keys:
    - id: team-a
      key_env: INBOUND
      allow_models: [gpt]
      rate_limit: {rps: 1, burst: 2}
models:
  - name: gpt
    provider_type: openai-compat
    base_url: https://y
    api_key_env: UP
"#);
        // Publish order: ensure new slot first, then GC the old one.
        state.ensure_for_config(&other);
        let removed = state.retain_for_config(&other);
        assert_eq!(
            removed, 1,
            "only the old upstream slot is GC'd (per-key kept)"
        );
        assert!(
            state.upstream(SHARED_KEY).is_none(),
            "old slot no longer routable"
        );
        // The in-flight Arc is still usable — state lives until the request drops it.
        assert!(Arc::strong_count(&inflight) >= 1);
        let _ = inflight.check_limit();
    }
}
