//! Runtime state, split into two deliberately decoupled layers.
//!
//! - [`Runtime`] holds the **hot-swappable** config-derived data: the resolved
//!   models/recipes and the built provider registry. It lives behind an
//!   `ArcSwap` so a config reload can atomically replace it without touching
//!   in-flight requests.
//! - [`StatefulState`] holds **per-upstream / per-key** state (rate limiters,
//!   concurrency semaphores, circuit breakers) keyed by stable `upstream_id` /
//!   key id. It must survive a config reload, so it is intentionally NOT part of
//!   `Runtime`. Each entry is an `Arc`, and [`StatefulState::reconcile`]
//!   preserves the `Arc` for any upstream/key that still exists — this is the
//!   state-preserving foundation P3-3 (hot reload) builds on. P3-3 itself (the
//!   file watcher) is out of scope here; this ticket only lays the structure and
//!   proves preservation in a unit test.

use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwap;
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
/// stable `upstream_id`. Holds the per-upstream token bucket, concurrency cap,
/// and circuit breaker — all shared by passthrough and MoA arms hitting the same
/// upstream (plan §1.4).
pub struct UpstreamState {
    /// Per-upstream token bucket. `None` when no rate limit is configured.
    pub limiter: Option<TokenBucket>,
    /// Per-upstream concurrency cap (unbounded when unconfigured).
    pub concurrency: Concurrency,
    /// Per-upstream circuit breaker.
    pub breaker: CircuitBreaker,
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
        }
    }

    /// Enforce the per-upstream limiter (429 `rate_limited` when empty).
    pub fn check_limit(&self) -> Result<()> {
        match &self.limiter {
            Some(b) => b.check(),
            None => Ok(()),
        }
    }
}

/// Per-upstream and per-key stateful slots that survive config reloads.
#[derive(Default)]
pub struct StatefulState {
    /// Per-upstream slots, keyed by stable `upstream_id`.
    per_upstream: HashMap<String, Arc<UpstreamState>>,
    /// Per-key inbound limiters, keyed by `caller_key_id`.
    per_key: HashMap<String, Arc<TokenBucket>>,
}

impl StatefulState {
    /// Build a fresh stateful layer from validated config.
    pub fn from_config(config: &RuntimeConfig) -> Self {
        Self::reconcile(None, config)
    }

    /// Build the stateful layer for `config`, **preserving** the `Arc` (and thus
    /// the live bucket/breaker state) for any `upstream_id` / key id that also
    /// existed in `prev`. New ids get fresh state; removed ids are dropped.
    ///
    /// This is the state-preserving seam P3-3 needs: a reload that does not touch
    /// an upstream must not reset its limiter remaining or breaker state.
    pub fn reconcile(prev: Option<&StatefulState>, config: &RuntimeConfig) -> Self {
        let mut per_upstream: HashMap<String, Arc<UpstreamState>> = HashMap::new();
        for m in config.models.values() {
            let id = &m.upstream_id;
            if per_upstream.contains_key(id) {
                continue; // one slot per upstream_id even if several models share it
            }
            let preserved = prev.and_then(|p| p.per_upstream.get(id).cloned());
            let entry = match preserved {
                Some(existing) => existing,
                None => Arc::new(UpstreamState::build(m, config.server.breaker)),
            };
            per_upstream.insert(id.clone(), entry);
        }

        let mut per_key: HashMap<String, Arc<TokenBucket>> = HashMap::new();
        for k in &config.keys {
            if let Some(limit) = k.rate_limit {
                let preserved = prev.and_then(|p| p.per_key.get(&k.id).cloned());
                let entry = preserved.unwrap_or_else(|| Arc::new(TokenBucket::new(limit)));
                per_key.insert(k.id.clone(), entry);
            }
        }

        Self {
            per_upstream,
            per_key,
        }
    }

    /// Look up the per-upstream stateful slot by stable `upstream_id`.
    pub fn upstream(&self, upstream_id: &str) -> Option<Arc<UpstreamState>> {
        self.per_upstream.get(upstream_id).cloned()
    }

    /// Enforce the inbound per-key limiter, if one is configured for this key.
    /// `Ok(())` when no limit applies or a token is available.
    pub fn check_key_limit(&self, caller_key_id: &str) -> Result<()> {
        match self.per_key.get(caller_key_id) {
            Some(b) => b.check(),
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
}

impl AppState {
    /// Build app state from an initial runtime, deriving the stateful layer from
    /// the same config. Convenience for tests; production wiring builds the
    /// stateful layer first (so providers can be wrapped) via
    /// [`AppState::with_stateful`].
    pub fn new(runtime: Runtime) -> Self {
        let stateful = Arc::new(StatefulState::from_config(&runtime.config));
        Self {
            runtime: Arc::new(ArcSwap::from_pointee(runtime)),
            stateful,
        }
    }

    /// Build app state from a runtime and a pre-built stateful layer. Used by the
    /// bin so the provider registry can be wrapped against the same stateful
    /// slots the handlers will read.
    pub fn with_stateful(runtime: Runtime, stateful: Arc<StatefulState>) -> Self {
        Self {
            runtime: Arc::new(ArcSwap::from_pointee(runtime)),
            stateful,
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

    #[test]
    fn reconcile_preserves_unchanged_upstream_state() {
        let config = cfg(YAML);
        let s1 = StatefulState::from_config(&config);
        // Drain the per-upstream bucket (burst=2 -> two allowed, third rejected).
        let up = s1.upstream("shared").unwrap();
        assert!(up.check_limit().is_ok());
        assert!(up.check_limit().is_ok());
        assert!(up.check_limit().is_err());

        // Reload with an unchanged upstream: state must be preserved (still empty).
        let s2 = StatefulState::reconcile(Some(&s1), &config);
        let up2 = s2.upstream("shared").unwrap();
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

        let s2 = StatefulState::reconcile(Some(&s1), &config);
        assert!(
            s2.check_key_limit("team-a").is_err(),
            "per-key bucket preserved across reconcile"
        );
    }
}
