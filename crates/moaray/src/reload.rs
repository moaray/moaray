//! Config hot reload — state-preserving, all-or-nothing (P3-3).
//!
//! A [`ConfigReloader`] owns the live [`AppState`] (the `ArcSwap<Runtime>` + the
//! reload-surviving `Arc<StatefulState>`), the persistent upstream `reqwest`
//! [`Client`] (the connection-pool carrier, F5), and the config path. Calling
//! [`ConfigReloader::reload`] re-reads the file and atomically swaps in a new
//! `Runtime`, **preserving** the per-upstream limiter/breaker state for every
//! upstream identity that survives the change.
//!
//! ## Pinned publish order (DESIGN-P3-3 P0-2)
//!
//! The order below is load-bearing and must not be reordered — it is what makes
//! the invariant *"a routable upstream always has a bucket"* hold even under
//! concurrent traffic during the swap:
//!
//! ```text
//! 1. read + validate the new config (total / all-or-nothing). On ANY error,
//!    return Err and leave the old Runtime in place — the server keeps serving.
//! 2. diff added / removed / unchanged upstream identities (for logging only;
//!    the state map is reconciled by set operations below).
//! 3. ensure_for_config: insert a fresh limiter/breaker slot for every NEW
//!    upstream identity, PRESERVING the Arc for unchanged ones. (state first)
//! 4. build the new providers, reusing unchanged Arcs (F5) and looking up the
//!    slots that step 3 just guaranteed to exist. Fail-closed if any is missing.
//! 5. ArcSwap.store(new Runtime)  <-- only now are new models routable; their
//!    buckets already exist (steps 3-4), so there is no "provider without
//!    limiter" window.
//! 6. retain_for_config AFTER a drain window: GC the slots of removed upstreams.
//!    This is delayed (F4) so an in-flight request that resolved the OLD Runtime
//!    keeps using its still-alive Arc<UpstreamState> until it completes.
//! ```
//!
//! ## Hot vs frozen server fields (F2)
//!
//! `request_timeout_ms` / `max_body_bytes` / `moa_expose_metadata` are read live
//! from the swapped-in config on every request (see [`crate::app::ServerCtx`]), so
//! a reload changes them immediately. `bind` / `port` / `shutdown_grace_ms` need a
//! restart; a reload that changes them logs a warning and ignores the new value.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use moaray_config::RuntimeConfig;
use reqwest::Client;
use tokio::sync::Mutex;

use crate::registry::{build_orchestrator_from_built, build_providers_with, BuiltProvider};
use crate::runtime::{AppState, Runtime};

/// Default drain window before orphaned per-upstream state is GC'd. A removed
/// upstream's limiter/breaker must outlive any in-flight request that resolved the
/// previous `Runtime`; this window bounds that wait. Kept independent of (and at
/// least as long as) a typical request timeout.
pub const DEFAULT_GC_DELAY: Duration = Duration::from_secs(120);

/// Owns everything a reload needs and serializes reloads against each other.
pub struct ConfigReloader {
    state: AppState,
    /// Persistent upstream client — the connection-pool carrier shared across
    /// reloads so unchanged upstreams never reconnect (F5).
    client: Client,
    config_path: String,
    /// The previous build's providers, tagged with signatures, for F5 diff-reuse.
    last_built: Arc<Mutex<HashMap<String, BuiltProvider>>>,
    /// Delay between publishing the new Runtime and GC'ing orphaned state (F4).
    gc_delay: Duration,
}

/// Summary of what a successful reload changed (returned for logging/tests).
#[derive(Debug, Default, PartialEq, Eq)]
pub struct ReloadOutcome {
    /// Upstream identities (state_keys) newly added this reload.
    pub upstreams_added: usize,
    /// Upstream identities present before and after (state preserved).
    pub upstreams_unchanged: usize,
    /// Upstream identities removed (their state is GC'd after the drain window).
    pub upstreams_removed: usize,
    /// Models whose provider Arc was reused (warm pool kept, F5).
    pub providers_reused: usize,
    /// Models whose provider Arc was (re)built.
    pub providers_built: usize,
}

impl ConfigReloader {
    /// Build a reloader around the live state, the persistent client, and the
    /// config path. `initial_built` is the provider map from the first
    /// (startup) build so the first reload can diff-reuse against it.
    pub fn new(
        state: AppState,
        client: Client,
        config_path: impl Into<String>,
        initial_built: HashMap<String, BuiltProvider>,
    ) -> Self {
        Self {
            state,
            client,
            config_path: config_path.into(),
            last_built: Arc::new(Mutex::new(initial_built)),
            gc_delay: DEFAULT_GC_DELAY,
        }
    }

    /// Override the GC drain window (tests use a short one).
    pub fn with_gc_delay(mut self, d: Duration) -> Self {
        self.gc_delay = d;
        self
    }

    /// Re-read the config file and apply it. See the module-level publish order.
    ///
    /// On validation/parse failure the old `Runtime` is left untouched (the
    /// server keeps serving the last-good config) and an `Err` is returned (F7
    /// all-or-nothing). On success the new `Runtime` is published atomically with
    /// per-upstream state preserved, and orphaned state is scheduled for delayed
    /// GC. Reloads are serialized via an internal lock.
    pub async fn reload(&self) -> Result<ReloadOutcome> {
        // Serialize reloads so two concurrent triggers can't interleave the
        // ensure -> build -> swap -> gc sequence.
        let mut last_built = self.last_built.lock().await;

        // 1. read + validate (all-or-nothing). Any error returns here, old
        //    Runtime stays in place, service survives.
        let yaml = std::fs::read_to_string(&self.config_path)
            .with_context(|| format!("reading config from {}", self.config_path))?;
        let new_config: RuntimeConfig =
            moaray_config::load_yaml(&yaml).map_err(|e| anyhow::anyhow!("invalid config: {e}"))?;

        let prev_runtime = self.state.runtime.load_full();
        let outcome = self.apply(&new_config, &prev_runtime.config, &mut last_built)?;
        Ok(outcome)
    }

    /// Apply an already-validated config. Split out so tests can drive a reload
    /// from an in-memory `RuntimeConfig` without touching the filesystem. Runs
    /// publish-order steps 2-6.
    pub async fn apply_validated(&self, new_config: &RuntimeConfig) -> Result<ReloadOutcome> {
        let mut last_built = self.last_built.lock().await;
        let prev_runtime = self.state.runtime.load_full();
        self.apply(new_config, &prev_runtime.config, &mut last_built)
    }

    fn apply(
        &self,
        new_config: &RuntimeConfig,
        prev_config: &RuntimeConfig,
        last_built: &mut HashMap<String, BuiltProvider>,
    ) -> Result<ReloadOutcome> {
        // F2: warn on changes to non-hot fields (need a restart; ignored here).
        warn_on_frozen_field_changes(prev_config, new_config);
        // F6: warn on referenced-but-unset upstream api_key_env (do NOT reject).
        warn_on_missing_api_key_env(new_config);

        // 2. diff upstream identities (state_keys) for the outcome summary.
        let prev_keys: std::collections::HashSet<&str> = prev_config
            .models
            .values()
            .map(|m| m.state_key.as_str())
            .collect();
        let new_keys: std::collections::HashSet<&str> = new_config
            .models
            .values()
            .map(|m| m.state_key.as_str())
            .collect();
        let upstreams_added = new_keys.difference(&prev_keys).count();
        let upstreams_removed = prev_keys.difference(&new_keys).count();
        let upstreams_unchanged = new_keys.intersection(&prev_keys).count();

        // 3. state first: ensure a slot exists for every new upstream identity,
        //    preserving the Arc for unchanged ones.
        self.state.stateful.ensure_for_config(new_config);

        // 4. build providers, reusing unchanged Arcs (F5) and looking up the slots
        //    step 3 just guaranteed. Fail-closed if any slot is missing.
        let built = build_providers_with(
            new_config,
            &self.state.stateful,
            &self.client,
            Some(last_built),
        )?;
        let providers_reused = built
            .iter()
            .filter(|(name, b)| {
                last_built
                    .get(*name)
                    .is_some_and(|p| Arc::ptr_eq(&p.provider, &b.provider))
            })
            .count();
        let providers_built = built.len() - providers_reused;

        let orchestrator = build_orchestrator_from_built(new_config, &built);
        let plain: HashMap<String, Arc<dyn moaray_core::provider::Provider>> = built
            .iter()
            .map(|(n, b)| (n.clone(), b.provider.clone()))
            .collect();

        // 4b. Install/refresh per-key inbound limiters BEFORE the swap. The
        //     fallible provider build above already succeeded, so the reload is
        //     still all-or-nothing; installing now means any request that can see
        //     the new runtime also sees the new/tightened per-key bucket (no
        //     "new config visible but limiter missing" window). Removals are
        //     applied after the swap (relaxing a limit is the safe last step).
        let k_updated = self.state.stateful.install_key_limiters(new_config);

        // 5. publish: only now are new models routable, and their buckets already
        //    exist (steps 3-4) — no "provider without limiter" window.
        let new_runtime = Runtime {
            config: new_config.clone(),
            providers: plain,
            orchestrator,
        };
        self.state.runtime.store(Arc::new(new_runtime));

        // Remember this build for the next reload's diff-reuse.
        *last_built = built;

        // 6a. Reclaim orphaned per-key limiters AFTER the swap. A per-key bucket is
        //     looked up fresh per request and never held across one, so dropping it
        //     has no in-flight hazard; doing it post-swap relaxes (never tightens)
        //     a caller's limit, which is the safe ordering.
        let k_removed = self.state.stateful.retain_key_limiters(new_config);
        if k_updated > 0 || k_removed > 0 {
            tracing::info!(
                updated = k_updated,
                removed = k_removed,
                "reload: reconciled per-key limiters"
            );
        }

        // 6b. delayed GC of orphaned PER-UPSTREAM state (F4): spawn so the swap
        //     returns immediately; the removed upstreams' Arc<UpstreamState> stays
        //     alive for any in-flight request on the old Runtime until the window
        //     elapses.
        //
        //     The GC task re-acquires the reload lock and retains against the
        //     **live** runtime config read under that lock. Holding the lock means
        //     no reload is mid-flight (a reload owns the lock across its whole
        //     ensure→build→swap sequence), so the GC can never observe a
        //     half-published config and never deletes a slot a newer reload just
        //     re-added (generation-safe + race-free with publishing).
        if upstreams_removed > 0 {
            let state = self.state.clone();
            let lock = self.last_built.clone();
            let delay = self.gc_delay;
            tokio::spawn(async move {
                tokio::time::sleep(delay).await;
                let _guard = lock.lock().await; // serialize with any in-flight reload
                let live = state.runtime.load();
                let removed = state.stateful.retain_for_config(&live.config);
                if removed > 0 {
                    tracing::info!(removed, "reload: GC'd orphaned per-upstream state");
                }
            });
        }

        tracing::info!(
            upstreams_added,
            upstreams_unchanged,
            upstreams_removed,
            providers_reused,
            providers_built,
            "config reloaded"
        );

        Ok(ReloadOutcome {
            upstreams_added,
            upstreams_unchanged,
            upstreams_removed,
            providers_reused,
            providers_built,
        })
    }

    /// Access the underlying app state (for wiring / tests).
    pub fn state(&self) -> &AppState {
        &self.state
    }
}

/// F2: server fields that require a restart. Warn (do not error) if a reload
/// tries to change them; the running value is kept.
fn warn_on_frozen_field_changes(prev: &RuntimeConfig, new: &RuntimeConfig) {
    for field in frozen_field_changes(prev, new) {
        tracing::warn!(
            field,
            "reload: `{field}` change needs a restart — ignoring the new value"
        );
    }
}

/// Pure detection of which restart-frozen server fields changed between two
/// configs. Returned names drive the warn path; the running value is always kept.
/// `usage_store` is frozen because the sink + its OS-thread writer are
/// process-lifetime (per-model PRICES are NOT here — they hot-reload per request).
fn frozen_field_changes(prev: &RuntimeConfig, new: &RuntimeConfig) -> Vec<&'static str> {
    let mut changed = Vec::new();
    if prev.server.bind != new.server.bind {
        changed.push("bind");
    }
    if prev.server.port != new.server.port {
        changed.push("port");
    }
    if prev.server.shutdown_grace_ms != new.server.shutdown_grace_ms {
        changed.push("shutdown_grace_ms");
    }
    if prev.server.usage_store != new.server.usage_store {
        changed.push("usage_store");
    }
    changed
}

/// F6: warn (do not reject) when a model references an api_key_env that is not
/// set in the environment. The provider will resolve to an empty key and the
/// upstream will reject — but startup/reload stays lenient by design. The value
/// is never logged (only the env var name).
fn warn_on_missing_api_key_env(config: &RuntimeConfig) {
    for m in config.models.values() {
        if std::env::var(&m.api_key_env).is_err() {
            tracing::warn!(
                model = %m.name,
                api_key_env = %m.api_key_env,
                "reload: referenced api_key_env is not set in the environment \
                 (upstream calls for this model will fail until it is provisioned)"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::{build_orchestrator_from_built, build_providers_with};
    use crate::runtime::{AppState, Runtime, StatefulState};

    fn cfg(yaml: &str) -> RuntimeConfig {
        struct E;
        impl moaray_config::EnvSource for E {
            fn get(&self, k: &str) -> Option<String> {
                Some(format!("env-{k}"))
            }
        }
        moaray_config::load_yaml_with_env(yaml, &E).expect("valid config")
    }

    fn reloader_for(config: &RuntimeConfig) -> ConfigReloader {
        let stateful = Arc::new(StatefulState::from_config(config));
        let client = Client::new();
        let built =
            build_providers_with(config, &stateful, &client, None).expect("providers build");
        let orchestrator = build_orchestrator_from_built(config, &built);
        let providers = built
            .iter()
            .map(|(n, b)| (n.clone(), b.provider.clone()))
            .collect();
        let runtime = Runtime {
            config: config.clone(),
            providers,
            orchestrator,
        };
        let state = AppState::with_stateful(runtime, stateful);
        ConfigReloader::new(state, client, "test.yaml", built)
            .with_gc_delay(Duration::from_millis(10))
    }

    const V1: &str = r#"
auth:
  keys:
    - id: team-a
      key_env: INBOUND
      allow_models: [a, b]
models:
  - {name: a, provider_type: openai-compat, base_url: https://a, api_key_env: KA}
  - {name: b, provider_type: openai-compat, base_url: https://b, api_key_env: KB}
"#;

    /// F5: a reload that changes only model `b` reuses model `a`'s provider Arc
    /// (warm connection pool kept), rebuilding only the changed one.
    #[tokio::test]
    async fn reload_reuses_unchanged_provider_arc() {
        let v1 = cfg(V1);
        let reloader = reloader_for(&v1);

        // v2: same `a`, change `b`'s base_url (new identity -> rebuild b only).
        let v2 = cfg(r#"
auth:
  keys:
    - id: team-a
      key_env: INBOUND
      allow_models: [a, b]
models:
  - {name: a, provider_type: openai-compat, base_url: https://a, api_key_env: KA}
  - {name: b, provider_type: openai-compat, base_url: https://b2, api_key_env: KB}
"#);
        let out = reloader.apply_validated(&v2).await.unwrap();
        assert_eq!(out.providers_reused, 1, "model `a` reused");
        assert_eq!(out.providers_built, 1, "only model `b` rebuilt");
        assert_eq!(out.upstreams_added, 1, "b's new identity added");
        assert_eq!(out.upstreams_removed, 1, "b's old identity removed");
        assert_eq!(out.upstreams_unchanged, 1, "a's identity unchanged");
    }

    /// F6: a reload that references an api_key_env which is unset must NOT reject —
    /// it warns and applies (lenient startup posture). We use the real OS env path
    /// here (load_yaml), with an env var we deliberately leave unset.
    #[tokio::test]
    async fn reload_with_unset_api_key_env_is_not_rejected() {
        // Build an initial valid runtime (env resolves via cfg's fake source), then
        // apply a config whose api_key_env is unset in the REAL environment. The
        // F6 warn path runs inside apply(); it must not error.
        let v1 = cfg(V1);
        let reloader = reloader_for(&v1);
        // Same shape; api_key_env names are not set in the real OS env -> warn only.
        let out = reloader.apply_validated(&v1).await;
        assert!(out.is_ok(), "unset api_key_env must warn, not reject");
    }

    /// The `usage_store` knobs are restart-frozen: a reload that changes them must
    /// be detected by the frozen-field check (warn path) — the running store is
    /// kept because the sink is process-lifetime in AppState, untouched by reload.
    #[test]
    fn usage_store_change_is_detected_as_frozen() {
        const WITHOUT: &str = r#"
auth:
  keys:
    - id: team-a
      key_env: INBOUND
      allow_models: [a]
models:
  - {name: a, provider_type: openai-compat, base_url: https://a, api_key_env: KA}
"#;
        const WITH_STORE: &str = r#"
server:
  usage_store:
    path: /tmp/moaray-usage.db
auth:
  keys:
    - id: team-a
      key_env: INBOUND
      allow_models: [a]
models:
  - {name: a, provider_type: openai-compat, base_url: https://a, api_key_env: KA}
"#;
        let without = cfg(WITHOUT);
        let with_store = cfg(WITH_STORE);

        // Adding a store is a frozen change.
        let changed = frozen_field_changes(&without, &with_store);
        assert!(
            changed.contains(&"usage_store"),
            "adding usage_store must be flagged frozen, got {changed:?}"
        );
        // Changing the path is a frozen change.
        let with_other = cfg(&WITH_STORE.replace("/tmp/moaray-usage.db", "/tmp/other.db"));
        let changed2 = frozen_field_changes(&with_store, &with_other);
        assert!(
            changed2.contains(&"usage_store"),
            "path change must be flagged"
        );
        // No change → not flagged (prices are NOT frozen and live elsewhere).
        let changed3 = frozen_field_changes(&with_store, &cfg(WITH_STORE));
        assert!(
            !changed3.contains(&"usage_store"),
            "identical store must not be flagged, got {changed3:?}"
        );
    }
}
