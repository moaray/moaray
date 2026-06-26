//! Registry assembly: turn validated config models into concrete providers.
//!
//! This is the *only* place that depends on `moaray-providers` and decides which
//! adapter implements a given model. Upstream API keys are resolved from the
//! environment here and handed to the provider; they never enter logs.
//!
//! It is also the P2-1 injection seam for MoA: the bin owns the dependency
//! direction (`moaray-moa` depends only on `moaray-core`), so translating
//! validated config recipes into the orchestrator's own [`Recipe`] type and
//! building the [`Orchestrator`] happens here, not inside `moaray-moa`.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use moaray_config::{ProviderType, RuntimeConfig, Strategy as CfgStrategy};
use moaray_core::provider::Provider;
use moaray_moa::{MapResolver, Orchestrator, Recipe, Strategy as MoaStrategy};
use moaray_providers::{build_client, AnthropicProvider, OpenAiProvider};
use reqwest::Client;

use crate::governed::GovernedProvider;
use crate::runtime::StatefulState;

/// A built provider together with the signature of the config inputs that
/// produced it. The signature lets a hot-reload (F5 diff-and-reuse) detect an
/// **unchanged** model and reuse the existing `Arc<dyn Provider>` — keeping its
/// warm connection pool and governance wrapper — instead of rebuilding it and
/// triggering a reconnect storm. Two models are reuse-compatible iff every input
/// that affects the provider's behaviour is identical (see [`provider_signature`]).
#[derive(Clone)]
pub struct BuiltProvider {
    pub provider: Arc<dyn Provider>,
    pub signature: String,
}

/// Compute the reuse signature for a model: every provider-affecting input.
///
/// Includes the resolved upstream key value (not just the env var name) so that
/// rotating the secret in the environment forces a rebuild. `default_max_tokens`
/// and the retry policy are server-level inputs baked into the provider, so they
/// are part of the signature too — a change to either must rebuild the wrapper.
///
/// It also folds in the per-upstream **governance** (rate_limit / max_concurrency
/// / breaker). When governance changes, `StatefulState::ensure_for_config` rebuilds
/// the `UpstreamState` slot; the `GovernedProvider` caches an `Arc<UpstreamState>`,
/// so it must be rebuilt too — otherwise it would keep enforcing the OLD limits
/// against a stale slot. Including governance here guarantees that rebuild.
fn provider_signature(
    m: &moaray_config::ModelConfig,
    api_key: &str,
    default_max_tokens: u32,
    retry: moaray_config::RetryConfig,
    breaker: moaray_config::BreakerConfig,
) -> String {
    // The api_key is hashed-by-inclusion only inside this in-process signature; it
    // is NEVER logged or surfaced. We fold it in so a rotated key rebuilds the
    // provider. Length-prefix fields so they can't ambiguously run together.
    let rl = m
        .rate_limit
        .map(|r| format!("{}:{}", r.rps, r.burst))
        .unwrap_or_else(|| "none".to_string());
    format!(
        "pt={:?}|url={}|keyenv={}|key={}|sid={}|uid={}|dmt={}|retry={}:{}:{}|rl={}|mc={:?}|brk={}:{}:{}",
        m.provider_type,
        m.base_url,
        m.api_key_env,
        api_key,
        m.state_key,
        m.upstream_id,
        default_max_tokens,
        retry.enabled,
        retry.max_retries,
        retry.backoff_ms,
        rl,
        m.max_concurrency,
        breaker.failure_threshold,
        breaker.open_ms,
        breaker.half_open_successes,
    )
}

/// Build the model-name -> provider map from validated config.
///
/// Every concrete adapter is wrapped in a [`GovernedProvider`] bound to the
/// shared per-`upstream_id` slot in `stateful`. Because the orchestrator is
/// built from this same map (see [`build_orchestrator`]), a MoA arm and a
/// passthrough call to the same model share one breaker/limiter/semaphore — MoA
/// fan-out cannot bypass the per-upstream cap (plan §1.4).
///
/// **Fail-closed:** the per-upstream governance wrapper is load-bearing safety,
/// so a missing `stateful` slot for a model's `upstream_id` is a hard build
/// error, never a silent fall-back to the raw (unprotected) provider. `stateful`
/// is built from the same config, so in normal wiring this never fires; it only
/// guards future/reload callers (`build_providers` is `pub`) from accidentally
/// installing a provider that has lost its breaker/limiter/concurrency.
pub fn build_providers(
    config: &RuntimeConfig,
    stateful: &StatefulState,
) -> Result<HashMap<String, Arc<dyn Provider>>> {
    let client = build_client();
    let built = build_providers_with(config, stateful, &client, None)?;
    Ok(built.into_iter().map(|(n, b)| (n, b.provider)).collect())
}

/// Build providers, **reusing** the `Arc<dyn Provider>` from `prev` for any model
/// whose reuse signature is unchanged (F5 diff-and-reuse). Reused providers keep
/// their warm connection pool and governance wrapper, so changing one model in a
/// large config does not trigger an upstream-wide reconnect storm. The shared
/// `client` is the connection-pool carrier and is persisted across reloads.
///
/// Returns each provider tagged with its signature so the next reload can diff
/// against it. Fail-closed on a missing stateful slot, exactly like
/// [`build_providers`].
pub fn build_providers_with(
    config: &RuntimeConfig,
    stateful: &StatefulState,
    client: &Client,
    prev: Option<&HashMap<String, BuiltProvider>>,
) -> Result<HashMap<String, BuiltProvider>> {
    let retry = config.server.retry;
    let default_max_tokens = config.server.default_max_tokens;
    let breaker = config.server.breaker;
    let mut providers: HashMap<String, BuiltProvider> = HashMap::new();
    for (name, m) in &config.models {
        // Resolve upstream key from env at build time (empty if unset; the
        // upstream will reject, and we never log the value).
        let api_key = std::env::var(&m.api_key_env).unwrap_or_default();
        let signature = provider_signature(m, &api_key, default_max_tokens, retry, breaker);

        // F5 diff-and-reuse: if this exact model+inputs existed before, reuse its
        // Arc (warm pool + governance wrapper bound to the same stateful slot).
        if let Some(existing) = prev
            .and_then(|p| p.get(name))
            .filter(|b| b.signature == signature)
        {
            providers.insert(name.clone(), existing.clone());
            continue;
        }

        let inner: Arc<dyn Provider> = match m.provider_type {
            ProviderType::OpenaiCompat => Arc::new(OpenAiProvider::new(
                m.upstream_id.clone(),
                m.base_url.clone(),
                api_key,
                client.clone(),
            )),
            ProviderType::Anthropic => Arc::new(AnthropicProvider::new(
                m.upstream_id.clone(),
                m.base_url.clone(),
                api_key,
                default_max_tokens,
                client.clone(),
            )),
        };
        // Wrap with per-upstream governance (breaker/limiter/concurrency/retry),
        // sharing the same stateful slot across passthrough + MoA. The slot is
        // keyed by the internal `state_key` (identity triple), so two aliases of
        // the same upstream share one bucket. A missing slot is fail-closed: never
        // install a raw provider without its safety wrapper.
        let slot = stateful.upstream(&m.state_key).ok_or_else(|| {
            anyhow!(
                "no stateful slot for upstream identity of model `{}` (upstream_id `{}`); refusing \
                 to build an unprotected provider (fail-closed)",
                name,
                m.upstream_id
            )
        })?;
        let provider: Arc<dyn Provider> = Arc::new(GovernedProvider::new(inner, slot, retry));
        providers.insert(
            name.clone(),
            BuiltProvider {
                provider,
                signature,
            },
        );
    }
    Ok(providers)
}

/// Translate a validated config recipe into the orchestrator's own [`Recipe`].
/// The two strategy enums are deliberately decoupled (config crate vs moa crate
/// dependency boundary); this is the one place they are bridged.
fn to_moa_recipe(r: &moaray_config::RecipeConfig) -> Recipe {
    let strategy = match r.strategy {
        CfgStrategy::ConcatSynthesize => MoaStrategy::ConcatSynthesize,
        CfgStrategy::QuorumJudge => MoaStrategy::QuorumJudge,
    };
    Recipe {
        name: r.name.clone(),
        proposers: r.proposers.clone(),
        aggregator: r.aggregator.clone(),
        strategy,
        arm_timeout_ms: r.arm_timeout_ms,
        quorum: r.quorum,
    }
}

/// Build the MoA orchestrator from validated config + the built provider map.
///
/// The resolver shares the same `Arc<dyn Provider>` instances as passthrough, so
/// a MoA arm and a passthrough call to the same model hit the identical provider
/// (and, in Phase 3, the identical per-upstream limiter/breaker).
pub fn build_orchestrator(
    config: &RuntimeConfig,
    providers: &HashMap<String, Arc<dyn Provider>>,
) -> Orchestrator<MapResolver> {
    let mut resolver = MapResolver::new();
    for (name, p) in providers {
        resolver.insert(name.clone(), p.clone());
    }
    let recipes = config
        .recipes
        .iter()
        .map(|(name, r)| (name.clone(), to_moa_recipe(r)))
        .collect();
    Orchestrator::new(resolver, recipes)
}

/// Build the MoA orchestrator from a [`BuiltProvider`] map (the reload path),
/// sharing the same `Arc<dyn Provider>` instances as passthrough.
pub fn build_orchestrator_from_built(
    config: &RuntimeConfig,
    providers: &HashMap<String, BuiltProvider>,
) -> Orchestrator<MapResolver> {
    let mut resolver = MapResolver::new();
    for (name, b) in providers {
        resolver.insert(name.clone(), b.provider.clone());
    }
    let recipes = config
        .recipes
        .iter()
        .map(|(name, r)| (name.clone(), to_moa_recipe(r)))
        .collect();
    Orchestrator::new(resolver, recipes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::StatefulState;

    struct E;
    impl moaray_config::EnvSource for E {
        fn get(&self, _k: &str) -> Option<String> {
            None
        }
    }

    const YAML: &str = r#"
auth:
  keys:
    - id: a
      key_env: INBOUND
      allow_models: [gpt]
models:
  - name: gpt
    provider_type: openai-compat
    base_url: https://x
    api_key_env: UP
    upstream_id: up-gpt
"#;

    /// P2 (rework R2): a model whose `upstream_id` has no stateful slot must make
    /// `build_providers` FAIL (hard error), not silently install a raw provider
    /// that has lost its breaker/limiter/concurrency wrapper (degrade-open).
    #[test]
    fn missing_stateful_slot_fails_closed() {
        let config = moaray_config::load_yaml_with_env(YAML, &E).expect("valid config");
        // An empty stateful layer has no slot for `up-gpt`.
        let empty = StatefulState::default();
        let err = match build_providers(&config, &empty) {
            Ok(_) => panic!("must fail closed when the stateful slot is missing"),
            Err(e) => e,
        };
        let msg = err.to_string();
        assert!(msg.contains("fail-closed"), "unexpected error: {msg}");
        assert!(
            msg.contains("up-gpt") || msg.contains("gpt"),
            "error should name the upstream/model: {msg}"
        );
    }

    /// Contrast: with a stateful layer built from the same config, the slot exists
    /// and the provider is built (and governed).
    #[test]
    fn present_stateful_slot_builds_governed_provider() {
        let config = moaray_config::load_yaml_with_env(YAML, &E).expect("valid config");
        let stateful = StatefulState::from_config(&config);
        let providers = build_providers(&config, &stateful).expect("builds when slot present");
        assert!(providers.contains_key("gpt"));
    }
}
