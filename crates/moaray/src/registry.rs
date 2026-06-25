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

use crate::governed::GovernedProvider;
use crate::runtime::StatefulState;

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
    let retry = config.server.retry;
    let mut providers: HashMap<String, Arc<dyn Provider>> = HashMap::new();
    for (name, m) in &config.models {
        // Resolve upstream key from env at build time (empty if unset; the
        // upstream will reject, and we never log the value).
        let api_key = std::env::var(&m.api_key_env).unwrap_or_default();
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
                config.server.default_max_tokens,
                client.clone(),
            )),
        };
        // Wrap with per-upstream governance (breaker/limiter/concurrency/retry),
        // sharing the same stateful slot across passthrough + MoA. A missing slot
        // is fail-closed: never install a raw provider without its safety wrapper.
        let slot = stateful.upstream(&m.upstream_id).ok_or_else(|| {
            anyhow!(
                "no stateful slot for upstream_id `{}` (model `{}`); refusing to build an \
                 unprotected provider (fail-closed)",
                m.upstream_id,
                name
            )
        })?;
        let provider: Arc<dyn Provider> = Arc::new(GovernedProvider::new(inner, slot, retry));
        providers.insert(name.clone(), provider);
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
            msg.contains("up-gpt"),
            "error should name the upstream: {msg}"
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
