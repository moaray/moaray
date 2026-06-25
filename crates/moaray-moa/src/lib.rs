//! moaray-moa — the MoA orchestrator.
//!
//! **Dependency boundary (load-bearing):** this crate depends *only* on
//! `moaray-core`. It drives upstreams exclusively through `Arc<dyn Provider>`
//! and never depends on `moaray-providers`; the `moaray` bin is what assembles
//! concrete providers into a registry and hands them here. This keeps the
//! orchestration logic testable with fakes and prevents a provider-layer
//! dependency from leaking into the fan-out core.
//!
//! Phase 2 activates the fan-out / aggregate / quorum-judge logic on top of the
//! Phase 1 boundary. The orchestrator holds a [`ProviderResolver`] (model name
//! -> `Arc<dyn Provider>`) plus the resolved recipes, both injected by the bin.

pub mod orchestrator;
pub mod recipe;
pub mod strategy;

use std::collections::HashMap;
use std::sync::Arc;

use moaray_core::provider::Provider;

pub use orchestrator::{ArmOutcome, ArmStatus, MoaResult};
pub use recipe::{Recipe, Strategy};

/// A resolver from model name to a concrete provider instance. The orchestrator
/// only ever sees `Arc<dyn Provider>`, never a concrete adapter type.
pub trait ProviderResolver: Send + Sync {
    /// Resolve a model name to its provider, if configured.
    fn resolve(&self, model: &str) -> Option<Arc<dyn Provider>>;
}

/// A simple map-backed resolver (used by the bin and by tests).
#[derive(Default, Clone)]
pub struct MapResolver {
    providers: HashMap<String, Arc<dyn Provider>>,
}

impl MapResolver {
    /// Create an empty resolver.
    pub fn new() -> Self {
        Self {
            providers: HashMap::new(),
        }
    }

    /// Register a provider under a model name.
    pub fn insert(&mut self, model: impl Into<String>, provider: Arc<dyn Provider>) {
        self.providers.insert(model.into(), provider);
    }
}

impl ProviderResolver for MapResolver {
    fn resolve(&self, model: &str) -> Option<Arc<dyn Provider>> {
        self.providers.get(model).cloned()
    }
}

/// The MoA orchestrator: a [`ProviderResolver`] plus the resolved recipe table.
///
/// Construction is the P2-1 injection seam: the bin translates validated config
/// recipes into [`Recipe`]s and builds the resolver from the provider registry,
/// then hands both here. Unknown recipe names surface as `model_not_found` at
/// run time (see [`Orchestrator::run`]).
pub struct Orchestrator<R: ProviderResolver> {
    resolver: R,
    recipes: HashMap<String, Recipe>,
}

impl<R: ProviderResolver> Orchestrator<R> {
    /// Build an orchestrator from a resolver and the resolved recipe table.
    pub fn new(resolver: R, recipes: HashMap<String, Recipe>) -> Self {
        Self { resolver, recipes }
    }

    /// Look up a recipe by name (the `<recipe>` part of `moa/<recipe>`).
    pub fn recipe(&self, name: &str) -> Option<&Recipe> {
        self.recipes.get(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn map_resolver_returns_none_for_unknown() {
        let r = MapResolver::new();
        assert!(r.resolve("nope").is_none());
    }

    #[test]
    fn orchestrator_recipe_lookup() {
        let recipe = Recipe {
            name: "arm-e".into(),
            proposers: vec!["a".into()],
            aggregator: "agg".into(),
            strategy: Strategy::ConcatSynthesize,
            arm_timeout_ms: 1000,
            quorum: 1,
        };
        let mut recipes = HashMap::new();
        recipes.insert("arm-e".to_string(), recipe);
        let orch = Orchestrator::new(MapResolver::new(), recipes);
        assert!(orch.recipe("arm-e").is_some());
        assert!(orch.recipe("ghost").is_none());
    }
}
