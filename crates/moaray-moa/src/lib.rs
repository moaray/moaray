//! moaray-moa — the MoA orchestrator.
//!
//! **Dependency boundary (load-bearing):** this crate depends *only* on
//! `moaray-core`. It drives upstreams exclusively through `Arc<dyn Provider>`
//! and never depends on `moaray-providers`; the `moaray` bin is what assembles
//! concrete providers into a registry and hands them here. This keeps the
//! orchestration logic testable with fakes and prevents a provider-layer
//! dependency from leaking into the fan-out core.
//!
//! Phase 1 establishes the boundary and the recipe-resolution surface; the
//! fan-out / aggregate / quorum-judge logic is implemented in Phase 2.

use std::collections::HashMap;
use std::sync::Arc;

use moaray_core::provider::Provider;

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn map_resolver_returns_none_for_unknown() {
        let r = MapResolver::new();
        assert!(r.resolve("nope").is_none());
    }
}
